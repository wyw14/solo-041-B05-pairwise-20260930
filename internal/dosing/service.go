package dosing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

type CommandPublisher interface {
	Publish(context.Context, model.DeviceCommand) error
}

type Service struct {
	journal   journal.Appender
	publisher CommandPublisher
	ledger    *Ledger
}

func NewService(appender journal.Appender, publisher CommandPublisher, ledger *Ledger) (*Service, error) {
	if appender == nil || publisher == nil || ledger == nil {
		return nil, errors.New("dosing service dependencies are required")
	}
	return &Service{journal: appender, publisher: publisher, ledger: ledger}, nil
}

// ChangeRate commits a dosing change and drives its doser.
//
// The command's intra-generation revision is the sequence of the event that
// committed the change, so the order recorded in the journal is exactly the
// order enforced on the device bus: a change committed first can never roll
// back one committed later, no matter how the publishes interleave.
//
// The device command carries a stable identity derived from the change ID.
// Re-delivering the same change (a retry or a replay) therefore lands on the
// same command and is deduped on the bus, so the doser is driven at most once
// per change.
func (s *Service) ChangeRate(ctx context.Context, change Change, now time.Time) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	// In-process redelivery of an already-committed change: do not append a
	// second event; republish the identical command and let the bus dedup it.
	if committed, revision, ok := s.ledger.Lookup(change.ID); ok {
		return s.dispatch(ctx, committed, revision, now)
	}
	event, err := model.NewEvent(change.LineID, change.Generation, model.EventDosingChanged, change, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	persisted, err := s.journal.Append(ctx, event)
	if err != nil {
		return model.DeviceCommand{}, fmt.Errorf("append dosing batch failed: %w", err)
	}
	committed, revision := s.ledger.Commit(change, persisted.Sequence)
	return s.dispatch(ctx, committed, revision, now)
}

// ApplyRecovered rebuilds dosing state from a persisted dosing.changed event.
// It never appends a new event and never double-drives a device: the command
// identity is derived from the change, so a revision already on the bus is
// deduped, while an older revision replayed out of commit order is ignored.
func (s *Service) ApplyRecovered(ctx context.Context, event model.Event) error {
	if event.Kind != model.EventDosingChanged {
		return nil
	}
	var change Change
	if err := event.Decode(&change); err != nil {
		return err
	}
	committed, revision := s.ledger.Commit(change, event.Sequence)
	_, err := s.dispatch(ctx, committed, revision, event.OccurredAt)
	return err
}

func (s *Service) dispatch(ctx context.Context, change Change, revision uint64, now time.Time) (model.DeviceCommand, error) {
	command := buildCommand(change, revision, now)
	if err := s.publisher.Publish(ctx, command); err != nil {
		// An older revision losing to a newer one after arriving late is the
		// normal ordering outcome; the event stays on record and the bus keeps
		// the newer setting. Do not turn ordering into a failure callers retry.
		if !errors.Is(err, model.ErrStaleDeviceCommand) {
			return model.DeviceCommand{}, fmt.Errorf("publish dosing command: %w", err)
		}
	}
	return command, nil
}

func buildCommand(change Change, revision uint64, now time.Time) model.DeviceCommand {
	command, _ := model.NewDeviceCommand(change.LineID, change.BatchID, "doser-"+change.Chemical, model.DeviceDoser, change.Generation, "set", now)
	command.ID = commandIDFor(change.ID)
	command.Revision = revision
	command.Setpoint = change.Rate
	command.Metadata["change_id"] = change.ID.String()
	return command
}

func (s *Service) Latest(batchID uuid.UUID) (Change, bool) {
	return s.ledger.Latest(batchID)
}
