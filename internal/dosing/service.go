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
	Publish(context.Context, model.DeviceCommand) (bool, error)
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

// ChangeRate commits a dosing change and drives the doser. The change and the
// resulting command are appended to the journal before the device is touched,
// and the journal sequence becomes the command revision: the order committed
// to the event log is exactly the order in which commands may win on the bus.
//
// When a newer setting has already been applied, Publish rejects the command as
// stale and the caller receives model.ErrStaleDeviceCommand; the late command
// never overwrites the device. A redelivery of the very same command is a
// legal retry: it is acknowledged without driving the device a second time.
func (s *Service) ChangeRate(ctx context.Context, change Change, now time.Time) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	event, err := model.NewEvent(change.LineID, change.Generation, model.EventDosingChanged, change, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	persisted, err := s.journal.Append(ctx, event)
	if err != nil {
		return model.DeviceCommand{}, fmt.Errorf("append dosing batch failed: %w", err)
	}
	change.Sequence = persisted.Sequence

	command, err := model.NewDeviceCommand(change.LineID, change.BatchID, "doser-"+change.Chemical, model.DeviceDoser, change.Generation, persisted.Sequence, "set", now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	command.Setpoint = change.Rate
	command.Metadata["change_id"] = change.ID.String()

	commandEvent, err := model.NewEvent(change.LineID, change.Generation, model.EventDeviceCommanded, command, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if _, err := s.journal.Append(ctx, commandEvent); err != nil {
		return model.DeviceCommand{}, fmt.Errorf("append dosing command failed: %w", err)
	}

	// The change is now committed history. Mirror it into the status-page
	// ledger before touching the bus, so a command that later loses the
	// ordering race still leaves an in-memory projection consistent with the
	// journal (and with restart replay). Record is ordered and idempotent.
	s.ledger.Record(change)

	driven, err := s.publisher.Publish(ctx, command)
	if err != nil {
		return model.DeviceCommand{}, fmt.Errorf("publish dosing command: %w", err)
	}
	if !driven {
		// The identical command was already applied: a legal retry that must
		// not drive the device again. Succeed without a second side effect.
		return command, nil
	}
	return command, nil
}

// Restore replays a historically committed dosing change into the ledger
// without touching the device bus.
func (s *Service) Restore(change Change) {
	s.ledger.Record(change)
}

func (s *Service) Latest(batchID uuid.UUID) (Change, bool) {
	return s.ledger.Latest(batchID)
}
