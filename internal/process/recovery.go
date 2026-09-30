package process

import (
	"context"
	"errors"

	"github.com/wyw14/cry-97/internal/dosing"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

func (p *Plant) RecoverLine(ctx context.Context, lineID model.LineID) (uint64, error) {
	if snapshot, found, err := p.snapshots.Load(lineID); err != nil {
		return 0, err
	} else if found {
		p.mu.Lock()
		p.lines[lineID] = snapshot
		p.mu.Unlock()
	}
	replayer, err := journal.NewReplayer(p.store, p)
	if err != nil {
		return 0, err
	}
	return replayer.ReplayLine(ctx, lineID)
}

// ApplyRecovered rebuilds read models from a committed event. Every projection
// here is idempotent — the device bus rejects stale commands and dedups
// redelivered ones, the dosing ledger dedups change IDs, and isolation adopts a
// fixed generation instead of advancing it — so replaying the journal on top of
// a snapshot converges on the same state rather than drifting on every restart.
func (p *Plant) ApplyRecovered(ctx context.Context, event model.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state, ok := p.lines[event.LineID]
	if !ok {
		return errors.New("recovery line is not configured")
	}
	if event.IsTerminal() {
		var record EmergencyRecord
		if err := event.Decode(&record); err != nil {
			return err
		}
		if record.LineID != event.LineID || record.Generation != event.Generation {
			return errors.New("emergency recovery payload does not match event identity")
		}
		state = state.IsolateAt(record.Generation, event.Sequence, event.OccurredAt)
		p.lines[event.LineID] = state
		// Bump every previously replayed command for this line into the
		// emergency generation; the bus refuses to let anything older back in.
		p.devices.StopLine(event.LineID, record.Generation)
		return nil
	}
	switch event.Kind {
	case model.EventDeviceCommanded:
		var command model.DeviceCommand
		if err := event.Decode(&command); err != nil {
			return err
		}
		// A command that loses ordering during replay is simply skipped; it
		// never overwrites the newer command already on the bus.
		if _, err := p.devices.Publish(ctx, command); err != nil && !errors.Is(err, model.ErrStaleDeviceCommand) {
			return err
		}
	case model.EventDosingChanged:
		var change dosing.Change
		if err := event.Decode(&change); err != nil {
			return err
		}
		// Rebuild status-page dosing history without re-driving the bus; the
		// matching device.commanded event is what restores the bus.
		change.Sequence = event.Sequence
		p.dosing.Restore(change)
	}
	state.LastEventSeq = event.Sequence
	state.UpdatedAt = event.OccurredAt
	p.lines[event.LineID] = state
	return nil
}
