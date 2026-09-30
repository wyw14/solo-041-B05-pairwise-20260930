package dosing

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

func newService(t *testing.T) (*Service, *journal.FileStore) {
	t.Helper()
	store, err := journal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bus := newRecordingPublisher()
	service, err := NewService(store, bus, NewLedger())
	if err != nil {
		t.Fatal(err)
	}
	return service, store
}

type recordingPublisher struct {
	commands []model.DeviceCommand
}

func newRecordingPublisher() *recordingPublisher { return &recordingPublisher{} }

func (r *recordingPublisher) Publish(_ context.Context, command model.DeviceCommand) (bool, error) {
	r.commands = append(r.commands, command)
	return true, nil
}

func changeFor(t *testing.T, rate float64) Change {
	t.Helper()
	change, err := NewChange("line-1", uuid.New(), "pac", rate, 1, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return change
}

func TestChangeRateAppendsCommandEventAndLedger(t *testing.T) {
	service, store := newService(t)
	change := changeFor(t, 4)
	command, err := service.ChangeRate(context.Background(), change, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if command.Revision == 0 {
		t.Fatal("command revision was not assigned from journal sequence")
	}
	events, err := store.Events(context.Background(), "line-1")
	if err != nil {
		t.Fatal(err)
	}
	var kinds []model.EventKind
	for _, event := range events {
		kinds = append(kinds, event.Kind)
	}
	if len(kinds) != 2 || kinds[0] != model.EventDosingChanged || kinds[1] != model.EventDeviceCommanded {
		t.Fatalf("events = %v, want [dosing.changed device.commanded]", kinds)
	}
	if latest, ok := service.Latest(change.BatchID); !ok || latest.Rate != 4 {
		t.Fatalf("ledger latest = %v ok=%v", latest, ok)
	}
}

func TestLedgerOrdersByCommittedSequence(t *testing.T) {
	service, _ := newService(t)
	batchID := uuid.New()

	older := changeFor(t, 2)
	newer := changeFor(t, 4)
	older.BatchID = batchID
	newer.BatchID = batchID
	older.Sequence = 10
	newer.Sequence = 11

	// The older setting arrives at the ledger last (late retry / out of order
	// projection): it must still sort beneath the newer committed setting.
	service.Restore(newer)
	service.Restore(older)

	latest, ok := service.Latest(batchID)
	if !ok {
		t.Fatal("ledger empty")
	}
	if latest.ID != newer.ID || latest.Rate != 4 {
		t.Fatalf("latest = rate %v, want 4", latest.Rate)
	}

	// Re-recording either change is a no-op (no duplicate history entry).
	service.Restore(older)
	service.Restore(newer)
	latest, _ = service.Latest(batchID)
	if latest.ID != newer.ID {
		t.Fatalf("dedup regressed latest to %v", latest.ID)
	}
}
