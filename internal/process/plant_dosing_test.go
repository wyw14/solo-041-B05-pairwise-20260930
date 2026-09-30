package process

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

func newTestPlant(t *testing.T) *Plant {
	t.Helper()
	now := time.Unix(1_000, 0).UTC()
	clock := func() time.Time { return now }
	plant, err := NewPlant(t.TempDir(), []model.LineID{"line-1"}, clock)
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	return plant
}

func TestPlantDoseUpdateStaysConsistentWhenOldCommandArrivesLate(t *testing.T) {
	ctx := context.Background()
	plant := newTestPlant(t)

	batch, err := plant.StartBatch(ctx, "line-1", 100, "influent")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}

	// Operator sets rate 2, then rate 4 on the same line/batch/generation.
	cmd2, err := plant.ChangeDose(ctx, "line-1", batch.ID, "pac", 2)
	if err != nil {
		t.Fatalf("set dose 2: %v", err)
	}
	cmd4, err := plant.ChangeDose(ctx, "line-1", batch.ID, "pac", 4)
	if err != nil {
		t.Fatalf("set dose 4: %v", err)
	}
	if !(cmd2.Revision < cmd4.Revision) {
		t.Fatalf("command revisions must be ordered like the events: %d !< %d", cmd2.Revision, cmd4.Revision)
	}

	// The older command is retried / delivered late after 4 has been applied.
	if err := plant.devices.Publish(ctx, cmd2); !errors.Is(err, model.ErrStaleDeviceCommand) {
		t.Fatalf("late older command: want stale, got %v", err)
	}

	status, err := plant.Status("line-1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	// Latest dose on the status page is 4 ...
	if status.Dose == nil || status.Dose.Rate != 4 {
		t.Fatalf("status page dose = %+v, want 4", status.Dose)
	}
	// ... and the command the device bus currently holds must also be 4.
	var current *model.DeviceCommand
	for i := range status.Commands {
		if status.Commands[i].DeviceID == "doser-pac" {
			current = &status.Commands[i]
		}
	}
	if current == nil || current.Setpoint != 4 || current.Revision != cmd4.Revision {
		t.Fatalf("device bus command = %+v, want 4 @ rev %d", current, cmd4.Revision)
	}
}

func TestPlantEmergencySwitchThenLateDoseIsRejected(t *testing.T) {
	ctx := context.Background()
	plant := newTestPlant(t)

	batch, err := plant.StartBatch(ctx, "line-1", 100, "influent")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	cmd, err := plant.ChangeDose(ctx, "line-1", batch.ID, "pac", 4)
	if err != nil {
		t.Fatalf("set dose: %v", err)
	}

	if _, err := plant.EmergencyStop(ctx, "line-1", "operator"); err != nil {
		t.Fatalf("emergency stop: %v", err)
	}
	// A pre-emergency dosing command delivered late must not revive the doser.
	if err := plant.devices.Publish(ctx, cmd); !errors.Is(err, model.ErrStaleDeviceCommand) {
		t.Fatalf("late pre-emergency command: want stale, got %v", err)
	}
	for _, command := range plant.devices.All() {
		if command.DeviceID == "doser-pac" && (command.Action != "stop" || command.Setpoint != 0) {
			t.Fatalf("emergency stop was overridden: %+v", command)
		}
	}
}

func TestPlantRecoveryRebuildsLatestDoseAndBus(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Unix(1_000, 0).UTC()

	first, err := NewPlant(dir, []model.LineID{"line-1"}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	batch, err := first.StartBatch(ctx, "line-1", 100, "influent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.ChangeDose(ctx, "line-1", batch.ID, "pac", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := first.ChangeDose(ctx, "line-1", batch.ID, "pac", 4); err != nil {
		t.Fatal(err)
	}

	// Fresh process instance recovers the line purely from the journal.
	second, err := NewPlant(dir, []model.LineID{"line-1"}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.RecoverLine(ctx, "line-1"); err != nil {
		t.Fatalf("recover: %v", err)
	}
	// The dosing ledger rebuilt from events remembers 4 as the latest rate ...
	latest, ok := second.dosing.Latest(batch.ID)
	if !ok || latest.Rate != 4 {
		t.Fatalf("recovered latest dose = %+v ok=%v, want 4", latest, ok)
	}
	// ... and the device bus, rebuilt through the same ordered events, agrees.
	var current *model.DeviceCommand
	for _, command := range second.devices.All() {
		if command.DeviceID == "doser-pac" {
			cmd := command
			current = &cmd
		}
	}
	if current == nil || current.Setpoint != 4 {
		t.Fatalf("recovered device bus = %+v, want 4", current)
	}
}
