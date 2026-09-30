package process

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

func newTestPlant(t *testing.T) (*Plant, model.LineID) {
	t.Helper()
	lineID := model.LineID("line-1")
	plant, err := NewPlant(t.TempDir(), []model.LineID{lineID}, func() time.Time { return time.Now().UTC() })
	if err != nil {
		t.Fatal(err)
	}
	return plant, lineID
}

func startTestBatch(t *testing.T, plant *Plant, lineID model.LineID) uuid.UUID {
	t.Helper()
	batch, err := plant.StartBatch(context.Background(), lineID, 100, "test")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	return batch.ID
}

func busSetpoint(plant *Plant, deviceID string) (float64, uint64, bool) {
	for _, command := range plant.devices.All() {
		if command.DeviceID == deviceID {
			return command.Setpoint, command.Generation, true
		}
	}
	return 0, 0, false
}

// The reported defect: rate set to 2 then 4 on the same batch; a late retry of
// the rate-2 instruction must not roll the device bus back to 2.
func TestChangeDoseLateRetryDoesNotRegressBus(t *testing.T) {
	plant, lineID := newTestPlant(t)
	batchID := startTestBatch(t, plant, lineID)
	ctx := context.Background()
	deviceID := "doser-pac"

	rateTwo, err := plant.ChangeDose(ctx, lineID, batchID, "pac", 2)
	if err != nil {
		t.Fatalf("dose 2: %v", err)
	}
	rateFour, err := plant.ChangeDose(ctx, lineID, batchID, "pac", 4)
	if err != nil {
		t.Fatalf("dose 4: %v", err)
	}

	if setpoint, _, ok := busSetpoint(plant, deviceID); !ok || setpoint != 4 {
		t.Fatalf("after updates setpoint=%v ok=%v, want 4", setpoint, ok)
	}

	// Late retry of the rate-2 instruction (its own command ID, older
	// revision): it must be rejected as stale and never roll the bus back.
	if _, err := plant.devices.Publish(ctx, rateTwo); !errors.Is(err, model.ErrStaleDeviceCommand) {
		t.Fatalf("late rate-2 retry: want ErrStaleDeviceCommand, got %v", err)
	}
	if setpoint, _, _ := busSetpoint(plant, deviceID); setpoint != 4 {
		t.Fatalf("late stale retry regressed bus to %v, want 4", setpoint)
	}

	// A legal redelivery of the currently-applied (rate-4) command is a retry
	// too: accepted, but it must not drive the device a second time.
	if driven, err := plant.devices.Publish(ctx, rateFour); err != nil || driven {
		t.Fatalf("current-command redelivery: driven=%v err=%v, want driven=false nil", driven, err)
	}
	if setpoint, _, _ := busSetpoint(plant, deviceID); setpoint != 4 {
		t.Fatalf("idempotent redelivery changed bus to %v, want 4", setpoint)
	}

	// The status page (ledger) and the bus agree on the newest dose.
	status, err := plant.Status(lineID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Dose == nil || status.Dose.Rate != 4 {
		t.Fatalf("status dose = %+v, want 4", status.Dose)
	}
}

func TestEmergencyStopDominatesCommands(t *testing.T) {
	plant, lineID := newTestPlant(t)
	batchID := startTestBatch(t, plant, lineID)
	ctx := context.Background()
	if _, err := plant.ChangeDose(ctx, lineID, batchID, "pac", 4); err != nil {
		t.Fatal(err)
	}
	state, err := plant.EmergencyStop(ctx, lineID, "test isolation")
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 2 {
		t.Fatalf("emergency generation = %d, want 2", state.Generation)
	}
	setpoint, generation, ok := busSetpoint(plant, "doser-pac")
	if !ok || generation != 2 || setpoint != 0 {
		t.Fatalf("post-emergency bus: gen=%d setpoint=%v ok=%v", generation, setpoint, ok)
	}

	// A pre-emergency command (old generation) can never come back.
	stale := model.DeviceCommand{}
	for _, command := range plant.devices.All() {
		if command.DeviceID == "doser-pac" {
			stale = command
		}
	}
	stale.Generation = 1
	if _, err := plant.devices.Publish(ctx, stale); err == nil {
		t.Fatal("old-generation command after emergency was accepted")
	}
}

// A restart (new plant over the same data directory) must rebuild the bus and
// dosing ledger to the same newest-wins state, and must not advance the
// emergency generation on every replay.
func TestRecoveryRebuildsNewestState(t *testing.T) {
	dir := t.TempDir()
	lineID := model.LineID("line-1")
	clock := func() time.Time { return time.Now().UTC() }

	first, err := NewPlant(dir, []model.LineID{lineID}, clock)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	batch, err := first.StartBatch(ctx, lineID, 100, "test")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	batchID := batch.ID
	if _, err := first.ChangeDose(ctx, lineID, batchID, "pac", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := first.ChangeDose(ctx, lineID, batchID, "pac", 4); err != nil {
		t.Fatal(err)
	}
	if _, err := first.EmergencyStop(ctx, lineID, "test isolation"); err != nil {
		t.Fatal(err)
	}

	// Simulate two successive restarts over the same journal + snapshot.
	for restart := 0; restart < 2; restart++ {
		restarted, err := NewPlant(dir, []model.LineID{lineID}, clock)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restarted.RecoverLine(ctx, lineID); err != nil {
			t.Fatalf("recover restart %d: %v", restart, err)
		}
		line := restarted.lines[lineID]
		if !line.Emergency || line.Generation != 2 {
			t.Fatalf("restart %d line: emergency=%v gen=%d, want emergency gen 2", restart, line.Emergency, line.Generation)
		}
		setpoint, generation, ok := busSetpoint(restarted, "doser-pac")
		if !ok || generation != 2 || setpoint != 0 {
			t.Fatalf("restart %d bus: gen=%d setpoint=%v ok=%v", restart, generation, setpoint, ok)
		}
		// Dosing history is restored; newest committed setting is rate 4.
		if latest, found := restarted.dosing.Latest(batchID); !found || latest.Rate != 4 {
			t.Fatalf("restart %d ledger latest=%+v found=%v", restart, latest, found)
		}
	}
}
