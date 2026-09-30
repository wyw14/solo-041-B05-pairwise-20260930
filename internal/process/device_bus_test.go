package process

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

func makeCommand(t *testing.T, generation, revision uint64, setpoint float64, id uuid.UUID) model.DeviceCommand {
	t.Helper()
	command, err := model.NewDeviceCommand("line-1", uuid.New(), "doser-pac", model.DeviceDoser, generation, "set", time.Unix(0, int64(revision)).UTC())
	if err != nil {
		t.Fatalf("new command: %v", err)
	}
	command.ID = id
	command.Revision = revision
	command.Setpoint = setpoint
	return command
}

func TestSameGenerationLateCommandCannotRollBack(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()

	rate2 := makeCommand(t, 1, 1, 2, uuid.New())
	rate4 := makeCommand(t, 1, 2, 4, uuid.New())

	if err := bus.Publish(ctx, rate2); err != nil {
		t.Fatalf("publish rate 2: %v", err)
	}
	if err := bus.Publish(ctx, rate4); err != nil {
		t.Fatalf("publish rate 4: %v", err)
	}
	// The older command is retried / delivered late after the newer one won.
	if err := bus.Publish(ctx, rate2); !errors.Is(err, model.ErrStaleDeviceCommand) {
		t.Fatalf("late same-generation command: want stale error, got %v", err)
	}

	got := bus.All()
	if len(got) != 1 || got[0].Setpoint != 4 || got[0].Revision != 2 {
		t.Fatalf("bus must keep newest setting 4/rev2, got %+v", got)
	}
}

func TestRedeliveredCommandIsIdempotent(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()
	command := makeCommand(t, 1, 1, 2, uuid.New())

	if err := bus.Publish(ctx, command); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	// Identical redelivery (same identity at the same position) is a no-op.
	if err := bus.Publish(ctx, command); err != nil {
		t.Fatalf("legitimate retry must be idempotent, got %v", err)
	}
	if len(bus.All()) != 1 {
		t.Fatalf("retry must not duplicate the command: %+v", bus.All())
	}
}

func TestSamePositionDifferentIdentityConflicts(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()
	first := makeCommand(t, 1, 1, 2, uuid.New())
	impostor := makeCommand(t, 1, 1, 3, uuid.New())

	if err := bus.Publish(ctx, first); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if err := bus.Publish(ctx, impostor); !errors.Is(err, model.ErrDeviceCommandConflict) {
		t.Fatalf("distinct command at same position: want conflict, got %v", err)
	}
}

func TestEmergencyGenerationRejectsPreviousGeneration(t *testing.T) {
	bus := NewDeviceBus()
	ctx := context.Background()

	active := makeCommand(t, 1, 5, 4, uuid.New())
	if err := bus.Publish(ctx, active); err != nil {
		t.Fatalf("publish active: %v", err)
	}
	// Emergency stop bumps the line onto generation 2.
	bus.StopLine("line-1", 2)
	stopped := bus.All()[0]
	if stopped.Generation != 2 || stopped.Action != "stop" || stopped.Setpoint != 0 {
		t.Fatalf("stop did not take effect: %+v", stopped)
	}

	// A late command from the pre-emergency generation must not revive the pump.
	late := makeCommand(t, 1, 6, 4, uuid.New())
	if err := bus.Publish(ctx, late); !errors.Is(err, model.ErrStaleDeviceCommand) {
		t.Fatalf("pre-emergency command: want stale, got %v", err)
	}
	got := bus.All()[0]
	if got.Action != "stop" || got.Setpoint != 0 {
		t.Fatalf("emergency stop was overridden: %+v", got)
	}

	// A genuinely newer command on the emergency generation is accepted.
	newer := makeCommand(t, 2, 6, 1, uuid.New())
	if err := bus.Publish(ctx, newer); err != nil {
		t.Fatalf("new-generation command: %v", err)
	}
}
