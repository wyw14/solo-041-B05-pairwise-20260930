package process

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

func newTestCommand(t *testing.T, generation, revision uint64, setpoint float64, issued time.Time) model.DeviceCommand {
	t.Helper()
	command, err := model.NewDeviceCommand("line-1", uuid.New(), "doser-pac", model.DeviceDoser, generation, revision, "set", issued)
	if err != nil {
		t.Fatalf("new command: %v", err)
	}
	command.Setpoint = setpoint
	return command
}

func TestPublishNewerSameGenerationWins(t *testing.T) {
	bus := NewDeviceBus()
	base := time.Now().UTC()
	rateTwo := newTestCommand(t, 1, 1, 2, base)
	rateFour := newTestCommand(t, 1, 2, 4, base.Add(time.Second))

	if driven, err := bus.Publish(context.Background(), rateTwo); err != nil || !driven {
		t.Fatalf("publish rate 2: driven=%v err=%v", driven, err)
	}
	if driven, err := bus.Publish(context.Background(), rateFour); err != nil || !driven {
		t.Fatalf("publish rate 4: driven=%v err=%v", driven, err)
	}

	// The rate-2 command arrives late (a concurrent retry). It must not win.
	if _, err := bus.Publish(context.Background(), rateTwo); !errors.Is(err, model.ErrStaleDeviceCommand) {
		t.Fatalf("late rate-2 publish: want ErrStaleDeviceCommand, got %v", err)
	}
	got := bus.commands[rateTwo.DeviceID]
	if got.ID != rateFour.ID || got.Setpoint != 4 {
		t.Fatalf("bus regressed to %v setpoint %v, want rate-4 command", got.ID, got.Setpoint)
	}
}

func TestPublishSameCommandIsIdempotent(t *testing.T) {
	bus := NewDeviceBus()
	command := newTestCommand(t, 1, 1, 4, time.Now().UTC())

	driven, err := bus.Publish(context.Background(), command)
	if err != nil || !driven {
		t.Fatalf("first publish: driven=%v err=%v", driven, err)
	}
	// A legal redelivery of the identical command must succeed without
	// driving the device a second time.
	driven, err = bus.Publish(context.Background(), command)
	if err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	if driven {
		t.Fatal("identical retry reported driven=true; device would be driven twice")
	}
}

func TestPublishRejectsLowerGeneration(t *testing.T) {
	bus := NewDeviceBus()
	base := time.Now().UTC()
	stop := newTestCommand(t, 2, 1, 0, base.Add(time.Second))
	stop.Action = "stop"
	old := newTestCommand(t, 1, 99, 4, base)

	if _, err := bus.Publish(context.Background(), stop); err != nil {
		t.Fatalf("publish emergency stop: %v", err)
	}
	if _, err := bus.Publish(context.Background(), old); !errors.Is(err, model.ErrStaleDeviceCommand) {
		t.Fatalf("old-generation command: want stale, got %v", err)
	}
	got := bus.commands[stop.DeviceID]
	if got.Generation != 2 || got.Action != "stop" {
		t.Fatalf("emergency stop was overwritten: %+v", got)
	}
}

func TestStopLineIsIdempotentAndOneWay(t *testing.T) {
	bus := NewDeviceBus()
	base := time.Now().UTC()
	command := newTestCommand(t, 1, 5, 4, base)
	if _, err := bus.Publish(context.Background(), command); err != nil {
		t.Fatalf("publish: %v", err)
	}
	bus.StopLine("line-1", 2)
	got := bus.commands[command.DeviceID]
	if got.Generation != 2 || got.Setpoint != 0 || got.Action != "stop" {
		t.Fatalf("stop did not isolate command: %+v", got)
	}
	// Replaying the emergency (same or older generation) changes nothing.
	bus.StopLine("line-1", 2)
	bus.StopLine("line-1", 1)
	got = bus.commands[command.DeviceID]
	if got.Generation != 2 || got.Setpoint != 0 {
		t.Fatalf("repeated stop downgraded command: %+v", got)
	}
}

func TestStopLineLeavesOtherLines(t *testing.T) {
	bus := NewDeviceBus()
	base := time.Now().UTC()
	lineOne := newTestCommand(t, 1, 1, 4, base)
	lineTwo, err := model.NewDeviceCommand("line-2", uuid.New(), "doser-pam", model.DeviceDoser, 1, 1, "set", base)
	if err != nil {
		t.Fatal(err)
	}
	lineTwo.Setpoint = 3
	if _, err := bus.Publish(context.Background(), lineOne); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Publish(context.Background(), lineTwo); err != nil {
		t.Fatal(err)
	}
	bus.StopLine("line-1", 2)
	if got := bus.commands[lineTwo.DeviceID]; got.Generation != 1 || got.Setpoint != 3 {
		t.Fatalf("stop on line-1 touched line-2: %+v", got)
	}
}

// Concurrent delivery of revisions 1..N, in shuffled order across goroutines,
// must converge on the highest revision: an old instruction racing in late can
// never become the current command.
func TestPublishConcurrentConvergesOnNewest(t *testing.T) {
	bus := NewDeviceBus()
	base := time.Now().UTC()
	const revisions = 200
	commands := make([]model.DeviceCommand, revisions)
	for revision := 1; revision <= revisions; revision++ {
		// IssuedAt is held equal so ordering rests solely on (gen, revision).
		commands[revision-1] = newTestCommand(t, 1, uint64(revision), float64(revision), base)
	}

	var wg sync.WaitGroup
	for _, offset := range randPerm(revisions) {
		wg.Add(1)
		go func(command model.DeviceCommand) {
			defer wg.Done()
			_, _ = bus.Publish(context.Background(), command)
		}(commands[offset])
	}
	wg.Wait()

	got := bus.commands["doser-pac"]
	if got.Revision != revisions || got.Setpoint != float64(revisions) {
		t.Fatalf("concurrent bus settled at revision %d setpoint %v, want %d", got.Revision, got.Setpoint, revisions)
	}

	// Replaying every command a second time drives nothing new.
	for _, command := range commands {
		driven, err := bus.Publish(context.Background(), command)
		if err != nil && !errors.Is(err, model.ErrStaleDeviceCommand) {
			t.Fatalf("replay: %v", err)
		}
		if driven {
			t.Fatal("replayed command drove the device again")
		}
	}
}

func randPerm(n int) []int {
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	// Deterministic shuffle so the test needs no randomness source.
	for i := n - 1; i > 0; i-- {
		j := (i * 7) % (i + 1)
		perm[i], perm[j] = perm[j], perm[i]
	}
	return perm
}
