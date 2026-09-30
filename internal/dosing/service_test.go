package dosing_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/dosing"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/process"
)

type drivenPublisher struct {
	mu     sync.Mutex
	inner  *process.DeviceBus
	drives map[uuid.UUID]int
}

func newDrivenPublisher(bus *process.DeviceBus) *drivenPublisher {
	return &drivenPublisher{inner: bus, drives: make(map[uuid.UUID]int)}
}

func (p *drivenPublisher) Publish(ctx context.Context, command model.DeviceCommand) error {
	before := p.currentSetpoint(command.DeviceID)
	if err := p.inner.Publish(ctx, command); err != nil {
		return err
	}
	// Only count an attempt that actually moved the device; an idempotent
	// redelivery leaves the held setpoint unchanged.
	if p.currentSetpoint(command.DeviceID) != before {
		p.mu.Lock()
		p.drives[command.ID]++
		p.mu.Unlock()
	}
	return nil
}

func (p *drivenPublisher) currentSetpoint(deviceID string) float64 {
	for _, command := range p.inner.All() {
		if command.DeviceID == deviceID {
			return command.Setpoint
		}
	}
	return -1
}

func (p *drivenPublisher) driveCount(id uuid.UUID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.drives[id]
}

func newService(t *testing.T) (*dosing.Service, *process.DeviceBus, *journal.FileStore, *drivenPublisher) {
	t.Helper()
	store, err := journal.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	bus := process.NewDeviceBus()
	pub := newDrivenPublisher(bus)
	svc, err := dosing.NewService(store, pub, dosing.NewLedger())
	if err != nil {
		t.Fatalf("service: %v", err)
	}
	return svc, bus, store, pub
}

func TestChangeRateOrdersByJournalSequenceAndRetryIsIdempotent(t *testing.T) {
	ctx := context.Background()
	svc, bus, store, pub := newService(t)
	now := time.Unix(100, 0).UTC()
	batchID := uuid.New()
	line := model.LineID("line-1")

	rate2, err := dosing.NewChange(line, batchID, "pac", 2, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	rate4, err := dosing.NewChange(line, batchID, "pac", 4, 1, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	cmd2, err := svc.ChangeRate(ctx, rate2, now)
	if err != nil {
		t.Fatalf("rate 2: %v", err)
	}
	cmd4, err := svc.ChangeRate(ctx, rate4, now.Add(time.Second))
	if err != nil {
		t.Fatalf("rate 4: %v", err)
	}
	if !(cmd2.Revision < cmd4.Revision) {
		t.Fatalf("command revisions must follow journal sequence: %d !< %d", cmd2.Revision, cmd4.Revision)
	}

	events, err := store.Events(ctx, line)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 committed events, got %d", len(events))
	}

	// Late redelivery of the older change (concurrent retry arriving after 4).
	if _, err := svc.ChangeRate(ctx, rate2, now.Add(2*time.Second)); err != nil {
		t.Fatalf("late older change must be tolerated: %v", err)
	}

	events, _ = store.Events(ctx, line)
	if len(events) != 2 {
		t.Fatalf("retry must not append another event, got %d", len(events))
	}
	commands := bus.All()
	if len(commands) != 1 || commands[0].Setpoint != 4 {
		t.Fatalf("bus current command must remain 4, got %+v", commands)
	}
	// The doser physically moved once per change (2 then 4); the late retry of
	// 2 must not move it a third time.
	if pub.driveCount(cmd2.ID) != 1 || pub.driveCount(cmd4.ID) != 1 {
		t.Fatalf("each change must drive the device exactly once: %+v", pub.drives)
	}
}

func TestRecoveredChangesRebuildBusWithoutDoubleDriving(t *testing.T) {
	ctx := context.Background()
	svc, bus, store, _ := newService(t)
	now := time.Unix(100, 0).UTC()
	batchID := uuid.New()
	line := model.LineID("line-1")

	rate2, _ := dosing.NewChange(line, batchID, "pac", 2, 1, now)
	rate4, _ := dosing.NewChange(line, batchID, "pac", 4, 1, now.Add(time.Second))
	if _, err := svc.ChangeRate(ctx, rate2, now); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ChangeRate(ctx, rate4, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	// Replay into a fresh service/bus exactly as startup recovery does.
	recoveredBus := process.NewDeviceBus()
	recovered, err := dosing.NewService(store, newDrivenPublisher(recoveredBus), dosing.NewLedger())
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.Events(ctx, line)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if err := recovered.ApplyRecovered(ctx, event); err != nil {
			t.Fatalf("replay: %v", err)
		}
	}
	got := recoveredBus.All()
	if len(got) != 1 || got[0].Setpoint != 4 {
		t.Fatalf("recovered bus must settle on 4, got %+v", got)
	}

	// Replaying twice (snapshot + log overlap) stays idempotent.
	for _, event := range events {
		if err := recovered.ApplyRecovered(ctx, event); err != nil {
			t.Fatalf("second replay: %v", err)
		}
	}
	if len(recoveredBus.All()) != 1 {
		t.Fatalf("idempotent replay changed bus: %+v", recoveredBus.All())
	}

	// Out-of-order replay (newer first, older later) still ends at 4.
	oooBus := process.NewDeviceBus()
	ooo, err := dosing.NewService(store, newDrivenPublisher(oooBus), dosing.NewLedger())
	if err != nil {
		t.Fatal(err)
	}
	for i := len(events) - 1; i >= 0; i-- {
		if err := ooo.ApplyRecovered(ctx, events[i]); err != nil {
			t.Fatalf("out-of-order replay: %v", err)
		}
	}
	if commands := oooBus.All(); len(commands) != 1 || commands[0].Setpoint != 4 {
		t.Fatalf("out-of-order replay must settle on 4, got %+v", commands)
	}
	_ = bus
}

func TestConcurrentSameGenerationChangesEndAtLastCommitted(t *testing.T) {
	ctx := context.Background()
	svc, bus, store, _ := newService(t)
	now := time.Unix(100, 0).UTC()
	batchID := uuid.New()
	line := model.LineID("line-1")

	const n = 25
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			change, err := dosing.NewChange(line, batchID, "pac", float64(i+1), 1, now.Add(time.Duration(i)*time.Millisecond))
			if err != nil {
				errs <- err
				return
			}
			if _, err := svc.ChangeRate(ctx, change, now.Add(time.Duration(i)*time.Millisecond)); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent change: %v", err)
	}

	events, err := store.Events(ctx, line)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != n {
		t.Fatalf("expected %d events, got %d", n, len(events))
	}
	var last model.Event
	for _, event := range events {
		if event.Sequence > last.Sequence {
			last = event
		}
	}
	var winner dosing.Change
	if err := last.Decode(&winner); err != nil {
		t.Fatal(err)
	}
	commands := bus.All()
	if len(commands) != 1 {
		t.Fatalf("expected one current command, got %d", len(commands))
	}
	if commands[0].Setpoint != winner.Rate || commands[0].Revision != last.Sequence {
		t.Fatalf("bus (%g @rev%d) must match last committed event (%g @seq%d)",
			commands[0].Setpoint, commands[0].Revision, winner.Rate, last.Sequence)
	}
}
