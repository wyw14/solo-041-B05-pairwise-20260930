package process

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/wyw14/cry-97/internal/model"
)

// ErrStaleCommand mirrors model.ErrStaleDeviceCommand so callers in this
// package can match it without reaching into model.
var ErrStaleCommand = model.ErrStaleDeviceCommand

// ErrCommandConflict mirrors model.ErrDeviceCommandConflict.
var ErrCommandConflict = model.ErrDeviceCommandConflict

type DeviceBus struct {
	mu       sync.RWMutex
	commands map[string]model.DeviceCommand
}

func NewDeviceBus() *DeviceBus {
	return &DeviceBus{commands: make(map[string]model.DeviceCommand)}
}

// Publish accepts a command only when it is newer than what the bus already
// holds for the device. Ordering is (generation, revision): generation moves
// on an emergency switch, while revision orders commands issued within the
// same generation. A redelivered command (same identity at the same position)
// is a legitimate retry and is accepted as an idempotent no-op so the device
// is never driven twice.
func (b *DeviceBus) Publish(ctx context.Context, command model.DeviceCommand) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if command.DeviceID == "" || command.Generation == 0 {
		return errors.New("device command is incomplete")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, exists := b.commands[command.DeviceID]
	if exists {
		switch comparePosition(command, current) {
		case -1:
			// Older generation, or an earlier revision within this generation:
			// a late command must never roll back a newer setting.
			return model.ErrStaleDeviceCommand
		case 0:
			// Same position: only the identical command may land here.
			if command.ID != current.ID {
				return model.ErrDeviceCommandConflict
			}
			return nil
		}
	}
	b.commands[command.DeviceID] = command
	return nil
}

// comparePosition reports whether command is older (-1), equal (0) or newer
// (+1) than current on the (generation, revision) ordering.
func comparePosition(command, current model.DeviceCommand) int {
	switch {
	case command.Generation < current.Generation:
		return -1
	case command.Generation > current.Generation:
		return 1
	case command.Revision < current.Revision:
		return -1
	case command.Revision > current.Revision:
		return 1
	default:
		return 0
	}
}

func (b *DeviceBus) All() []model.DeviceCommand {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]model.DeviceCommand, 0, len(b.commands))
	for _, command := range b.commands {
		result = append(result, command)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].LineID != result[j].LineID {
			return result[i].LineID < result[j].LineID
		}
		if result[i].Generation != result[j].Generation {
			return result[i].Generation < result[j].Generation
		}
		if result[i].Revision != result[j].Revision {
			return result[i].Revision < result[j].Revision
		}
		return result[i].IssuedAt.Before(result[j].IssuedAt)
	})
	return result
}

// StopLine moves every command for the line onto the emergency generation so
// that late commands from the previous generation are rejected as stale. The
// revision is carried forward so ordering within the new generation never
// regresses either.
func (b *DeviceBus) StopLine(lineID model.LineID, generation uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, command := range b.commands {
		if command.LineID != lineID {
			continue
		}
		if generation < command.Generation {
			continue
		}
		command.Generation = generation
		command.Action = "stop"
		command.Setpoint = 0
		b.commands[id] = command
	}
}
