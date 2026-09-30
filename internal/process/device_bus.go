package process

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/wyw14/cry-97/internal/model"
)

type DeviceBus struct {
	mu       sync.RWMutex
	commands map[string]model.DeviceCommand
}

func NewDeviceBus() *DeviceBus {
	return &DeviceBus{commands: make(map[string]model.DeviceCommand)}
}

// Publish applies a command to its device. Commands are totally ordered per
// device by (generation, revision, issued at): a late command that is older
// than the one already applied is rejected with model.ErrStaleDeviceCommand and
// never overwrites newer settings.
//
// The boolean reports whether the command actually drove the device. A
// re-delivered command carrying the same ID as the command already in place is
// a legal retry: it is accepted as a success but reports false so callers do
// not drive the device or record the side effects a second time.
func (b *DeviceBus) Publish(ctx context.Context, command model.DeviceCommand) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if command.DeviceID == "" || command.Generation == 0 || command.Revision == 0 {
		return false, errors.New("device command is incomplete")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, exists := b.commands[command.DeviceID]
	if exists {
		if !command.NewerThan(current) {
			// A redelivery is idempotent only when it is byte-for-byte the
			// same applied command. The ID alone is not enough: an emergency
			// stop rewrites the generation in place, so a replayed pre-stop
			// command keeps the ID yet must be rejected as stale.
			if command.ID == current.ID && command.Generation == current.Generation && command.Revision == current.Revision {
				return false, nil
			}
			return false, model.ErrStaleDeviceCommand
		}
	}
	b.commands[command.DeviceID] = command
	return true, nil
}

func (b *DeviceBus) All() []model.DeviceCommand {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]model.DeviceCommand, 0, len(b.commands))
	for _, command := range b.commands {
		result = append(result, command)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].DeviceID != result[j].DeviceID {
			return result[i].DeviceID < result[j].DeviceID
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

// StopLine isolates every device on a line at the emergency generation. The
// switch is one-way and idempotent: commands already at that generation or a
// newer one are left untouched, so replaying an emergency (or delivering it
// twice) can never downgrade a device back to an older setting.
func (b *DeviceBus) StopLine(lineID model.LineID, generation uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, command := range b.commands {
		if command.LineID != lineID || command.Generation >= generation {
			continue
		}
		command.Generation = generation
		command.Revision = 1
		command.Action = "stop"
		command.Setpoint = 0
		b.commands[id] = command
	}
}
