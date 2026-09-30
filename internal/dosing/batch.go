package dosing

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

type Change struct {
	ID          uuid.UUID    `json:"id"`
	LineID      model.LineID `json:"line_id"`
	BatchID     uuid.UUID    `json:"batch_id"`
	Chemical    string       `json:"chemical"`
	Rate        float64      `json:"rate"`
	Generation  uint64       `json:"generation"`
	RequestedAt time.Time    `json:"requested_at"`
}

func NewChange(lineID model.LineID, batchID uuid.UUID, chemical string, rate float64, generation uint64, now time.Time) (Change, error) {
	if lineID == "" || batchID == uuid.Nil || chemical == "" || rate <= 0 || generation == 0 {
		return Change{}, errors.New("dosing change is incomplete")
	}
	return Change{
		ID: uuid.New(), LineID: lineID, BatchID: batchID, Chemical: chemical,
		Rate: rate, Generation: generation, RequestedAt: now.UTC(),
	}, nil
}

// commandNamespace seeds the deterministic derivation of a device command ID
// from a dosing change ID. A change always maps to the same command, so a
// redelivery or a replay reuses the identity and the device bus can dedup it
// instead of driving the doser a second time.
var commandNamespace = uuid.MustParse("7d2c9a41-6b3e-4f2a-9c58-1f5e0a7d3b26")

func commandIDFor(changeID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(commandNamespace, []byte(changeID.String()))
}

// record pairs a change with its position within the generation. The position
// is the journal sequence of the event that committed the change, which is the
// authoritative total order shared by the event log and the device bus.
type record struct {
	change   Change
	revision uint64
}

type Ledger struct {
	mu      sync.RWMutex
	byBatch map[uuid.UUID][]record
	byID    map[uuid.UUID]record
}

func NewLedger() *Ledger {
	return &Ledger{
		byBatch: make(map[uuid.UUID][]record),
		byID:    make(map[uuid.UUID]record),
	}
}

// Lookup returns the committed record for a change ID, enabling a retried
// request to short-circuit without appending another event.
func (l *Ledger) Lookup(changeID uuid.UUID) (Change, uint64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	entry, ok := l.byID[changeID]
	if !ok {
		return Change{}, 0, false
	}
	return entry.change, entry.revision, true
}

// Commit records a change at the given revision (its journal sequence).
// Recording the same change ID again is idempotent and keeps the original
// revision, so replay or duplicate delivery never renumbers a change.
func (l *Ledger) Commit(change Change, revision uint64) (Change, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, ok := l.byID[change.ID]; ok {
		return existing.change, existing.revision
	}
	entry := record{change: change, revision: revision}
	l.byBatch[change.BatchID] = append(l.byBatch[change.BatchID], entry)
	l.byID[change.ID] = entry
	return change, revision
}

// Latest returns the change with the highest revision for the batch rather
// than the one that happened to be appended last, so out-of-order arrivals
// never surface an older rate as the current dose.
func (l *Ledger) Latest(batchID uuid.UUID) (Change, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	values := l.byBatch[batchID]
	if len(values) == 0 {
		return Change{}, false
	}
	latest := values[0]
	for _, value := range values[1:] {
		if value.revision > latest.revision {
			latest = value
		}
	}
	return latest.change, true
}
