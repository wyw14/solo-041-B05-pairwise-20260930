package dosing

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

type Change struct {
	ID         uuid.UUID    `json:"id"`
	LineID     model.LineID `json:"line_id"`
	BatchID    uuid.UUID    `json:"batch_id"`
	Chemical   string       `json:"chemical"`
	Rate       float64      `json:"rate"`
	Generation uint64       `json:"generation"`
	// Sequence is the journal sequence the change was committed at. It is the
	// authoritative order of dosing settings and drives the command revision.
	Sequence    uint64    `json:"sequence,omitempty"`
	RequestedAt time.Time `json:"requested_at"`
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

type Ledger struct {
	mu      sync.RWMutex
	changes map[uuid.UUID][]Change
}

func NewLedger() *Ledger {
	return &Ledger{changes: make(map[uuid.UUID][]Change)}
}

// changeOrder reports whether a precedes b as the older setting for a batch.
// The journal sequence is authoritative; changes committed before sequences
// were recorded fall back to generation and request time, with the change ID
// as a deterministic final tie-breaker.
func changeOrder(a, b Change) bool {
	if a.Sequence != 0 && b.Sequence != 0 {
		return a.Sequence < b.Sequence
	}
	if a.Generation != b.Generation {
		return a.Generation < b.Generation
	}
	if !a.RequestedAt.Equal(b.RequestedAt) {
		return a.RequestedAt.Before(b.RequestedAt)
	}
	return a.ID.String() < b.ID.String()
}

// Record stores a change, dropping an already-seen change ID so retries and
// event replay cannot duplicate or reorder the ledger. The slice is kept in
// application order regardless of the order changes arrive.
func (l *Ledger) Record(change Change) {
	l.mu.Lock()
	defer l.mu.Unlock()
	values := l.changes[change.BatchID]
	for _, existing := range values {
		if existing.ID == change.ID {
			return
		}
	}
	values = append(values, change)
	sort.Slice(values, func(i, j int) bool { return changeOrder(values[i], values[j]) })
	l.changes[change.BatchID] = values
}

func (l *Ledger) Latest(batchID uuid.UUID) (Change, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	values := l.changes[batchID]
	if len(values) == 0 {
		return Change{}, false
	}
	return values[len(values)-1], true
}
