package memory

import (
	"slices"
	"sync"
	"time"

	"github.com/vbloher/payment-processing-system/services/mockbank/internal/domain"
)

// Storage is the in-memory state of the bank.
type Storage struct {
	mu        sync.Mutex
	transfers map[string]domain.Transfer
	attempts  map[string]int
	available bool
}

// New returns a storage of an available bank.
func New() *Storage {
	return &Storage{
		transfers: make(map[string]domain.Transfer),
		attempts:  make(map[string]int),
		available: true,
	}
}

func (s *Storage) Get(reference string) (domain.Transfer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.transfers[reference]

	return t, ok
}

func (s *Storage) Save(t domain.Transfer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.transfers[t.Reference] = t
}

// List returns the transfers created within [from, to], oldest first.
func (s *Storage) List(from, to time.Time) []domain.Transfer {
	s.mu.Lock()
	defer s.mu.Unlock()

	var res []domain.Transfer

	for _, t := range s.transfers {
		if !t.CreatedAt.Before(from) && !t.CreatedAt.After(to) {
			res = append(res, t)
		}
	}

	slices.SortFunc(res, func(a, b domain.Transfer) int {
		return a.CreatedAt.Compare(b.CreatedAt)
	})

	return res
}

func (s *Storage) NextAttempt(reference string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.attempts[reference]++

	return s.attempts[reference]
}

func (s *Storage) Available() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.available
}

func (s *Storage) SetAvailable(available bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.available = available
}
