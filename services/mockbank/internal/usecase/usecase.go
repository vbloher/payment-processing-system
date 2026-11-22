package usecase

import (
	"sync"
	"time"

	"github.com/vbloher/payment-processing-system/services/mockbank/internal/domain"
)

type Storage interface {
	Get(reference string) (domain.Transfer, bool)
	Save(t domain.Transfer)
	List(from, to time.Time) []domain.Transfer
	NextAttempt(reference string) int // 1 on the first call for a reference
	Available() bool
	SetAvailable(available bool)
}

type UseCase struct {
	mu      sync.Mutex // serialises all operations
	storage Storage
}

func New(storage Storage) *UseCase {
	return &UseCase{storage: storage}
}
