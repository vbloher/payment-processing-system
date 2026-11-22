package domain

// FailureReason explains why a payment failed.
type FailureReason string

const (
	ReasonNone              FailureReason = ""
	ReasonInsufficientFunds FailureReason = "insufficient_funds"
	ReasonAccountNotFound   FailureReason = "account_not_found"
	ReasonLedgerRejected    FailureReason = "ledger_rejected"
	ReasonBankDeclined      FailureReason = "bank_declined"
	ReasonBankCancelled     FailureReason = "bank_cancelled"
)

// BankOutcome is the definitive state of a bank transfer.
type BankOutcome string

const (
	BankSucceeded BankOutcome = "succeeded"
	BankDeclined  BankOutcome = "declined"
	BankCancelled BankOutcome = "cancelled"
)

// BankTransfer is the bank's view of a transfer.
type BankTransfer struct {
	ID        string
	Reference string
	Amount    int64
	Currency  string
	Status    BankOutcome
}

// HoldState is the state of a ledger hold as the payment service sees it.
type HoldState string

const (
	HoldAbsent   HoldState = ""
	HoldActive   HoldState = "active"
	HoldCaptured HoldState = "captured"
	HoldReleased HoldState = "released"
)
