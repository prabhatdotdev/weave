package brokerstress

import (
	"errors"
	"testing"
)

func TestLedgerTracksOnlyUnfinishedConfirmedDeliveries(t *testing.T) {
	for _, beforeConfirmation := range []bool{false, true} {
		l := ledger{pending: make(map[string]delivery)}
		l.register("message")
		if beforeConfirmation {
			l.see("message")
		}
		l.confirm("message", nil)
		if !beforeConfirmation && !l.contains("message") {
			t.Fatal("confirmed, undelivered message was lost")
		}
		l.see("message")
		l.see("message")
		if l.count() != 0 {
			t.Fatal("completed deliveries or duplicates grew the ledger")
		}
		l.register("uncertain")
		l.confirm("uncertain", errors.New("connection lost"))
		l.see("uncertain")
		if l.count() != 0 {
			t.Fatal("uncertain publish created a false delivery obligation")
		}
	}
}
