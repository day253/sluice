package testutil

import (
	"net"
	"testing"
)

func TestTCPAddressReservationsHoldUniquePortsUntilRelease(t *testing.T) {
	const count = 32
	reservations, err := ReserveLoopbackTCP(count)
	if err != nil {
		t.Fatal(err)
	}
	defer reservations.Close()

	seen := make(map[string]struct{}, count)
	for index := 0; index < count; index++ {
		address, err := reservations.Address(index)
		if err != nil {
			t.Fatal(err)
		}
		if _, duplicate := seen[address]; duplicate {
			t.Fatalf("address %s was reserved more than once", address)
		}
		seen[address] = struct{}{}
		if listener, listenErr := net.Listen("tcp4", address); listenErr == nil {
			listener.Close()
			t.Fatalf("reserved address %s was bindable before release", address)
		}
	}

	releasedAddress, err := reservations.Address(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservations.Release(0); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", releasedAddress)
	if err != nil {
		t.Fatalf("released address %s was not bindable: %v", releasedAddress, err)
	}
	listener.Close()
}
