// Package testutil contains deterministic infrastructure helpers shared by
// unit and integration tests.
package testutil

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

// TCPAddressReservations keeps loopback ports bound until the caller is ready
// to construct the server that will own them. Merely asking the kernel for an
// ephemeral port and immediately closing it permits that same port to be
// returned again before a multi-server test has started all of its listeners.
type TCPAddressReservations struct {
	mu        sync.Mutex
	addresses []string
	listeners []net.Listener
}

// ReserveLoopbackTCP binds count distinct IPv4 loopback ports.
func ReserveLoopbackTCP(count int) (*TCPAddressReservations, error) {
	if count < 0 {
		return nil, fmt.Errorf("reservation count must be non-negative, got %d", count)
	}
	reservations := &TCPAddressReservations{
		addresses: make([]string, count),
		listeners: make([]net.Listener, count),
	}
	for index := 0; index < count; index++ {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			_ = reservations.Close()
			return nil, fmt.Errorf("reserve loopback TCP address %d: %w", index, err)
		}
		reservations.addresses[index] = listener.Addr().String()
		reservations.listeners[index] = listener
	}
	return reservations, nil
}

// Address returns the address held at index. It remains unavailable to another
// listener until Release or Close is called.
func (r *TCPAddressReservations) Address(index int) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index < 0 || index >= len(r.addresses) {
		return "", fmt.Errorf("reservation index %d out of range", index)
	}
	return r.addresses[index], nil
}

// Release closes one reservation immediately before the real server binds it.
func (r *TCPAddressReservations) Release(index int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index < 0 || index >= len(r.listeners) {
		return fmt.Errorf("reservation index %d out of range", index)
	}
	if r.listeners[index] == nil {
		return fmt.Errorf("reservation index %d already released", index)
	}
	err := r.listeners[index].Close()
	r.listeners[index] = nil
	if err != nil {
		return fmt.Errorf("release reservation index %d: %w", index, err)
	}
	return nil
}

// Close releases every reservation that has not already been handed off.
func (r *TCPAddressReservations) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for index, listener := range r.listeners {
		if listener == nil {
			continue
		}
		if err := listener.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close reservation index %d: %w", index, err))
		}
		r.listeners[index] = nil
	}
	return errors.Join(errs...)
}
