// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"math/big"
	"testing"

	"github.com/celo-org/celo-blockchain/common"
	"github.com/celo-org/celo-blockchain/consensus/istanbul"
)

// notice: the normal case have been tested in integration tests.
func TestHandleMsg(t *testing.T) {
	N := uint64(4)
	F := uint64(1)
	sys := NewTestSystemWithBackend(N, F)

	closer := sys.Run(true)
	defer closer()

	v0 := sys.backends[0]
	r0 := v0.engine.(*core)

	m, _ := Encode(&istanbul.Subject{
		View: &istanbul.View{
			Sequence: big.NewInt(0),
			Round:    big.NewInt(0),
		},
		Digest: common.BytesToHash([]byte("1234567890")),
	})
	// with a matched payload. istanbul.MsgPreprepare should match with *istanbul.Preprepare in normal case.
	msg := &istanbul.Message{
		Code:      istanbul.MsgPreprepare,
		Msg:       m,
		Address:   v0.Address(),
		Signature: []byte{},
	}

	_, val := v0.Validators(nil).GetByAddress(v0.Address())
	if err := r0.handleCheckedMsg(msg, val); err != errFailedDecodePreprepare {
		t.Errorf("error mismatch: have %v, want %v", err, errFailedDecodePreprepare)
	}

	m, _ = Encode(&istanbul.Preprepare{
		View: &istanbul.View{
			Sequence: big.NewInt(0),
			Round:    big.NewInt(0),
		},
		Proposal: makeBlock(1),
	})
	// with a unmatched payload. istanbul.MsgPrepare should match with *istanbul.Subject in normal case.
	msg = &istanbul.Message{
		Code:      istanbul.MsgPrepare,
		Msg:       m,
		Address:   v0.Address(),
		Signature: []byte{},
	}

	_, val = v0.Validators(nil).GetByAddress(v0.Address())
	if err := r0.handleCheckedMsg(msg, val); err != errFailedDecodePrepare {
		t.Errorf("error mismatch: have %v, want %v", err, errFailedDecodePreprepare)
	}

	m, _ = Encode(&istanbul.Preprepare{
		View: &istanbul.View{
			Sequence: big.NewInt(0),
			Round:    big.NewInt(0),
		},
		Proposal: makeBlock(2),
	})
	// with a unmatched payload. istanbul.MsgCommit should match with *istanbul.Subject in normal case.
	msg = &istanbul.Message{
		Code:      istanbul.MsgCommit,
		Msg:       m,
		Address:   v0.Address(),
		Signature: []byte{},
	}

	_, val = v0.Validators(nil).GetByAddress(v0.Address())
	if err := r0.handleCheckedMsg(msg, val); err != errFailedDecodeCommit {
		t.Errorf("error mismatch: have %v, want %v", err, errFailedDecodeCommit)
	}

	m, _ = Encode(&istanbul.Preprepare{
		View: &istanbul.View{
			Sequence: big.NewInt(0),
			Round:    big.NewInt(0),
		},
		Proposal: makeBlock(3),
	})
	// invalid message code. message code is not exists in list
	msg = &istanbul.Message{
		Code:      uint64(99),
		Msg:       m,
		Address:   v0.Address(),
		Signature: []byte{},
	}

	_, val = v0.Validators(nil).GetByAddress(v0.Address())
	if err := r0.handleCheckedMsg(msg, val); err == nil {
		t.Errorf("error mismatch: have %v, want nil", err)
	}

	// with malicious payload
	if err := r0.handleMsg([]byte{1}); err == nil {
		t.Errorf("error mismatch: have %v, want nil", err)
	}
}

func BenchmarkHandleMsg(b *testing.B) {
	N := uint64(2)
	F := uint64(1) // F does not affect tests

	sys := NewMutedTestSystemWithBackend(N, F)
	// sys := NewTestSystemWithBackend(N, F)

	for i, backend := range sys.backends {
		c := backend.engine.(*core)

		c.current = newTestRoundState(
			&istanbul.View{
				Round:    big.NewInt(0),
				Sequence: big.NewInt(1),
			},
			backend.peers,
		)

		if i == 0 {
			// replica 0 is the proposer
			c.current.(*roundStateImpl).state = StatePreprepared
		}
	}

	sys.Run(false)

	v0 := sys.backends[0]
	v1 := sys.backends[1]
	c := v0.engine.(*core)
	sub := v0.engine.(*core).current.Subject()

	payload, _ := Encode(sub)
	msg := istanbul.Message{
		Code: 1000,
		Msg:  payload,
	}

	msg, _ = v1.finalizeAndReturnMessage(&msg)
	payload, _ = c.finalizeMessage(&msg)

	// benchmarked portion
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c.handleMsg(payload)
		if err != errInvalidMessage {
			b.Errorf("error mismatch: have %v, want errInvalidMessage", err)
		}
	}
}

// newInitializedTestSystem creates a test system
// It optionally creates a round state db in a temporary directory
func newInitializedTestSystem(b *testing.B, useRoundStateDB bool) *testSystem {
	N := uint64(2)
	F := uint64(1) // F does not affect tests

	sys := NewMutedTestSystemWithBackend(N, F)
	// sys := NewTestSystemWithBackend(N, F)

	for i, backend := range sys.backends {
		c := backend.engine.(*core)

		if useRoundStateDB {
			rsdb, err := newRoundStateDB(b.TempDir(), nil)
			if err != nil {
				b.Errorf("Failed to create rsdb: %v", err)
			}

			c.current = withSavingDecorator(rsdb, newTestRoundState(
				&istanbul.View{
					Round:    big.NewInt(0),
					Sequence: big.NewInt(1),
				},
				backend.peers,
			))

			if i == 0 {
				// replica 0 is the proposer
				c.current.(*rsSaveDecorator).rs.(*roundStateImpl).state = StatePreprepared
			}
		} else {
			c.current = newTestRoundState(
				&istanbul.View{
					Round:    big.NewInt(0),
					Sequence: big.NewInt(1),
				},
				backend.peers,
			)
			if i == 0 {
				// replica 0 is the proposer
				c.current.(*roundStateImpl).state = StatePreprepared
			}
		}
	}

	sys.Run(false)
	return sys
}

func BenchmarkE2EHandleCommit(b *testing.B) {
	sys := newInitializedTestSystem(b, false)
	bemchmarkE2EHandleCommit(b, sys)
}

func BenchmarkE2EHandleCommitWithSave(b *testing.B) {
	sys := newInitializedTestSystem(b, true)
	bemchmarkE2EHandleCommit(b, sys)

}

func bemchmarkE2EHandleCommit(b *testing.B, sys *testSystem) {

	v0 := sys.backends[0]
	v1 := sys.backends[1]
	c := v0.engine.(*core)
	subject := v0.engine.(*core).current.Subject()

	committedSeal, err := v0.engine.(*core).generateCommittedSeal(subject)
	if err != nil {
		b.Errorf("Got error: %v", err)
	}
	committedSubject := &istanbul.CommittedSubject{
		Subject:       subject,
		CommittedSeal: committedSeal[:],
	}
	payload, err := Encode(committedSubject)
	if err != nil {
		b.Errorf("Got error: %v", err)
	}

	msg := istanbul.Message{
		Code: istanbul.MsgCommit,
		Msg:  payload,
	}

	msg, err = v1.finalizeAndReturnMessage(&msg)
	if err != nil {
		b.Errorf("Got error: %v", err)
	}
	payload, _ = c.finalizeMessage(&msg)
	if err != nil {
		b.Errorf("Got error: %v", err)
	}

	// benchmarked portion
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c.handleMsg(payload)
		if err != nil {
			b.Errorf("error mismatch: have %v, want nil", err)
		}
	}
}

func BenchmarkE2EHandlePrepareWithSave(b *testing.B) {
	sys := newInitializedTestSystem(b, true)
	benchmarkE2EHandlePrepare(b, sys)
}

func BenchmarkE2EHandlePrepare(b *testing.B) {
	sys := newInitializedTestSystem(b, false)
	benchmarkE2EHandlePrepare(b, sys)

}

func benchmarkE2EHandlePrepare(b *testing.B, sys *testSystem) {
	v0 := sys.backends[0]
	v1 := sys.backends[1]
	c := v0.engine.(*core)
	sub := v0.engine.(*core).current.Subject()

	payload, _ := Encode(sub)
	msg := istanbul.Message{
		Code: istanbul.MsgPrepare,
		Msg:  payload,
	}

	msg, _ = v1.finalizeAndReturnMessage(&msg)
	payload, _ = c.finalizeMessage(&msg)

	// benchmarked portion
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := c.handleMsg(payload)
		if err != nil {
			b.Errorf("error mismatch: have %v, want nil", err)
		}
	}
}
