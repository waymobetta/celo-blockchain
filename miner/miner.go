// Copyright 2014 The go-ethereum Authors
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

// Package miner implements Ethereum block creation and mining.
package miner

import (
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/celo-org/celo-blockchain/accounts"
	"github.com/celo-org/celo-blockchain/common"
	"github.com/celo-org/celo-blockchain/common/hexutil"
	"github.com/celo-org/celo-blockchain/consensus"
	"github.com/celo-org/celo-blockchain/contract_comm/random"
	"github.com/celo-org/celo-blockchain/core"
	"github.com/celo-org/celo-blockchain/core/rawdb"
	"github.com/celo-org/celo-blockchain/core/state"
	"github.com/celo-org/celo-blockchain/core/types"
	"github.com/celo-org/celo-blockchain/eth/downloader"
	"github.com/celo-org/celo-blockchain/ethdb"
	"github.com/celo-org/celo-blockchain/event"
	"github.com/celo-org/celo-blockchain/log"
	"github.com/celo-org/celo-blockchain/params"
)

// Backend wraps all methods required for mining.
type Backend interface {
	AccountManager() *accounts.Manager
	BlockChain() *core.BlockChain
	TxPool() *core.TxPool
}

// Config is the configuration parameters of mining.
type Config struct {
	Validator           common.Address `toml:",omitempty"` // Public address for block signing and randomness (default = first account)
	Notify              []string       `toml:",omitempty"` // HTTP URL list to be notified of new work packages(only useful in ethash).
	ExtraData           hexutil.Bytes  `toml:",omitempty"` // Block extra data set by the miner
	GasFloor            uint64         // Target gas floor for mined blocks.
	GasCeil             uint64         // Target gas ceiling for mined blocks.
	GasPrice            *big.Int       // Minimum gas price for mining a transaction
	Recommit            time.Duration  // The time interval for miner to re-create mining work.
	Noverify            bool           // Disable remote mining solution verification(only useful in ethash).
	VerificationService string         // Celo verification service URL
}

// Miner creates blocks and searches for proof-of-work values.
type Miner struct {
	mux            *event.TypeMux
	worker         *worker
	validator      common.Address
	txFeeRecipient common.Address
	eth            Backend
	engine         consensus.Engine
	exitCh         chan struct{}
	db             ethdb.Database // Needed for randomness

	canStart    int32 // can start indicates whether we can start the mining operation
	shouldStart int32 // should start indicates whether we should start after sync
}

func New(eth Backend, config *Config, chainConfig *params.ChainConfig, mux *event.TypeMux, engine consensus.Engine, isLocalBlock func(block *types.Block) bool, db ethdb.Database) *Miner {
	miner := &Miner{
		eth:      eth,
		mux:      mux,
		engine:   engine,
		exitCh:   make(chan struct{}),
		worker:   newWorker(config, chainConfig, engine, eth, mux, isLocalBlock, db, true),
		db:       db,
		canStart: 1,
	}
	go miner.update()

	return miner
}

// update keeps track of the downloader events. Please be aware that this is a one shot type of update loop.
// It's entered once and as soon as `Done` or `Failed` has been broadcasted the events are unregistered and
// the loop is exited. This to prevent a major security vuln where external parties can DOS you with blocks
// and halt your mining operation for as long as the DOS continues.
func (miner *Miner) update() {
	events := miner.mux.Subscribe(downloader.StartEvent{}, downloader.DoneEvent{}, downloader.FailedEvent{})
	defer events.Unsubscribe()

	for {
		select {
		case ev := <-events.Chan():
			if ev == nil {
				return
			}
			switch ev.Data.(type) {
			case downloader.StartEvent:
				atomic.StoreInt32(&miner.canStart, 0)
				if miner.Mining() {
					miner.Stop()
					atomic.StoreInt32(&miner.shouldStart, 1)
					log.Info("Mining aborted due to sync")
				}
			case downloader.DoneEvent, downloader.FailedEvent:
				// If this is using the istanbul consensus engine, then we need to check
				// for the randomness cache for the randomness beacon protocol
				_, isIstanbul := miner.engine.(consensus.Istanbul)
				if isIstanbul {
					// getCurrentBlockAndState
					currentBlock := miner.eth.BlockChain().CurrentBlock()
					currentHeader := currentBlock.Header()
					currentState, err := miner.eth.BlockChain().StateAt(currentBlock.Root())
					if err != nil {
						log.Error("Error in retrieving state", "block hash", currentHeader.Hash(), "error", err)
						return
					}

					if currentHeader.Number.Uint64() > 0 {
						// Check to see if we already have the commitment cache
						lastCommitment, err := random.GetLastCommitment(miner.validator, currentHeader, currentState)
						if err != nil {
							log.Error("Error in retrieving last commitment", "error", err)
							return
						}

						// If there is a non empty last commitment and if we don't have that commitment's
						// cache entry, then we need to recover it.
						if (lastCommitment != common.Hash{}) && (rawdb.ReadRandomCommitmentCache(miner.db, lastCommitment) == common.Hash{}) {
							err := miner.eth.BlockChain().RecoverRandomnessCache(lastCommitment, currentBlock.Hash())
							if err != nil {
								log.Error("Error in recovering randomness cache", "error", err)
								return
							}
						}
					}
				}

				shouldStart := atomic.LoadInt32(&miner.shouldStart) == 1
				atomic.StoreInt32(&miner.canStart, 1)
				atomic.StoreInt32(&miner.shouldStart, 0)
				if shouldStart {
					miner.Start(miner.validator, miner.txFeeRecipient)
				}
				// stop immediately and ignore all further pending events
				return
			}
		case <-miner.exitCh:
			return
		}
	}
}

func (miner *Miner) Start(validator common.Address, txFeeRecipient common.Address) {
	atomic.StoreInt32(&miner.shouldStart, 1)
	miner.SetValidator(validator)
	miner.SetTxFeeRecipient(txFeeRecipient)

	if atomic.LoadInt32(&miner.canStart) == 0 {
		log.Info("Network syncing, will start miner afterwards")
		return
	}
	miner.worker.start()
}

func (miner *Miner) Stop() {
	miner.worker.stop()
	atomic.StoreInt32(&miner.shouldStart, 0)
}

func (miner *Miner) Close() {
	miner.worker.close()
	close(miner.exitCh)
}

func (miner *Miner) Mining() bool {
	return miner.worker.isRunning()
}

func (miner *Miner) HashRate() uint64 {
	if pow, ok := miner.engine.(consensus.PoW); ok {
		return uint64(pow.Hashrate())
	}
	return 0
}

func (miner *Miner) SetExtra(extra []byte) error {
	if uint64(len(extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("extra exceeds max length. %d > %v", len(extra), params.MaximumExtraDataSize)
	}
	miner.worker.setExtra(extra)
	return nil
}

// SetRecommitInterval sets the interval for sealing work resubmitting.
func (miner *Miner) SetRecommitInterval(interval time.Duration) {
	miner.worker.setRecommitInterval(interval)
}

// Pending returns the currently pending block and associated state.
func (miner *Miner) Pending() (*types.Block, *state.StateDB) {
	return miner.worker.pending()
}

// PendingBlock returns the currently pending block.
//
// Note, to access both the pending block and the pending state
// simultaneously, please use Pending(), as the pending state can
// change between multiple method calls
func (miner *Miner) PendingBlock() *types.Block {
	return miner.worker.pendingBlock()
}

// SetValidator sets the miner and worker's address for message and block signing
func (miner *Miner) SetValidator(addr common.Address) {
	miner.validator = addr
	miner.worker.setValidator(addr)
}

// SetTxFeeRecipient sets the address where the miner and worker will receive fees
func (miner *Miner) SetTxFeeRecipient(addr common.Address) {
	miner.txFeeRecipient = addr
	miner.worker.setTxFeeRecipient(addr)
}

// EnablePreseal turns on the preseal mining feature. It's enabled by default.
// Note this function shouldn't be exposed to API, it's unnecessary for users
// (miners) to actually know the underlying detail. It's only for outside project
// which uses this library.
func (miner *Miner) EnablePreseal() {
	miner.worker.enablePreseal()
}

// DisablePreseal turns off the preseal mining feature. It's necessary for some
// fake consensus engine which can seal blocks instantaneously.
// Note this function shouldn't be exposed to API, it's unnecessary for users
// (miners) to actually know the underlying detail. It's only for outside project
// which uses this library.
func (miner *Miner) DisablePreseal() {
	miner.worker.disablePreseal()
}

// SubscribePendingLogs starts delivering logs from pending transactions
// to the given channel.
func (miner *Miner) SubscribePendingLogs(ch chan<- []*types.Log) event.Subscription {
	return miner.worker.pendingLogsFeed.Subscribe(ch)
}
