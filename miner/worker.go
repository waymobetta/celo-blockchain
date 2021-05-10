// Copyright 2015 The go-ethereum Authors
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

package miner

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/celo-org/celo-blockchain/common"
	"github.com/celo-org/celo-blockchain/consensus"
	"github.com/celo-org/celo-blockchain/core"
	"github.com/celo-org/celo-blockchain/core/state"
	"github.com/celo-org/celo-blockchain/core/types"
	"github.com/celo-org/celo-blockchain/ethdb"
	"github.com/celo-org/celo-blockchain/event"
	"github.com/celo-org/celo-blockchain/log"
	"github.com/celo-org/celo-blockchain/metrics"
	"github.com/celo-org/celo-blockchain/params"
)

const (
	// resultQueueSize is the size of channel listening to sealing result.
	resultQueueSize = 10
	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10
)

// Gauge used to measure block finalization time from created to after written to chain.
var blockFinalizationTimeGauge = metrics.NewRegisteredGauge("miner/block/finalizationTime", nil)

// task contains all information for consensus engine sealing and result submitting.
type task struct {
	receipts  []*types.Receipt
	state     *state.StateDB
	block     *types.Block
	createdAt time.Time
	cancel    context.CancelFunc // call to abort engine.Seal for this request

}

// blockState is the state associated with an in flight block.
// It is not thread safe, and should only be used once.
type blockState struct {
	state    *state.StateDB // apply state changes here
	tcount   int            // tx count in cycle
	gasPool  *core.GasPool  // available gas used to pack transactions
	gasLimit uint64

	header     *types.Header
	txs        []*types.Transaction
	receipts   []*types.Receipt
	randomness *types.Randomness // The types.Randomness of the last block by mined by this worker.
}

type IstanbulSealer interface {
	Prepare(chain consensus.ChainReader, header *types.Header) error
	FinalizeAndAssemble(chain consensus.ChainReader, header *types.Header, state *state.StateDB, txs []*types.Transaction, receipts []*types.Receipt, randomness *types.Randomness) (*types.Block, error)
	Seal(chain consensus.ChainReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error
	SealHash(header *types.Header) common.Hash
	GenerateRandomness(parentHash common.Hash) (common.Hash, common.Hash, error)
	UpdateValSetDiff(chain consensus.ChainReader, header *types.Header, state *state.StateDB) error
}

// worker is the main object which takes care of submitting new work to consensus engine
// and gathering the sealing result.
type worker struct {
	// Chain state info
	signer      types.Signer // Used to check the signature, not to sign
	chainConfig *params.ChainConfig
	engine      IstanbulSealer
	chain       *core.BlockChain
	db          ethdb.Database // Needed for randomness
	eth         Backend        // for eth.TxPool()

	// Validator Info
	mu             sync.RWMutex // The lock used to protect the validator, txFeeRecipient and extra fields
	validator      common.Address
	txFeeRecipient common.Address
	extra          []byte

	// Feeds
	pendingLogsFeed event.Feed

	// Subscriptions
	mux          *event.TypeMux
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription

	// Channels
	resultCh chan *types.Block
	startCh  chan struct{}
	exitCh   chan struct{}

	pendingMu    sync.RWMutex
	pendingTasks map[common.Hash]*task

	snapshotMu    sync.RWMutex // The lock used to protect the block snapshot and state snapshot
	snapshotBlock *types.Block
	snapshotState *state.StateDB

	running         bool // The indicator whether the consensus engine is running or not.
	validatorCancel context.CancelFunc

	blockConstructGauge metrics.Gauge
}

func newWorker(config *Config, chainConfig *params.ChainConfig, engine IstanbulSealer, eth Backend, mux *event.TypeMux, db ethdb.Database) *worker {
	worker := &worker{
		signer:      types.NewEIP155Signer(chainConfig.ChainID),
		chainConfig: chainConfig,
		engine:      engine,
		chain:       eth.BlockChain(),
		db:          db,
		eth:         eth,

		validator: config.Validator,
		// txFeeRecipient: config.TxFeeRecipient, // TODO: make this part of the config
		extra: config.ExtraData,

		mux:         mux,
		chainHeadCh: make(chan core.ChainHeadEvent, chainHeadChanSize),

		resultCh: make(chan *types.Block, resultQueueSize),
		exitCh:   make(chan struct{}),
		startCh:  make(chan struct{}, 1),

		pendingTasks:        make(map[common.Hash]*task),
		blockConstructGauge: metrics.NewRegisteredGauge("miner/worker/block_construct", nil),
	}
	// Subscribe events for blockchain
	worker.chainHeadSub = eth.BlockChain().SubscribeChainHeadEvent(worker.chainHeadCh)

	fmt.Println("created a worker")

	return worker
}

// validator loop is launched when mining begins
// Note: need to figure out how to cancel everything
func (w *worker) validatorLoop(ctx context.Context) {
	fmt.Println("started validator loop")
	for {
		select {
		case <-w.startCh:
			// Send FinalCommittedEvent to the IBFT engine
			if h, ok := w.engine.(consensus.Handler); ok {
				h.NewWork()
			}
			// Run the next task
			go w.runBlockCreationThroughSealing(ctx)
		case <-w.chainHeadCh:
			// Send FinalCommittedEvent to the IBFT engine
			if h, ok := w.engine.(consensus.Handler); ok {
				h.NewWork()
			}
			// Run the next task
			go w.runBlockCreationThroughSealing(ctx)
		// runBlockCreationThroughSealing submits a task to engine.Seal which returns to w.resultCh
		case block := <-w.resultCh:
			go w.insertBlock(block)
		case <-ctx.Done():
			return
		}
	}

}

// Note: the context must be cancelled to close engine.Seal
func (w *worker) runBlockCreationThroughSealing(ctx context.Context) {
	b, _ := newBlockState(w, time.Now().Unix())
	// newBlockState sleeps, so put a cancel point here
	select {
	case <-ctx.Done():
		return
	default:
	}

	localTxs, remoteTxs := generateTransactionLists(w.eth.TxPool(), w)
	// Each round of tx commit is also a cancel point
	if localTxs != nil {
		b.commitTransactions(ctx, localTxs, w)
		fmt.Println("committed local transactions")

	}
	if remoteTxs != nil {
		b.commitTransactions(ctx, remoteTxs, w)
		fmt.Println("committed remote transactions")

	}
	task, err := b.finalizeBlock(w, time.Now())
	fmt.Println("finalized block")
	if err != nil {
		// TODO: fix
		panic(err)
	}
	w.updateSnapshot(b)

	sealCtx, cancel := context.WithCancel(ctx)
	task.cancel = cancel

	// Tie this seal request to the context
	if err := w.engine.Seal(w.chain, task.block, w.resultCh, sealCtx.Done()); err != nil {
		log.Warn("Block sealing failed", "err", err)
	}
	fmt.Println("set off seal")

	// Concurrently store task
	sealHash := w.engine.SealHash(task.block.Header())
	w.pendingMu.Lock()
	w.pendingTasks[sealHash] = task
	w.pendingMu.Unlock()

}

func (w *worker) insertBlock(block *types.Block) {
	// Short circuit when receiving empty result.
	if block == nil {
		return
	}
	// Short circuit when receiving duplicate result caused by resubmitting.
	if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
		return
	}
	var (
		sealhash = w.engine.SealHash(block.Header())
		hash     = block.Hash()
	)
	w.pendingMu.RLock()
	task, exist := w.pendingTasks[sealhash]
	w.pendingMu.RUnlock()
	if !exist {
		log.Error("Block found but no relative pending task", "number", block.Number(), "sealhash", sealhash, "hash", hash)
		return
	}
	// Different block could share same sealhash, deep copy here to prevent write-write conflict.
	var (
		receipts = make([]*types.Receipt, len(task.receipts))
		logs     []*types.Log
	)
	for i, receipt := range task.receipts {
		// add block location fields
		receipt.BlockHash = hash
		receipt.BlockNumber = block.Number()
		receipt.TransactionIndex = uint(i)

		receipts[i] = new(types.Receipt)
		*receipts[i] = *receipt
		// Update the block hash in all logs since it is now available and not when the
		// receipt/log of individual transactions were created.
		for _, log := range receipt.Logs {
			log.BlockHash = hash
			// Handle block finalization receipt
			if (log.TxHash == common.Hash{}) {
				log.TxHash = hash
			}
		}
		logs = append(logs, receipt.Logs...)
	}
	// Commit block and state to database.
	_, err := w.chain.WriteBlockWithState(block, receipts, logs, task.state, true)
	if err != nil {
		log.Error("Failed writing block to chain", "err", err)
		return
	}
	blockFinalizationTimeGauge.Update(time.Now().UnixNano() - int64(block.Time())*1000000000)
	log.Info("Successfully sealed new block", "number", block.Number(), "sealhash", sealhash, "hash", hash,
		"elapsed", common.PrettyDuration(time.Since(task.createdAt)))

	// Broadcast the block and announce chain insertion event
	w.mux.Post(core.NewMinedBlockEvent{Block: block})
}

// setValidator sets the validator address that signs messages and commits randomness
func (w *worker) setValidator(addr common.Address) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.validator = addr
}

// setTxFeeRecipient sets the address to receive tx fees, stored in header.Coinbase
func (w *worker) setTxFeeRecipient(addr common.Address) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.txFeeRecipient = addr
}

// setExtra sets the content used to initialize the block extra field.
func (w *worker) setExtra(extra []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.extra = extra
}

// pending returns the pending state and corresponding block.
func (w *worker) pending() (*types.Block, *state.StateDB) {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	if w.snapshotState == nil {
		return nil, nil
	}
	return w.snapshotBlock, w.snapshotState.Copy()
}

// updateSnapshot updates pending snapshot block and state.
// Note this function assumes the current variable is thread safe.
func (w *worker) updateSnapshot(b *blockState) {
	w.snapshotMu.Lock()
	defer w.snapshotMu.Unlock()

	w.snapshotBlock = types.NewBlock(
		b.header,
		b.txs,
		b.receipts,
		b.randomness,
	)

	w.snapshotState = b.state.Copy()
}

// pendingBlock returns pending block.
func (w *worker) pendingBlock() *types.Block {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	return w.snapshotBlock
}

// start sets the running status as 1 and triggers new work submitting.
func (w *worker) start() {
	if w.running {
		return
	}
	w.running = true
	w.startCh <- struct{}{}
	go w.validatorLoop(context.Background())

}

// stop sets the running status as 0.
func (w *worker) stop() {
	if !w.running {
		return
	}
	w.running = false
}

// isRunning returns an indicator whether worker is running or not.
func (w *worker) isRunning() bool {
	return w.running
}

// close terminates all background threads maintained by the worker.
// Note the worker does not support being closed multiple times.
func (w *worker) close() {
	w.running = false
	close(w.exitCh)
}

// copyReceipts makes a deep copy of the given receipts.
func copyReceipts(receipts []*types.Receipt) []*types.Receipt {
	result := make([]*types.Receipt, len(receipts))
	for i, l := range receipts {
		cpy := *l
		result[i] = &cpy
	}
	return result
}

// totalFees computes total consumed fees in ETH. Block transactions and receipts have to have the same order.
func totalFees(block *types.Block, receipts []*types.Receipt) *big.Float {
	feesWei := new(big.Int)
	for i, tx := range block.Transactions() {
		feesWei.Add(feesWei, new(big.Int).Mul(new(big.Int).SetUint64(receipts[i].GasUsed), tx.GasPrice()))
	}
	return new(big.Float).Quo(new(big.Float).SetInt(feesWei), new(big.Float).SetInt(big.NewInt(params.Ether)))
}
