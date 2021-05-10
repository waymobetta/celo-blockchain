// Copyright 2021 The go-ethereum Authors
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

// worker2.go runs a blocking block creation and insert attempt.
// Note that the block that is created may not be the one agreed on if the validator
// is not the proposer for the round.
package miner

import (
	"context"
	"fmt"
	"time"

	"github.com/celo-org/celo-blockchain/common"
	"github.com/celo-org/celo-blockchain/core"
	"github.com/celo-org/celo-blockchain/core/types"
	"github.com/celo-org/celo-blockchain/log"
)

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

func (w *worker) insertBlock(block *types.Block, task *task) {
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
