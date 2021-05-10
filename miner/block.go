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

// block.go implements block construction for the miner.
package miner

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/celo-org/celo-blockchain/common"
	"github.com/celo-org/celo-blockchain/contract_comm/currency"
	"github.com/celo-org/celo-blockchain/contract_comm/random"
	"github.com/celo-org/celo-blockchain/core"
	"github.com/celo-org/celo-blockchain/core/rawdb"
	"github.com/celo-org/celo-blockchain/core/types"
	"github.com/celo-org/celo-blockchain/log"
	"github.com/celo-org/celo-blockchain/params"
)

func createTxCmp() func(tx1 *types.Transaction, tx2 *types.Transaction) int {
	// TODO specify header & state
	currencyManager := currency.NewManager(nil, nil)

	return func(tx1 *types.Transaction, tx2 *types.Transaction) int {
		return currencyManager.CmpValues(tx1.GasPrice(), tx1.FeeCurrency(), tx2.GasPrice(), tx2.FeeCurrency())
	}
}

// newBlockState creates a new blockState that is initialized to be able to run transactions on.
func newBlockState(w *worker, timestamp int64) (*blockState, error) {
	parent := w.chain.CurrentBlock()

	if parent.Time() >= uint64(timestamp) {
		timestamp = int64(parent.Time() + 1)
	}
	num := parent.Number()
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		Extra:      w.extra,
		Time:       uint64(timestamp),
	}

	txFeeRecipient := w.txFeeRecipient
	if !w.chainConfig.IsDonut(header.Number) && w.txFeeRecipient != w.validator {
		txFeeRecipient = w.validator
		log.Warn("TxFeeRecipient and Validator flags set before split etherbase fork is active. Defaulting to the given validator address for the coinbase.")
	}

	if txFeeRecipient == (common.Address{}) {
		log.Error("Refusing to mine without etherbase")
		return nil, errors.New("Refusing to mine without etherbase")
	}
	header.Coinbase = txFeeRecipient

	// TODO: Don't sleep in engine.Prepare
	if err := w.engine.Prepare(w.chain, header); err != nil {
		log.Error("Failed to prepare header for mining", "err", err)
		return nil, fmt.Errorf("Failed to prepare header for mining %w", err)
	}

	state, err := w.chain.StateAt(parent.Root())
	if err != nil {
		log.Error("Failed to get parent state to create mining context", "err", err)
		return nil, err
	}

	env := &blockState{
		state:    state,
		header:   header,
		gasPool:  new(core.GasPool),
		gasLimit: core.CalcGasLimit(parent, state),
	}
	env.gasPool.AddGas(env.gasLimit)

	// Play our part in generating the random beacon.
	if random.IsRunning() {
		lastCommitment, err := random.GetLastCommitment(w.validator, env.header, env.state)
		if err != nil {
			log.Error("Failed to get last commitment", "err", err)
			return nil, err
		}

		lastRandomness := common.Hash{}
		if (lastCommitment != common.Hash{}) {
			lastRandomnessParentHash := rawdb.ReadRandomCommitmentCache(w.db, lastCommitment)
			if (lastRandomnessParentHash == common.Hash{}) {
				log.Error("Failed to get last randomness cache entry")
				return nil, err
			}

			var err error
			lastRandomness, _, err = w.engine.GenerateRandomness(lastRandomnessParentHash)
			if err != nil {
				log.Error("Failed to generate last randomness", "err", err)
				return nil, err
			}
		}

		_, newCommitment, err := w.engine.GenerateRandomness(env.header.ParentHash)
		if err != nil {
			log.Error("Failed to generate new randomness", "err", err)
			return nil, err
		}

		err = random.RevealAndCommit(lastRandomness, newCommitment, w.validator, env.header, env.state)
		if err != nil {
			log.Error("Failed to reveal and commit randomness", "randomness", lastRandomness.Hex(), "commitment", newCommitment.Hex(), "err", err)
			return nil, err
		}
		// always true (EIP158)
		env.state.IntermediateRoot(true)

		env.randomness = &types.Randomness{Revealed: lastRandomness, Committed: newCommitment}
	} else {
		env.randomness = &types.EmptyRandomness
	}
	return env, nil
}

// commitTransaction applies a single transaction to blockState.
func (b *blockState) commitTransaction(tx *types.Transaction, w *worker) ([]*types.Log, error) {
	snap := b.state.Snapshot()

	receipt, err := core.ApplyTransaction(w.chainConfig, w.chain, &w.txFeeRecipient, b.gasPool, b.state, b.header, tx, &b.header.GasUsed, *w.chain.GetVMConfig())
	if err != nil {
		b.state.RevertToSnapshot(snap)
		return nil, err
	}
	b.txs = append(b.txs, tx)
	b.receipts = append(b.receipts, receipt)

	return receipt.Logs, nil
}

// commitTransactions applies the transactions to blockState until the block is full our there are no more transactions.
func (b *blockState) commitTransactions(ctx context.Context, txs *types.TransactionsByPriceAndNonce, w *worker) {
	var coalescedLogs []*types.Log
	for {
		select {
		case <-ctx.Done():
			fmt.Println("context cancelled when committing transactions")
			return
		default:
		}

		// If we don't have enough gas for any further transactions then we're done
		if b.gasPool.Gas() < params.TxGas {
			log.Trace("Not enough gas for further transactions", "have", b.gasPool, "want", params.TxGas)
			return
		}
		// Retrieve the next transaction and abort if all done
		tx := txs.Peek()
		if tx == nil {
			return
		}
		// Short-circuit if the transaction requires more gas than we have in the pool.
		// If we didn't short-circuit here, we would get core.ErrGasLimitReached below.
		// Short-circuiting here saves us the trouble of checking the GPM and so on when the tx can't be included
		// anyway due to the block not having enough gas left.
		if b.gasPool.Gas() < tx.Gas() {
			log.Trace("Skipping transaction which requires more gas than is left in the block", "hash", tx.Hash(), "gas", b.gasPool.Gas(), "txgas", tx.Gas())
			txs.Pop()
			continue
		}
		// Error may be ignored here. The error has already been checked
		// during transaction acceptance is the transaction pool.
		//
		// We use the eip155 signer regardless of the current hf.
		from, _ := types.Sender(w.signer, tx)
		// Check whether the tx is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if tx.Protected() && !w.chainConfig.IsEIP155(b.header.Number) {
			log.Trace("Ignoring reply protected transaction", "hash", tx.Hash(), "eip155", w.chainConfig.EIP155Block)

			txs.Pop()
			continue
		}
		// Start executing the transaction
		b.state.Prepare(tx.Hash(), common.Hash{}, b.tcount)

		logs, err := b.commitTransaction(tx, w)
		switch {
		case errors.Is(err, core.ErrGasLimitReached):
			// Pop the current out-of-gas transaction without shifting in the next from the account
			log.Trace("Gas limit exceeded for current block", "sender", from)
			txs.Pop()

		case errors.Is(err, core.ErrNonceTooLow):
			// New head notification data race between the transaction pool and miner, shift
			log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
			txs.Shift()

		case errors.Is(err, core.ErrNonceTooHigh):
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Trace("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
			txs.Pop()

		case errors.Is(err, core.ErrGasPriceDoesNotExceedMinimum):
			// We are below the GPM, so we can stop (the rest of the transactions will either have
			// even lower gas price or won't be mineable yet due to their nonce)
			log.Trace("Skipping remaining transaction below the gas price minimum")
			return

		case errors.Is(err, nil):
			// Everything ok, collect the logs and shift in the next transaction from the same account
			coalescedLogs = append(coalescedLogs, logs...)
			b.tcount++
			txs.Shift()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Debug("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)
			txs.Shift()
		}
	}
}

// TxPool is the minimal interface that we need to have to select the transactions.
type TxPool interface {
	Pending() (map[common.Address]types.Transactions, error)
	Locals() []common.Address
}

// generateTransactionLists creates the potentially nil localTx & remoteTx sorted lists for transaction selection.
func generateTransactionLists(txPool TxPool, w *worker) (localTxs, remoteTxs *types.TransactionsByPriceAndNonce) {
	pending, err := txPool.Pending()
	if err != nil {
		fmt.Println("failed to get tx pool pending")
		log.Error("Failed to fetch pending transactions", "err", err)
		return nil, nil
	}

	// Short circuit if there is no available pending transactions.
	if len(pending) == 0 {
		fmt.Println("no pending txns")
		return nil, nil
	}
	// Split the pending transactions into locals and remotes
	localTxMap, remoteTxMap := make(map[common.Address]types.Transactions), pending
	for _, account := range txPool.Locals() {
		if txs := remoteTxMap[account]; len(txs) > 0 {
			delete(remoteTxMap, account)
			localTxMap[account] = txs
		}
	}

	txComparator := createTxCmp()
	// Create the transaction heaps
	if len(localTxMap) > 0 {
		localTxs = types.NewTransactionsByPriceAndNonce(w.signer, localTxMap, txComparator)
		fmt.Println("have local txs")
	}
	if len(remoteTxMap) > 0 {
		remoteTxs = types.NewTransactionsByPriceAndNonce(w.signer, remoteTxMap, txComparator)
		fmt.Println("have remote txs")
	}
	return localTxs, remoteTxs
}

// finalizeBlock runs any post-transaction state modifications and assembles the final block.
func (b *blockState) finalizeBlock(w *worker, start time.Time) (*task, error) {
	// Deep copy receipts here to avoid interaction between different tasks.
	receipts := make([]*types.Receipt, len(b.receipts))
	for i, l := range b.receipts {
		receipts[i] = new(types.Receipt)
		*receipts[i] = *l
	}
	state := b.state.Copy()

	block, err := w.engine.FinalizeAndAssemble(w.chain, b.header, state, b.txs, b.receipts, b.randomness)

	// Set the validator set diff in the new header if we're using Istanbul and it's the last block of the epoch
	if err := w.engine.UpdateValSetDiff(w.chain, block.MutableHeader(), state); err != nil {
		log.Error("Unable to update Validator Set Diff", "err", err)
		return nil, err
	}

	if len(b.state.GetLogs(common.Hash{})) > 0 {
		receipt := types.NewReceipt(nil, false, 0)
		receipt.Logs = state.GetLogs(common.Hash{})
		for i := range receipt.Logs {
			receipt.Logs[i].TxIndex = uint(len(b.receipts))
		}
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		b.receipts = append(b.receipts, receipt)
	}

	if err != nil {
		log.Error("Unable to finalize block", "err", err)
		return nil, err
	}

	feesWei := new(big.Int)
	for i, tx := range block.Transactions() {
		feesWei.Add(feesWei, new(big.Int).Mul(new(big.Int).SetUint64(b.receipts[i].GasUsed), tx.GasPrice()))
	}
	feesEth := new(big.Float).Quo(new(big.Float).SetInt(feesWei), new(big.Float).SetInt(big.NewInt(params.Ether)))

	log.Info("Created a new block to submit to consensus", "number", block.Number(), "sealhash", w.engine.SealHash(block.Header()),
		"txs", b.tcount, "gas", block.GasUsed(), "fees", feesEth, "elapsed", common.PrettyDuration(time.Since(start)))

	return &task{receipts: b.receipts, state: state, block: block, createdAt: time.Now()}, nil
}
