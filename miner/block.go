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
	"github.com/celo-org/celo-blockchain/consensus"
	"github.com/celo-org/celo-blockchain/contract_comm/currency"
	"github.com/celo-org/celo-blockchain/contract_comm/random"
	"github.com/celo-org/celo-blockchain/core"
	"github.com/celo-org/celo-blockchain/core/rawdb"
	"github.com/celo-org/celo-blockchain/core/state"
	"github.com/celo-org/celo-blockchain/core/types"
	"github.com/celo-org/celo-blockchain/ethdb"
	"github.com/celo-org/celo-blockchain/log"
	"github.com/celo-org/celo-blockchain/params"
)

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

// chainState is a read only collection of state required to create a block.
type chainState struct {
	signer      types.Signer // Used to check the signature, not to sign. Could create just once?
	chainConfig *params.ChainConfig
	engine      consensus.Istanbul
	chain       *core.BlockChain
	db          ethdb.Database // Needed for randomness
}

// workerState is a collection of worker defined inputs to the block production process.
// Assumed to be not concurently modified.
type workerState struct {
	validator      common.Address
	txFeeRecipient common.Address
	extra          []byte
}

func createTxCmp() func(tx1 *types.Transaction, tx2 *types.Transaction) int {
	// TODO specify header & state
	currencyManager := currency.NewManager(nil, nil)

	return func(tx1 *types.Transaction, tx2 *types.Transaction) int {
		return currencyManager.CmpValues(tx1.GasPrice(), tx1.FeeCurrency(), tx2.GasPrice(), tx2.FeeCurrency())
	}
}

// newBlockState creates a new blockState that is initialized to be able to run transactions on.
func newBlockState(c *chainState, w *workerState, timestamp int64) (*blockState, error) {
	parent := c.chain.CurrentBlock()

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
	if !c.chainConfig.IsDonut(header.Number) && w.txFeeRecipient != w.validator {
		txFeeRecipient = w.validator
		log.Warn("TxFeeRecipient and Validator flags set before split etherbase fork is active. Defaulting to the given validator address for the coinbase.")
	}

	if txFeeRecipient == (common.Address{}) {
		log.Error("Refusing to mine without etherbase")
		return nil, errors.New("Refusing to mine without etherbase")
	}
	header.Coinbase = txFeeRecipient

	// TODO: Don't sleep in engine.Prepare
	if err := c.engine.Prepare(c.chain, header); err != nil {
		log.Error("Failed to prepare header for mining", "err", err)
		return nil, fmt.Errorf("Failed to prepare header for mining %w", err)
	}

	state, err := c.chain.StateAt(parent.Root())
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
			lastRandomnessParentHash := rawdb.ReadRandomCommitmentCache(c.db, lastCommitment)
			if (lastRandomnessParentHash == common.Hash{}) {
				log.Error("Failed to get last randomness cache entry")
				return nil, err
			}

			var err error
			lastRandomness, _, err = c.engine.GenerateRandomness(lastRandomnessParentHash)
			if err != nil {
				log.Error("Failed to generate last randomness", "err", err)
				return nil, err
			}
		}

		_, newCommitment, err := c.engine.GenerateRandomness(env.header.ParentHash)
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
func (b *blockState) commitTransaction(tx *types.Transaction, c *chainState, txFeeRecipient common.Address) ([]*types.Log, error) {
	snap := b.state.Snapshot()

	receipt, err := core.ApplyTransaction(c.chainConfig, c.chain, &txFeeRecipient, b.gasPool, b.state, b.header, tx, &b.header.GasUsed, *c.chain.GetVMConfig())
	if err != nil {
		b.state.RevertToSnapshot(snap)
		return nil, err
	}
	b.txs = append(b.txs, tx)
	b.receipts = append(b.receipts, receipt)

	return receipt.Logs, nil
}

// commitTransactions applies the transactions to blockState until the block is full our there are no more transactions.
func (b *blockState) commitTransactions(ctx context.Context, txs *types.TransactionsByPriceAndNonce, c *chainState, w *workerState) {
	var coalescedLogs []*types.Log
	for {
		select {
		case <-ctx.Done():
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
		from, _ := types.Sender(c.signer, tx)
		// Check whether the tx is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if tx.Protected() && !c.chainConfig.IsEIP155(b.header.Number) {
			log.Trace("Ignoring reply protected transaction", "hash", tx.Hash(), "eip155", c.chainConfig.EIP155Block)

			txs.Pop()
			continue
		}
		// Start executing the transaction
		b.state.Prepare(tx.Hash(), common.Hash{}, b.tcount)

		logs, err := b.commitTransaction(tx, c, w.txFeeRecipient)
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
func generateTransactionLists(txPool TxPool, c *chainState) (localTxs, remoteTxs *types.TransactionsByPriceAndNonce) {
	pending, err := txPool.Pending()
	if err != nil {
		log.Error("Failed to fetch pending transactions", "err", err)
		return nil, nil
	}

	// Short circuit if there is no available pending transactions.
	if len(pending) == 0 {
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
		localTxs = types.NewTransactionsByPriceAndNonce(c.signer, localTxMap, txComparator)
	}
	if len(remoteTxMap) > 0 {
		remoteTxs = types.NewTransactionsByPriceAndNonce(c.signer, remoteTxMap, txComparator)
	}
	return localTxs, remoteTxs
}

// finalizeBlock runs any post-transaction state modifications and assembles the final block.
func (b *blockState) finalizeBlock(c *chainState, start time.Time) (*task, error) {
	block, err := c.engine.FinalizeAndAssemble(c.chain, b.header, b.state, b.txs, b.receipts, b.randomness)

	// Set the validator set diff in the new header if we're using Istanbul and it's the last block of the epoch
	if err := c.engine.UpdateValSetDiff(c.chain, block.MutableHeader(), b.state); err != nil {
		log.Error("Unable to update Validator Set Diff", "err", err)
		return nil, err
	}

	if len(b.state.GetLogs(common.Hash{})) > 0 {
		receipt := types.NewReceipt(nil, false, 0)
		receipt.Logs = b.state.GetLogs(common.Hash{})
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

	log.Info("Created a new block to submit to consensus", "number", block.Number(), "sealhash", c.engine.SealHash(block.Header()),
		"txs", b.tcount, "gas", block.GasUsed(), "fees", feesEth, "elapsed", common.PrettyDuration(time.Since(start)))

	return &task{receipts: b.receipts, state: b.state, block: block, createdAt: time.Now()}, nil
}
