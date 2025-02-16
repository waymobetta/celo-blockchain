// Copyright 2017 The Celo Authors
// This file is part of the celo library.
//
// The celo library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The celo library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the celo library. If not, see <http://www.gnu.org/licenses/>.

package currency

import (
	"math/big"
	"strings"

	"github.com/celo-org/celo-blockchain/accounts/abi"
	"github.com/celo-org/celo-blockchain/common"
	"github.com/celo-org/celo-blockchain/contract_comm"
	"github.com/celo-org/celo-blockchain/contract_comm/errors"
	"github.com/celo-org/celo-blockchain/core/types"
	"github.com/celo-org/celo-blockchain/core/vm"
	"github.com/celo-org/celo-blockchain/log"
	"github.com/celo-org/celo-blockchain/params"
)

const (
	// This is taken from celo-monorepo/packages/protocol/build/<env>/contracts/SortedOracles.json
	medianRateABI = `[
    {
      "constant": true,
      "inputs": [
        {
          "name": "token",
          "type": "address"
        }
      ],
      "name": "medianRate",
      "outputs": [
        {
          "name": "",
          "type": "uint128"
        },
        {
          "name": "",
          "type": "uint128"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    }]`

	// This is taken from celo-monorepo/packages/protocol/build/<env>/contracts/ERC20.json
	balanceOfABI = `[{"constant": true,
                          "inputs": [
                               {
                                   "name": "who",
                                   "type": "address"
                               }
                          ],
                          "name": "balanceOf",
                          "outputs": [
                               {
                                   "name": "",
                                   "type": "uint256"
                               }
                          ],
                          "payable": false,
                          "stateMutability": "view",
                          "type": "function"
                         }]`

	// This is taken from celo-monorepo/packages/protocol/build/<env>/contracts/FeeCurrency.json
	getWhitelistABI = `[{"constant": true,
	                     "inputs": [],
	                     "name": "getWhitelist",
	                     "outputs": [
			          {
			              "name": "",
				      "type": "address[]"
				  }
			     ],
			     "payable": false,
			     "stateMutability": "view",
			     "type": "function"
			    }]`
)

var (
	medianRateFuncABI, _   = abi.JSON(strings.NewReader(medianRateABI))
	balanceOfFuncABI, _    = abi.JSON(strings.NewReader(balanceOfABI))
	getWhitelistFuncABI, _ = abi.JSON(strings.NewReader(getWhitelistABI))
)

// NoopExchangeRate represents an exchange rate of 1 to 1
var NoopExchangeRate = ExchangeRate{common.Big1, common.Big1}

var CELOCurrency = Currency{
	Address:    common.ZeroAddress,
	toCELORate: NoopExchangeRate,
}

// Currency represent a system currency
// than can be converted to CELO
// Two currencies are deemed equal if they have the same address
type Currency struct {
	Address    common.Address
	toCELORate ExchangeRate
}

// ToCELO converts an currency's token amount to a CELO amount
func (c *Currency) ToCELO(tokenAmount *big.Int) *big.Int {
	return c.toCELORate.ToBase(tokenAmount)
}

// FromCELO converts an CELO amount to a currency tokens amount
func (c *Currency) FromCELO(celoAmount *big.Int) *big.Int {
	return c.toCELORate.FromBase(celoAmount)
}

// CmpToCurrency compares a currency amount to an amount in a different currency
func (c *Currency) CmpToCurrency(currencyAmount *big.Int, sndCurrencyAmount *big.Int, sndCurrency *Currency) int {
	if c == sndCurrency || c.Address == sndCurrency.Address {
		return currencyAmount.Cmp(sndCurrencyAmount)
	}

	// Below code block is basically evaluating this comparison:
	// currencyAmount * c.toCELORate.denominator / c.toCELORate.numerator < sndCurrencyAmount * sndCurrency.toCELORate.denominator / sndCurrency.toCELORate.numerator
	// It will transform that comparison to this, to remove having to deal with fractional values.
	// currencyAmount * c.toCELORate.denominator * sndCurrency.toCELORate.numerator < sndCurrencyAmount * sndCurrency.toCELORate.denominator * c.toCELORate.numerator
	leftSide := new(big.Int).Mul(
		currencyAmount,
		new(big.Int).Mul(
			c.toCELORate.denominator,
			sndCurrency.toCELORate.numerator,
		),
	)
	rightSide := new(big.Int).Mul(
		sndCurrencyAmount,
		new(big.Int).Mul(
			sndCurrency.toCELORate.denominator,
			c.toCELORate.numerator,
		),
	)

	return leftSide.Cmp(rightSide)
}

// ExchangeRate represent the exchangeRate [Base -> Token]
// Follows the equation: 1 base * ExchangeRate = X token
type ExchangeRate struct {
	numerator   *big.Int
	denominator *big.Int
}

// NewExchangeRate creates an exchange rate.
// Requires numerator >=0 && denominator >= 0
func NewExchangeRate(numerator *big.Int, denominator *big.Int) (*ExchangeRate, error) {
	if numerator == nil || common.Big0.Cmp(numerator) >= 0 {
		return nil, errors.ErrExchangeRateZero
	}
	if denominator == nil || common.Big0.Cmp(denominator) >= 0 {
		return nil, errors.ErrExchangeRateZero
	}
	return &ExchangeRate{numerator, denominator}, nil
}

// ToBase converts from token to base
func (er *ExchangeRate) ToBase(tokenAmount *big.Int) *big.Int {
	return new(big.Int).Div(new(big.Int).Mul(tokenAmount, er.denominator), er.numerator)
}

// FromGold converts from base to token
func (er *ExchangeRate) FromBase(goldAmount *big.Int) *big.Int {
	return new(big.Int).Div(new(big.Int).Mul(goldAmount, er.numerator), er.denominator)
}

// CurrencyManager provides an interface to access different fee currencies on a given point in time (header,state)
// and doing comparison or fetching exchange rates
//
// It's implements an internal cache to avoid perfoming duplicated EVM calls
type CurrencyManager struct {
	header *types.Header
	state  vm.StateDB

	currencyCache    map[common.Address]*Currency                                            // map of exchange rates of the form (CELO, token)
	_getExchangeRate func(*common.Address, *types.Header, vm.StateDB) (*ExchangeRate, error) // function to obtain exchange rate from blockchain state
}

// NewManager creates a new CurrencyManager
func NewManager(header *types.Header, state vm.StateDB) *CurrencyManager {
	return newManager(GetExchangeRate, header, state)
}

func newManager(_getExchangeRate func(*common.Address, *types.Header, vm.StateDB) (*ExchangeRate, error), header *types.Header, state vm.StateDB) *CurrencyManager {
	return &CurrencyManager{
		header:           header,
		state:            state,
		currencyCache:    make(map[common.Address]*Currency),
		_getExchangeRate: _getExchangeRate,
	}
}

// GetCurrency retrieves fee currency
func (cc *CurrencyManager) GetCurrency(currencyAddress *common.Address) (*Currency, error) {
	if currencyAddress == nil {
		return &CELOCurrency, nil
	}

	val, ok := cc.currencyCache[*currencyAddress]
	if ok {
		return val, nil
	}

	currencyExchangeRate, err := cc._getExchangeRate(currencyAddress, cc.header, cc.state)
	if err != nil {
		return nil, err
	}

	val = &Currency{
		Address:    *currencyAddress,
		toCELORate: *currencyExchangeRate,
	}

	cc.currencyCache[*currencyAddress] = val

	return val, nil
}

// CmpValues compares values of potentially different currencies
func (cc *CurrencyManager) CmpValues(val1 *big.Int, currencyAddr1 *common.Address, val2 *big.Int, currencyAddr2 *common.Address) int {
	// Short circuit if the fee currency is the same. nil currency => native currency
	if (currencyAddr1 == nil && currencyAddr2 == nil) || (currencyAddr1 != nil && currencyAddr2 != nil && *currencyAddr1 == *currencyAddr2) {
		return val1.Cmp(val2)
	}

	currency1, err1 := cc.GetCurrency(currencyAddr1)
	currency2, err2 := cc.GetCurrency(currencyAddr2)

	if err1 != nil || err2 != nil {
		currency1Output := "nil"
		if currencyAddr1 != nil {
			currency1Output = currencyAddr1.Hex()
		}
		currency2Output := "nil"
		if currencyAddr2 != nil {
			currency2Output = currencyAddr2.Hex()
		}
		log.Warn("Error in retrieving exchange rate.  Will do comparison of two values without exchange rate conversion.", "currency1", currency1Output, "err1", err1, "currency2", currency2Output, "err2", err2)
		return val1.Cmp(val2)
	}

	return currency1.CmpToCurrency(val1, val2, currency2)
}

// GetExchangeRate retrieves currency-to-CELO exchange rate
func GetExchangeRate(currencyAddress *common.Address, header *types.Header, state vm.StateDB) (*ExchangeRate, error) {
	if currencyAddress == nil {
		return &NoopExchangeRate, nil
	}

	var returnArray [2]*big.Int
	err := contract_comm.MakeStaticCall(params.SortedOraclesRegistryId, medianRateFuncABI, "medianRate", []interface{}{currencyAddress}, &returnArray, params.MaxGasForMedianRate, header, state)

	if err == errors.ErrSmartContractNotDeployed {
		log.Warn("Registry address lookup failed", "err", err)
		return &NoopExchangeRate, nil
	} else if err != nil {
		log.Error("medianRate invocation error", "feeCurrencyAddress", currencyAddress.Hex(), "err", err)
		return &NoopExchangeRate, nil
	}

	log.Trace("medianRate invocation success", "feeCurrencyAddress", currencyAddress, "returnArray", returnArray)
	return NewExchangeRate(returnArray[0], returnArray[1])
}

// GetBalanceOf returns an account's balance on a given ERC20 currency
func GetBalanceOf(accountOwner common.Address, contractAddress common.Address, header *types.Header, state vm.StateDB) (result *big.Int, err error) {
	log.Trace("GetBalanceOf() Called", "accountOwner", accountOwner.Hex(), "contractAddress", contractAddress)

	err = contract_comm.MakeStaticCallWithAddress(contractAddress, balanceOfFuncABI, "balanceOf", []interface{}{accountOwner}, &result, params.MaxGasToReadErc20Balance, header, state)
	if err != nil {
		log.Error("GetBalanceOf evm invocation error", "err", err)
	} else {
		log.Trace("GetBalanceOf evm invocation success", "accountOwner", accountOwner.Hex(), "Balance", result.String())
	}

	return result, err
}

// CurrencyWhitelist retrieves the list of currencies that can be used to pay transaction fees
func CurrencyWhitelist(header *types.Header, state vm.StateDB) ([]common.Address, error) {
	returnList := []common.Address{}

	err := contract_comm.MakeStaticCall(params.FeeCurrencyWhitelistRegistryId, getWhitelistFuncABI, "getWhitelist", []interface{}{}, &returnList, params.MaxGasForGetWhiteList, header, state)

	if err == errors.ErrSmartContractNotDeployed {
		log.Warn("Registry address lookup failed", "err", err)
	} else if err != nil {
		log.Error("getWhitelist invocation failed", "err", err)
	} else {
		log.Trace("getWhitelist invocation success")
	}

	return returnList, err
}

// IsWhitelisted indicates if a currency is whitelisted for transaction fee payments
func IsWhitelisted(feeCurrency *common.Address, header *types.Header, state vm.StateDB) bool {
	if feeCurrency == nil {
		return true
	}

	whitelistedCurrencies, err := CurrencyWhitelist(header, state)
	if err != nil {
		return true
	}

	for _, addr := range whitelistedCurrencies {
		if addr == *feeCurrency {
			return true
		}
	}
	return false
}
