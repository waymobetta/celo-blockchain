// Copyright 2021 The Celo Authors
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

package contract_comm

import (
	"github.com/celo-org/celo-blockchain/contract_comm/caller"
	"github.com/celo-org/celo-blockchain/contract_comm/election2"
	"github.com/celo-org/celo-blockchain/contract_comm/validators2"
	"github.com/celo-org/celo-blockchain/core/vm"
)

type ContractComm struct {
	validators.ValidatorsComm
	election.ElectionComm
}

func New(chain vm.ChainContext) (ContractComm, error) {
	caller, err := caller.New(chain)
	if err != nil {
		return ContractComm{}, err
	}
	comm := ContractComm{
		ValidatorsComm: validators.ValidatorsComm{ContractCaller: caller},
		ElectionComm:   election.ElectionComm{ContractCaller: caller},
	}
	return comm, nil
}
