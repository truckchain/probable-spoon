#! blockChain_contract provides functions for deploying and connecting to a
#! block chain based on the web3py 3.16.4 version.
#!
#! Copyright (c) 2017 Mayur Andulkar
#!
#! Permission is hereby granted, free of charge, to any person obtaining a copy
#! of this software and associated documentation files (the "Software"), to deal
#! in the Software without restriction, including without limitation the rights
#! to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#! copies of the Software, and to permit persons to whom the Software is
#! furnished to do so, subject to the following conditions:
#
#! The above copyright notice and this permission notice shall be included in all
#! copies or substantial portions of the Software.
#!
#! THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#! IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#! FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#! AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#! LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#! OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#! SOFTWARE.

import json
import web3

from web3 import Web3, HTTPProvider
from solc import compile_source
from web3.contract import Contract
import numpy as np
import datetime

import time

import sys
sys.path.append("./../refactored-train/")
path = "./../refactored-train/contracts/carrierRegistry.sol"
contract_source_code = open(path,'r').read()

def connect_to_chain(contract_address):
    compiled_sol = compile_source(contract_source_code) # Compiled source code
    contract_interface = compiled_sol['<stdin>:CarrierRegistry']

    # web3.py instance
    w3 = Web3(HTTPProvider('http://localhost:8545'))

    # Instantiate and deploy contract
    contract = w3.eth.contract(contract_interface['abi'], bytecode=contract_interface['bin'])

    # Contract instance in concise mode
    contract_instance = w3.eth.contract(contract_interface['abi'], contract_address, ContractFactoryClass=Contract)

    print(str(np.datetime64(datetime.datetime.now()))+" Initializing new trip for carrier : "+str(contract_instance.call().getCarrierName()))

    return w3, contract_instance


## If a new contrat has to be deployed
def deploy_contract():

    compiled_sol = compile_source(contract_source_code) # Compiled source code
    contract_interface = compiled_sol['<stdin>:CarrierRegistry']

    # web3.py instance
    w3 = Web3(HTTPProvider('http://localhost:8545'))

    # Instantiate and deploy contract
    contract = w3.eth.contract(contract_interface['abi'], bytecode=contract_interface['bin'])

    # Get transaction hash from deployed contract
    tx_hash = contract.deploy(transaction={'from': w3.eth.accounts[0], 'gas': 4100000 }, args=[0,0])
    # print(tx_hash)
    time.sleep(30)
    # Get tx receipt to get contract address
    tx_receipt = w3.eth.getTransactionReceipt(tx_hash)
    # print(tx_receipt)
    contract_address = tx_receipt['contractAddress']

    # Contract instance in concise mode
    contract_instance = w3.eth.contract(contract_interface['abi'], contract_address, ContractFactoryClass=ConciseContract)

    print(str(np.datetime64(datetime.datetime.now()))+" New Contract deployed successfully")

    return contract_instance, tx_receipt
