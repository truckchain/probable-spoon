import json
import web3

from web3 import Web3, HTTPProvider
from solc import compile_source
from web3.contract import ConciseContract

import time

import sys
sys.path.append("./../refactored-train/")
path = "./../refactored-train/contracts/carrierRegistry.sol"
contract_source_code = open(path,'r').read()

# Solidity source code
#contract_source_code = ''' '''

def connect_to_chain(contract_address):
    compiled_sol = compile_source(contract_source_code) # Compiled source code
    contract_interface = compiled_sol['<stdin>:CarrierRegistry']

    # web3.py instance
    w3 = Web3(HTTPProvider('http://localhost:8545'))

    # Instantiate and deploy contract
    contract = w3.eth.contract(contract_interface['abi'], bytecode=contract_interface['bin'])

    # Contract instance in concise mode
    contract_instance = w3.eth.contract(contract_interface['abi'], contract_address, ContractFactoryClass=ConciseContract)

    print("Contract connected successfully")
    print("CarrierName: ", str(contract_instance.getCarrierName()))
    # print("CarrierQuality: "+str(contract_instance.getCarrierQuality()))

    return w3, contract_instance

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

    print("Contract deployed successfully")

    return contract_instance, tx_receipt
