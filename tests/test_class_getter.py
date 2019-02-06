import pytest
from ethdata import ethdata
import datetime as dt
import json
import pkgutil

class TestAccountGetters(object):
    """Test cases:
        1. Multiple transactions
        2. No transactions
    """

    def test_getter_1_tx(self):
        my_account = ethdata.Account("0xa2381223639181689cd6c46d38a1a4884bb6d83c")
        my_account.query_range = {"start":"2019-01-29", "end":"2019-01-29"}
        # On this date, there were 14 normal tx and 2 internal tx
        assert len(my_account.transaction_receipts) == 14

    def test_getter_2_no_tx(self):
        my_account = ethdata.Account("0x1cB424cB77B19143825004d0bd0a4BEE2c5e91A8")
        my_account.query_range = {"start":"2019-01-01", "end":"2019-01-01"}
        assert len(my_account.transaction_receipts) == 0
        
class TestContractGetters(object):
    """Test cases:
        4. Contract created by normal tx, with ABI
        5. Contract created by internal tx, without ABI
        6. Invalid contract (normal address)
    """
        
    def test_getter_3_valid_contract_normal_deploy_with_abi(self):
        my_contract = ethdata.Contract("0x1f52b87C3503e537853e160adBF7E330eA0Be7C4")
        assert len(my_contract.abi) == 22
        assert len(my_contract.functions) == 14
        assert len(my_contract.events) == 6
        assert my_contract.creation_date == "2018-01-08"
        my_contract.query_range = {"start": "2019-01-28", "end": "2019-01-29"}
        # In this range, there were 2 normal tx and 1 internal tx
        assert len(my_contract.transaction_receipts) == 2
        # In this range, there were 2 events
        assert len(my_contract.event_logs) == 2
    
    def test_getter_4_valid_contract_internal_deploy_no_abi(self):
        my_contract = ethdata.Contract("0x85b530d1fe5c67696a8858c3b08379e5a5204a96")
        assert len(my_contract.abi) == 0
        assert len(my_contract.functions) == 0
        assert len(my_contract.events) == 0
        assert my_contract.creation_date == "2018-10-27"
        my_contract.query_range = {"start": "2018-11-15", "end": "2018-11-15"}
        # In this range, there was 1 normal tx and 1 internal tx
        assert len(my_contract.transaction_receipts) == 1
        # In this range, there were 2 events
        assert len(my_contract.event_logs) == 2
    
    def test_getter_5_invalid_contract(self):
        my_contract = ethdata.Contract("0x1cB424cB77B19143825004d0bd0a4BEE2c5e91A8")
        assert len(my_contract.abi) == 0
        with pytest.warns(UserWarning):
             assert my_contract.creation_date == "1970-01-01"
        my_contract.query_range = {"start":"2019-01-01", "end":"2019-01-01"}
        assert len(my_contract.transaction_receipts) == 0
        assert len(my_contract.event_logs) == 0

class TestTokenGetters(object):
    """Test cases:
        6. Valid address, token, compliant
        7. Valid address, token, exception list
        8. Valid address, token, fallback values
    """
    
    def test_getter_6_valid_token_compliant(self):
       my_token = ethdata.Token("0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2")
       assert my_token.address not in ethdata.exception_list
       assert my_token.name == "Maker"
       assert my_token.symbol == "MKR"
       assert my_token.decimals == 18.0
       assert my_token.total_supply == 1_000_000
       
    def test_getter_7_valid_token_exception_list(self):
       my_token = ethdata.Token("0xeb9951021698b42e4399f9cbb6267aa35f82d59d")
       assert my_token.address in ethdata.exception_list
       assert my_token.name == "Lif"
       assert my_token.symbol == "LIF"
       
    def test_getter_8_valid_token_fallback(self):
       my_token = ethdata.Token("0x9f8f72aa9304c8b593d555f12ef6589cc3a579a1")
       assert my_token.address not in ethdata.exception_list
       with pytest.warns(UserWarning):
           assert my_token.name == ""
       with pytest.warns(UserWarning):
           assert my_token.symbol == ""
       with pytest.warns(UserWarning):
           assert my_token.decimals == 18.0
       with pytest.warns(UserWarning):
           assert my_token.total_supply == 0.0

class TestEventLogsExceptions(object):
    """Test cases:
        9. One anonymous event
        10. Multiple anonymous events
    """
    
    def test_getter_9_anon_event(self):
        my_contract = ethdata.Contract("0x448a5065aebb8e423f0896e6c5d525c040f59af3")
        my_contract.query_range = {"start":"2018-11-06", "end":"2018-11-06"}
        assert len(my_contract.event_logs) == 800
    
    # def test_getter_10_anon_events(self):

class TestTransactionReceiptsExceptions(object):
    """Test cases:
        11. function without [inputs]
        12. transactions without data
    """
    
    # def test_getter_11_function_no_inputs(self):
    
    def test_getter_12_transaction_without_data(self):
        my_account = ethdata.Account("0xaa6c8f25f6027ff3fc27428ca86dc80202d702c0")
        my_account.query_range = {"start": "2019-01-31", "end": "2019-01-31"}
        assert len(my_account.transaction_receipts) == 3
        assert my_account.transaction_receipts.iloc[0]['function_signature'] == None
        assert my_account.transaction_receipts.iloc[0]['function_data'] == None