import pytest
from ethdata import ethdata

class TestAccountSetters(object):
    def test_setter_1_address(self):
        my_account = ethdata.Account("0x1cB424cB77B19143825004d0bd0a4BEE2c5e91A8")
        assert my_account.address == "0x1cb424cb77b19143825004d0bd0a4bee2c5e91a8"
        with pytest.raises(ValueError):
            my_account.address = ""
    
    def test_setter_2_transaction_receipts(self):
        my_account = ethdata.Account("0x1cB424cB77B19143825004d0bd0a4BEE2c5e91A8")
        my_account.transaction_receipts = "tx"
        assert my_account.transaction_receipts == "tx"
    
    def test_setter_3_query_range(self):
        my_account = ethdata.Account("0x1cB424cB77B19143825004d0bd0a4BEE2c5e91A8")
        assert my_account.query_range == {}
        my_account.query_range = {"start": "2018-01-01", "end": "2018-01-02"}
        assert my_account.query_range == {"start": "2018-01-01", "end": "2018-01-02"}
        my_account.query_range = {"start": "2018-01-03"}
        assert my_account.query_range == {"start": "2018-01-03"}
        my_account.query_range = {"end": "2018-01-04"}
        assert my_account.query_range == {"end": "2018-01-04"}
        my_account.query_range = {"key": "value"}
        assert my_account.query_range == {}