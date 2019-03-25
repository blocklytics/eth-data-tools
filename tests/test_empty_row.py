import pytest
from ethdata import ethdata


class TestDatetimeError:

    def test_no_tx(self):
        my_contract = ethdata.Contract("0x1cB424cB77B19143825004d0bd0a4BEE2c5e91A8")
        my_contract.query_range = {"start":"2019-01-01", "end":"2019-01-01"}
        # no transactions for this contract
        assert len(my_contract.transaction_receipts) == 0
        assert len(my_contract.event_logs) == 0

    def test_tx(self):
        my_contract = ethdata.Contract("0x1f52b87C3503e537853e160adBF7E330eA0Be7C4")
        my_contract.query_range = {"start":"2019-01-29", "end":"2019-01-29"}
        # On this date, there were 14 normal tx and 2 internal tx
        assert len(my_contract.transaction_receipts) > 0
        assert len(my_contract.event_logs) > 0
       