import pytest
from ethdata import ethdata
import datetime as dt

class TestAccountInit(object):
    """Test cases:
        1. Invalid address (length, characters)
        2. Valid address, external account
    """

    def test_init_1_invalid_address(self):
        with pytest.raises(ValueError):
            my_account = ethdata.Account("0x0")
        with pytest.raises(ValueError):
            my_account = ethdata.Account("0x0X0X0X0X0X0X0X0X0X0X0X0X0X0X0X0X0X0X0X0X")
        with pytest.raises(ValueError):
            my_account = ethdata.Account("a2381223639181689cd6c46d38a1a4884bb6d83c")
        
    def test_init_2_valid_address(self):
        my_account = ethdata.Account("0xa2381223639181689cd6c46d38a1a4884bb6d83c")
        assert my_account.address == "0xa2381223639181689cd6c46d38a1a4884bb6d83c"
        assert my_account.query_range == {}

    def test_init_3_valid_caps_address(self):
        my_account = ethdata.Account("0x1cB424cB77B19143825004d0bd0a4BEE2c5e91A8")
        assert my_account.address == "0x1cb424cb77b19143825004d0bd0a4bee2c5e91a8"
        assert my_account.query_range == {}
