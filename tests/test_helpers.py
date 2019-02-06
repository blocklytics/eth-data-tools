import pytest
from ethdata import ethdata

class TestHelpers(object):

    def test_keccack_hash_function_signature_method(self):
        my_function = "name()"
        assert ethdata.get_function_signature(my_function) == "0x06fdde03"

    def test_keccack_hash_event_method(self):
        my_event = "Transfer(address,address,uint256)"
        assert ethdata.get_event_hash(my_event) == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    def test_hex_to_address_method(self):
        assert ethdata.hex_to_address("2e6236591bfa37c683ce60d6cfde40396a114ff1") == "0x2e6236591bfa37c683ce60d6cfde40396a114ff1"
        assert ethdata.hex_to_address("0000000000000000000000002e6236591bfa37c683ce60d6cfde40396a114ff1") == "0x2e6236591bfa37c683ce60d6cfde40396a114ff1"
        assert ethdata.hex_to_address("0x0000000000000000000000002e6236591bfa37c683ce60d6cfde40396a114ff1") == "0x2e6236591bfa37c683ce60d6cfde40396a114ff1"

    def test_hex_to_string_method(self):
        assert ethdata.hex_to_string("516d554a445672414b313567626747545366367061336b51394757786f536e384e4a663579445832375962334741") == "QmUJDVrAK15gbgGTSf6pa3kQ9GWxoSn8NJf5yDX27Yb3GA"
        assert ethdata.hex_to_string("464f414d00000000000000000000000000000000000000000000000000000000") == "FOAM"
        assert ethdata.hex_to_string("0x464f414d00000000000000000000000000000000000000000000000000000000") == "FOAM"
        assert ethdata.hex_to_string("5343484150000000000000000000000000000000000000000000000000000000") == "SCHAP"

    def test_hex_to_float_method(self):
        assert ethdata.hex_to_float(1_000_000_000_000_000_000, decimals=18) == 1
        assert ethdata.hex_to_float(0.123) == 0.123
        assert ethdata.hex_to_float("0xdeadbeef") == 3_735_928_559
        assert ethdata.hex_to_float("deadbeef") == 3_735_928_559
        assert ethdata.hex_to_float(10) != ethdata.hex_to_float("10")
        assert ethdata.hex_to_float("0x0000000000000000000000000000000000000000033b2e3c9fd0803ce8000000") == 1e+27
        
    def clean_hex_data(self):
        assert ethdata.clean_hex_data("2e6236591bfa37c683ce60d6cfde40396a114ff1", "address") == "0x2e6236591bfa37c683ce60d6cfde40396a114ff1"
        assert ethdata.clean_hex_data("0xdeadbeef", "uint256") == 3_735_928_559
        assert ethdata.clean_hex_data("0xdeadbeef", "uint8") == 3_735_928_559
        assert ethdata.clean_hex_data("5343484150000000000000000000000000000000000000000000000000000000", "string") == "SCHAP"
        with pytest.warns(UserWarning):
             assert ethdata.clean_hex_data("0xdeadbeef", "unknown") == "0xdeadbeef"