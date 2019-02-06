import pytest
from ethdata import ethdata
import os

class TestEnvironmentVariables(object):
    def test_google_auth(self):
        assert os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") != None
    
    def test_infura_auth(self):
        assert os.environ.get("INFURA_PROJECT_ID") != None
        
    def test_etherscan_auth(self):
        assert os.environ.get("ETHERSCAN_API_KEY") != None

class TestExternalServiceInit(object):
    def test_infura_init(self):
        my_infura = ethdata.Infura()
        assert my_infura.project_id == os.environ.get("INFURA_PROJECT_ID")
        my_infura.project_id = "TEST"
        assert my_infura.project_id == "TEST"
        with pytest.warns(UserWarning):
            assert my_infura.eth_call("", "name()") == None

    def test_etherscan_init(self):
        my_etherscan = ethdata.Etherscan()
        assert my_etherscan.api_key == os.environ.get("ETHERSCAN_API_KEY")
        my_etherscan.api_key = "TEST"
        assert my_etherscan.api_key == "TEST"
        with pytest.warns(UserWarning):
            assert my_etherscan.get_abi("0x") == []

    # TODO: def test_bigquery_init(self):

class TestExternalServiceCalls(object):
    def test_infura_call(self):
        my_infura = ethdata.Infura()
        response = my_infura.eth_blockNumber()
        assert response.ok == True

    def test_etherscan_call(self):
        my_etherscan = ethdata.Etherscan()
        response = my_etherscan.get_abi_response("0x")
        assert response.ok == True

    # TODO: def test_bigquery_call(self):
        