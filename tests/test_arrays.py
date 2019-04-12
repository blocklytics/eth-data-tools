import pytest
import warnings
from ethdata import ethdata
import datetime as dt
import json
import pkgutil
import pandas as pd


class TestArraysTransactionReceipts:
    """Test cases:
        1. Decoding an array from Bancor smart contract
        2. Decoding a custom ABI and transaction for string and bytes type 
           (based on Augar smart contract)
        3. Decoding a custom ABI and transaction for static arrays
        4. Decoding a custom ABI and transaction for dynamic arrays
        5. Decoding a custom ABI and transaction for 2D arrays and types bytes and string[]
    """

    def test_static_array_in_bancor(self):
        my_contract = ethdata.Contract("0x6690819cb98c1211a8e38790d6cd48316ed518db")
        my_contract.query_range = {"start": "2019-03-29", "end": "2019-03-29"}
        df = my_contract.transaction_receipts
        returned_data = df.loc[df.transaction_hash == "0x6396ea4d7001cddde13597ea20e9a282f224a9c5db96bf7676bb08b09a3a09e5"].to_dict("r")[0]
        for key in list(returned_data.keys())[:6]:  # remove unnecessary keys
            returned_data.pop(key)

        expected_data = {
                        "param__path": ["0xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315",
                         "0x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c",
                         "0x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c"],
                        "param__amount": 5E+18,
                        "param__minReturn": 1.071390545473422e+21,
                        "param__toBlockchain": "656f730000000000000000000000000000000000000000000000000000000000",
                        "param__to": "626e723434343532343431330000000000000000000000000000000000000000",
                        "param__conversionId": 927411658303.0,
                        "param__block": 7466993.0,
                        "param__v": 28.0,
                        "param__r": "8d1c4d3f60fb71e291a1cfe72e433d2ffacb15dfa526ae55eca73389e22ef8e6",
                        "param__s": "772a420ca17e5c1a450c57e7d1e5aa57993f4f70bb89311e54e742a1c27bb83f"
                        }

        for key in returned_data:
            assert returned_data[key] == expected_data[key]

    def test_string_handling_for_transaction_receipts(self):
        my_contract = ethdata.Contract("0xe991247b78f937d7b69cfc00f1a487a293557677")
        my_contract.query_range = {"start": "2019-03-19", "end": "2019-03-19"}
        my_contract.abi = [{"constant":False,"inputs":[{"name":"_amount","type":"uint256"}],"name":"incrementOpenInterestFromMarket","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[],"name":"getOrCreateNextFeeWindow","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[{"name":"_amount","type":"uint256"}],"name":"decrementOpenInterestFromMarket","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"getRepMarketCapInAttoeth","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[],"name":"getOrCreatePreviousFeeWindow","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[{"name":"_shadyFeeToken","type":"address"}],"name":"isContainerForFeeToken","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getController","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getDisputeRoundDurationInSeconds","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[{"name":"_feeWindowId","type":"uint256"}],"name":"getFeeWindow","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[],"name":"fork","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"getTargetRepMarketCapInAttoeth","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[],"name":"getOrCreatePreviousPreviousFeeWindow","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[{"name":"_shadyShareToken","type":"address"}],"name":"isContainerForShareToken","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"_attotokens","type":"uint256"}],"name":"buyParticipationTokens","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[{"name":"_badMarkets","type":"uint256"},{"name":"_totalMarkets","type":"uint256"},{"name":"_targetDivisor","type":"uint256"},{"name":"_previousValue","type":"uint256"},{"name":"_defaultValue","type":"uint256"},{"name":"_floor","type":"uint256"}],"name":"calculateFloatingValue","outputs":[{"name":"_newValue","type":"uint256"}],"payable":False,"stateMutability":"pure","type":"function"},{"constant":True,"inputs":[],"name":"getInitialReportMinValue","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[],"name":"getOrCreateCurrentFeeWindow","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"getOpenInterestInAttoEth","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getPreviousFeeWindow","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getWinningChildUniverse","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"_endTime","type":"uint256"},{"name":"_feePerEthInWei","type":"uint256"},{"name":"_denominationToken","type":"address"},{"name":"_designatedReporterAddress","type":"address"},{"name":"_minPrice","type":"int256"},{"name":"_maxPrice","type":"int256"},{"name":"_numTicks","type":"uint256"},{"name":"_topic","type":"bytes32"},{"name":"_description","type":"string"},{"name":"_extraInfo","type":"string"}],"name":"createScalarMarket","outputs":[{"name":"_newMarket","type":"address"}],"payable":True,"stateMutability":"payable","type":"function"},{"constant":False,"inputs":[],"name":"removeMarketFrom","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"getForkEndTime","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getCurrentFeeWindow","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getForkReputationGoal","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"_reportingParticipants","type":"address[]"},{"name":"_feeWindows","type":"address[]"}],"name":"redeemStake","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[],"name":"getOrCacheReportingFeeDivisor","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[{"name":"_controller","type":"address"}],"name":"setController","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[{"name":"_shadyChild","type":"address"}],"name":"isParentOf","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[],"name":"updateForkValues","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[{"name":"_shadyMarket","type":"address"}],"name":"isContainerForMarket","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getParentUniverse","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[{"name":"_timestamp","type":"uint256"}],"name":"getFeeWindowByTimestamp","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[],"name":"getOrCacheValidityBond","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[],"name":"getInitialReportStakeSize","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[{"name":"_amount","type":"uint256"}],"name":"decrementOpenInterest","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[{"name":"_timestamp","type":"uint256"}],"name":"getOrCreateFeeWindowByTimestamp","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"getReputationToken","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"_parentUniverse","type":"address"},{"name":"_parentPayoutDistributionHash","type":"bytes32"}],"name":"initialize","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[],"name":"isForking","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"controllerLookupName","outputs":[{"name":"","type":"bytes32"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getParentPayoutDistributionHash","outputs":[{"name":"","type":"bytes32"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[{"name":"_shadyFeeWindow","type":"address"}],"name":"isContainerForFeeWindow","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"_endTime","type":"uint256"},{"name":"_feePerEthInWei","type":"uint256"},{"name":"_denominationToken","type":"address"},{"name":"_designatedReporterAddress","type":"address"},{"name":"_topic","type":"bytes32"},{"name":"_description","type":"string"},{"name":"_extraInfo","type":"string"}],"name":"createYesNoMarket","outputs":[{"name":"_newMarket","type":"address"}],"payable":True,"stateMutability":"payable","type":"function"},{"constant":True,"inputs":[],"name":"getForkingMarket","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"_amount","type":"uint256"}],"name":"incrementOpenInterest","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[{"name":"_endTime","type":"uint256"},{"name":"_feePerEthInWei","type":"uint256"},{"name":"_denominationToken","type":"address"},{"name":"_designatedReporterAddress","type":"address"},{"name":"_outcomes","type":"bytes32[]"},{"name":"_topic","type":"bytes32"},{"name":"_description","type":"string"},{"name":"_extraInfo","type":"string"}],"name":"createCategoricalMarket","outputs":[{"name":"_newMarket","type":"address"}],"payable":True,"stateMutability":"payable","type":"function"},{"constant":True,"inputs":[{"name":"_timestamp","type":"uint256"}],"name":"getFeeWindowId","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getTypeName","outputs":[{"name":"","type":"bytes32"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"_parentPayoutNumerators","type":"uint256[]"},{"name":"_parentInvalid","type":"bool"}],"name":"createChildUniverse","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[{"name":"_feeWindow","type":"address"}],"name":"getOrCreateFeeWindowBefore","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[],"name":"getOrCacheDesignatedReportStake","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[],"name":"getOrCacheMarketCreationCost","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[{"name":"_parentPayoutDistributionHash","type":"bytes32"}],"name":"getChildUniverse","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getNextFeeWindow","outputs":[{"name":"","type":"address"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getInitialized","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[{"name":"_parentPayoutDistributionHash","type":"bytes32"}],"name":"updateTentativeWinningChildUniverse","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":True,"inputs":[{"name":"_shadyReportingParticipant","type":"address"}],"name":"isContainerForReportingParticipant","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":True,"inputs":[],"name":"getDisputeThresholdForFork","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"view","type":"function"},{"constant":False,"inputs":[],"name":"getOrCacheDesignatedReportNoShowBond","outputs":[{"name":"","type":"uint256"}],"payable":False,"stateMutability":"nonpayable","type":"function"},{"constant":False,"inputs":[],"name":"addMarketTo","outputs":[{"name":"","type":"bool"}],"payable":False,"stateMutability":"nonpayable","type":"function"}]

        df = my_contract.transaction_receipts
        assert df.loc[df.transaction_hash == "0xcb6b15f23808338426ae791de9e5312a2d3665a365db952425d262b9766d50f9"].iloc[0].param__description == "Who will win the 2019 NCAA mens college basketball championship?"

    def test_string_handling_for_event_logs(self):
        my_contract = ethdata.Contract("0x75228dce4d82566d93068a8d5d49435216551599")
        my_contract.query_range = {"start": "2019-03-19", "end": "2019-03-19"}

        df = my_contract.event_logs
        assert df.loc[(df.transaction_hash == "0xcb6b15f23808338426ae791de9e5312a2d3665a365db952425d262b9766d50f9") & (df.event_name == "MarketCreated")].iloc[0].param__description == "Who will win the 2019 NCAA mens college basketball championship?"

    def test_string_and_bytes(self):
        my_contract = ethdata.Contract("0xE991247b78F937D7B69cFC00f1A487A293557677")
        my_contract.query_range = {"start": "2019-04-01", "end": "2019-04-01"}
        # overriding ABI with new values
        my_contract.abi = [{
                        "constant":False,
                        "inputs":[{"name":"_endTime","type":"uint256"},{"name":"_feePerEthInWei","type":"uint256"},
                                  {"name":"_denominationToken","type":"address"},{"name":"_designatedReporterAddress","type":"address"},
                                  {"name":"_minPrice","type":"int256"},{"name":"_maxPrice","type":"int256"},
                                  {"name":"_numTicks","type":"uint256"},{"name":"_topic","type":"bytes32"},
                                  {"name":"_description","type":"bytes"},{"name":"_extraInfo","type":"string"}],
                        "name":"createScalarMarket",
                        "outputs":[{"name":"_newMarket","type":"address"}],
                        "payable":True,
                        "stateMutability":"payable",
                        "type":"function"}]
        
        # test data to inject into DataFrame for testing
        test_data = [
                "000000000000000000000000000000000000000000000000000000005d160848",  # uint
                "0000000000000000000000000000000000000000000000000011c37937e08000",  # uint
                "000000000000000000000000d5524179cb7ae012f5b642c1d6d700bbaa76b96b",  # address
                "000000000000000000000000d953e24b1433fbcce94b5f5b282aa67b7e6d59fb",  # address
                "0000000000000000000000000000000000000000000000008ac7230489e80000",  # int
                "000000000000000000000000000000000000000000000001158e460913d00000",  # int
                "0000000000000000000000000000000000000000000000000000000000002710",  # uint
                "7665696c00000000000000000000000000000000000000000000000000000000",  # bytes32
                "0000000000000000000000000000000000000000000000000000000000000140",  # string - offset is 10
                "00000000000000000000000000000000000000000000000000000000000001a0",  # bytes - offset is 13
                "000000000000000000000000000000000000000000000000000000000000003c",  # num of elements bytes - 60 (two rows)
                "486f77206d616e79206c697374696e67732077696c6c20446546692050756c73",
                "652068617665206279204a756e6520323874682c2032303139203f2000000000",  
                "00000000000000000000000000000000000000000000000000000000000000f0",  # num of element string - 240 (eight rows)
                "7b226c6f6e674465736372697074696f6e223a22446546692050756c73652074",
                "7261636b73206f70656e2066696e616e6365206170706c69636174696f6e7320",
                "616e642070726f746f636f6c73206275696c74206f6e20657468657265756d2e",
                "5c6e5c6e54686973206d61726b657420696e766f6c76657320746865206e756d",
                "626572206f66206c697374696e67732077686963682063616e20626520666f75",
                "6e6420696e2074686520666972737420636f6c756d6e2e20222c227461677322",
                "3a5b5d2c227265736f6c7574696f6e536f75726365223a2268747470733a2f2f",
                "6465666970756c73652e636f6d2f227d00000000000000000000000000000000"]

        # create DataFrame for testing
        df = pd.DataFrame({
                    'transaction_hash': '0x498a18373623ca84e7caf058ab13fa288d34117dcd69cf20c7dd58d75e1d033f', 
                    'block_timestamp': pd.Timestamp('2019-02-20 12:00:00+0000', tz='UTC'), 
                    'from_address': '0x59550cdee3fe8685fdb76281f5bbd9a65dc50c51', 
                    'to_address': '0x6690819cb98c1211a8e38790d6cd48316ed518db', 
                    'value': 0.042943815049849295, 
                    'function_signature': list(my_contract.functions.keys())[0],
                    'function_data': "".join(test_data)
                              }, index=[0])

        # cleaning data in appropriate format
        returned_data = ethdata.clean_transaction_receipts_df(df, my_contract).iloc[0].to_dict()
        for key in list(returned_data.keys())[:6]:  # remove unnecessary keys
            returned_data.pop(key)

        expected_data = {
                        'transaction_hash': '0x498a18373623ca84e7caf058ab13fa288d34117dcd69cf20c7dd58d75e1d033f', 
                        'block_timestamp': pd.Timestamp('2019-04-01 12:00:00+0000', tz='UTC'), 
                        'from_address': '0x59550cdee3fe8685fdb76281f5bbd9a65dc50c51', 
                        'to_address': '0x6690819cb98c1211a8e38790d6cd48316ed518db', 
                        'value': 0.042943815049849295, 
                        'function_name': 'createScalarMarket', 
                        'param__endTime': 1561725000.0, 
                        'param__feePerEthInWei': 5000000000000000.0, 
                        'param__denominationToken': '0xd5524179cb7ae012f5b642c1d6d700bbaa76b96b', 
                        'param__designatedReporterAddress': '0xd953e24b1433fbcce94b5f5b282aa67b7e6d59fb', 
                        'param__minPrice': 1e+19, 'param__maxPrice': 2e+19, 
                        'param__numTicks': 10000.0, 
                        'param__topic': '7665696c00000000000000000000000000000000000000000000000000000000', 
                        'param__description': ('486f77206d616e79206c697374696e67732077696c6c20446546692050756'
                                               'c73652068617665206279204a756e6520323874682c2032303139203f20'), 
                        'param__extraInfo': ('{"longDescription":"DeFi Pulse tracks open finance applications '
                                               'and protocols built on ethereum.\\n\\nThis market involves the '
                                               'number of listings which can be found in the first column. '
                                               '","tags":[],"resolutionSource":"https://defipulse.com/"}')
                        }

        for key in returned_data:
            assert returned_data[key] == expected_data[key]

    def test_fake_static_array(self):
        my_contract = ethdata.Contract("0x6690819cb98c1211a8e38790d6cd48316ed518db")
        # overriding ABI with new values
        my_contract.abi = [{
                            'constant': False, 
                            'inputs': [{'name': '_kelly', 'type': 'address[11]'}, {'name': '_greg', 'type': 'uint8[2]'},
                                       {'name': '_thomas', 'type': 'bool[2]'}, {'name': '_garry', 'type': 'bytes32[4]'}],
                            'name': 'registerEtherToken', 
                            'outputs': [], 
                            'payable': False, 
                            'stateMutability': 'nonpayable', 
                            'type': 'function'
                          }]

        # test data to inject into DataFrame for testing
        test_data = [
               "0000000000000000000000000000000000000000000000000000000000000000", # [0000000000000000000000000000000000000000,
               "0000000000000000000000004e15361fd6b4bb609fa63c81a2be19d873717870",  # 4e15361fd6b4bb609fa63c81a2be19d873717870,
               "0000000000000000000000004E484D658700BA6642d075b1Ad1303A049fa23E8",  # 4E484D658700BA6642d075b1Ad1303A049fa23E8,
               "00000000000000000000000052bc44d5378309EE2abF1539BF71dE1b7d7bE3b5",  # 52bc44d5378309EE2abF1539BF71dE1b7d7bE3b5,
               "000000000000000000000000E1dfD77B46003Af9F8Bc6eb8d7a7ED1cAF1744D1",  # E1dfD77B46003Af9F8Bc6eb8d7a7ED1cAF1744D1,
               "0000000000000000000000003f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE",  # 3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE,
               "0000000000000000000000003f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be",  # 3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be,
               "0000000000000000000000000D8775F648430679A709E98d2b0Cb6250d2887EF",  # 0D8775F648430679A709E98d2b0Cb6250d2887EF, 
               "0000000000000000000000003095a47305EFD248f6ce272C2dB01297A91E8C41",  # 3095a47305EFD248f6ce272C2dB01297A91E8C41,
               "000000000000000000000000056a283ebc0979b1a81c8814c2760e57a03c30f2",  # 056a283ebc0979b1a81c8814c2760e57a03c30f2,
               "0000000000000000000000001042320137d4e923711e33d8f758e512a235ef8e",  # 1042320137d4e923711e33d8f758e512a235ef8e]
               "000000000000000000000000000000000000000000000000000000000000001c",  # [28,
               "000000000000000000000000000000000000000000000000000000000000001c",  # 28]
               "0000000000000000000000000000000000000000000000000000000000000000",  # [False,
               "0000000000000000000000000000000000000000000000000000000000000001",  # True]
               "58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30",  # [58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30
               "0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80",  # 0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80,
               "57dfd7bcba48c39f982a3a967da57ed42c1219c1d38169f035d84c1a5086a733",  # 57dfd7bcba48c39f982a3a967da57ed42c1219c1d38169f035d84c1a5086a733,
               "0a439bb94f79a4c46613b47cc75b3a1746e41d02b4304def3737a7b33d53b314"]   # 0a439bb94f79a4c46613b47cc75b3a1746e41d02b4304def3737a7b33d53b314]

        # create DataFrame for testing
        df = pd.DataFrame({
            'transaction_hash': '0x498a18373623ca84e7caf058ab13fa288d34117dcd69cf20c7dd58d75e1d033f', 
            'block_timestamp': pd.Timestamp('2019-02-20 12:00:00+0000', tz='UTC'), 
            'from_address': '0x59550cdee3fe8685fdb76281f5bbd9a65dc50c51', 
            'to_address': '0x6690819cb98c1211a8e38790d6cd48316ed518db', 
            'value': 0, 
            'function_signature': list(my_contract.functions.keys())[0],
            'function_data': "".join(test_data)
                      }, index=[0])

        # cleaning data in appropriate format
        returned_data = ethdata.clean_transaction_receipts_df(df, my_contract).iloc[0].to_dict()
        for key in list(returned_data.keys())[:6]:  # remove unnecessary keys
            returned_data.pop(key)

        expected_result = {
        'param__kelly': 
        ["0x0000000000000000000000000000000000000000",
        "0x4e15361fd6b4bb609fa63c81a2be19d873717870",
        "0x4E484D658700BA6642d075b1Ad1303A049fa23E8",
        "0x52bc44d5378309EE2abF1539BF71dE1b7d7bE3b5",
        "0xE1dfD77B46003Af9F8Bc6eb8d7a7ED1cAF1744D1",
        "0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE",
        "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be",
        "0x0D8775F648430679A709E98d2b0Cb6250d2887EF", 
        "0x3095a47305EFD248f6ce272C2dB01297A91E8C41",
        "0x056a283ebc0979b1a81c8814c2760e57a03c30f2",
        "0x1042320137d4e923711e33d8f758e512a235ef8e"], 
        'param__greg': [28.0, 28.0], 
        'param__thomas': [False, True], 
        'param__garry': ['58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30', 
        '0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80', 
        '57dfd7bcba48c39f982a3a967da57ed42c1219c1d38169f035d84c1a5086a733', 
        '0a439bb94f79a4c46613b47cc75b3a1746e41d02b4304def3737a7b33d53b314']
        }

        for key in returned_data:
            assert returned_data[key] == expected_result[key]

    def test_fake_dynamic_array(self):
        my_contract = ethdata.Contract("0x6690819cb98c1211a8e38790d6cd48316ed518db")
        # overriding ABI with new values
        my_contract.abi = [{
                            'constant': False, 
                            'inputs': [{'name': '_jack', 'type': 'address[]'}, {'name': '_price', 'type': 'int8[]'},
                                       {'name': '_yuri', 'type': 'bytes32[]'}, {'name': '_soap', 'type': 'bool[]'}], 
                            'name': 'registerEtherToken', 
                            'outputs': [], 
                            'payable': False, 
                            'stateMutability': 'nonpayable', 
                            'type': 'function'
                          }]

        # test data to inject into DataFrame for testing
        test_data = [
               "0000000000000000000000000000000000000000000000000000000000000040",  # address
               "00000000000000000000000000000000000000000000000000000000000000A0",  # int
               "0000000000000000000000000000000000000000000000000000000000000002",
               "0000000000000000000000000000000000000000000000000000000000000000",  # [0000000000000000000000000000000000000000,
               "0000000000000000000000004e15361fd6b4bb609fa63c81a2be19d873717870",  # 4e15361fd6b4bb609fa63c81a2be19d873717870]
               "0000000000000000000000000000000000000000000000000000000000000002",
               "000000000000000000000000000000000000000000000000000000000000001c",  # [28,
               "000000000000000000000000000000000000000000000000000000000000001c",  # 28]
               "0000000000000000000000000000000000000000000000000000000000000140",  # bytes32
               "00000000000000000000000000000000000000000000000000000000000001A0",  # bool
               "0000000000000000000000000000000000000000000000000000000000000002",
               "58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30",  # [58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30
               "0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80",  # 0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80]
               "0000000000000000000000000000000000000000000000000000000000000002", 
               "0000000000000000000000000000000000000000000000000000000000000000",  # [False,
               "0000000000000000000000000000000000000000000000000000000000000001"]  # True]

        # create DataFrame for testing
        df = pd.DataFrame({
            'transaction_hash': '0x498a18373623ca84e7caf058ab13fa288d34117dcd69cf20c7dd58d75e1d033f', 
            'block_timestamp': pd.Timestamp('2019-02-20 12:00:00+0000', tz='UTC'), 
            'from_address': '0x59550cdee3fe8685fdb76281f5bbd9a65dc50c51', 
            'to_address': '0x6690819cb98c1211a8e38790d6cd48316ed518db', 
            'value': 0, 
            'function_signature': list(my_contract.functions.keys())[0],
            'function_data': "".join(test_data)
                      }, index=[0])

        # cleaning data in appropriate format
        returned_data = ethdata.clean_transaction_receipts_df(df, my_contract).iloc[0].to_dict()
        for key in list(returned_data.keys())[:6]:  # remove unnecessary keys
            returned_data.pop(key)

        expected_result = {
        'param__jack': ['0x0000000000000000000000000000000000000000', '0x4e15361fd6b4bb609fa63c81a2be19d873717870'], 
        'param__price': [28.0, 28.0], 
        'param__yuri': ['58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30', 
        '0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80'], 
        'param__soap': [False, True]
         }

        for key in returned_data:
            assert returned_data[key] == expected_result[key]

    def test_unsupported_array_types(self):
        eg_unsupported_types = ['address[][]', 'int8[2][]', 'bytes32[][3]', 'bool[2][3]', 'string[2]', 'string[]']
        my_contract = ethdata.Contract("0x6690819cb98c1211a8e38790d6cd48316ed518db")

        # overriding ABI with new values
        my_contract.abi = [{
                            'constant': False, 
                            'inputs': [{'name': '_jack', 'type': 'address[][]'}, {'name': '_price', 'type': 'int8[2][]'},
                                       {'name': '_yuri', 'type': 'bytes32[][3]'}, {'name': '_soap', 'type': 'bool[2][3]'},
                                       {'name': '_greg', 'type': 'string[2]'}, {'name': '_thomas', 'type': 'string[]'}], 
                            'name': 'registerEtherToken', 
                            'outputs': [], 
                            'payable': False, 
                            'stateMutability': 'nonpayable', 
                            'type': 'function'
                          }]

        # test data to inject into DataFrame for testing (this data does not represent ABI's types)
        test_data = [
               "0000000000000000000000000000000000000000000000000000000000000040",
               "00000000000000000000000000000000000000000000000000000000000000A0",
               "0000000000000000000000000000000000000000000000000000000000000002",
               "000000000000000000000000000000000000000000000000000000000000032d",
               "0000000000000000000000004e15361fd6b4bb609fa63c81a2be19d873717870",
               "0000000000000000000000000000000000000000000000000000000000000001"]

        # create DataFrame for testing
        df = pd.DataFrame({
            'transaction_hash': '0x498a18373623ca84e7caf058ab13fa288d34117dcd69cf20c7dd58d75e1d033f', 
            'block_timestamp': pd.Timestamp('2019-02-20 12:00:00+0000', tz='UTC'), 
            'from_address': '0x59550cdee3fe8685fdb76281f5bbd9a65dc50c51', 
            'to_address': '0x6690819cb98c1211a8e38790d6cd48316ed518db', 
            'value': 0, 
            'function_signature': list(my_contract.functions.keys())[0],
            'function_data': "".join(test_data)
                      }, index=[0])

        with pytest.warns(UserWarning) as record:
            ethdata.clean_transaction_receipts_df(df, my_contract)
        record = [warning for warning in record if warning.category == UserWarning]
        for n, warning in enumerate(record):
            assert str(warning.message) == f"{eg_unsupported_types[n]} is not yet supported"


class TestArraysEventLogs:
    """Test cases:
        1. Decoding a string from Augur smart contract
        2. Decoding a custom ABI and an event log for string and bytes type
           (based on Augur smart contract)
        3. Decoding a custom ABI and an event log for static arrays - data passed in event data
        4. Decoding a custom ABI and an event log for static arrays - data passed in event topics
        5. Decoding a custom ABI and an event log for dynamic arrays - data passed in event data
        6. Decoding a custom ABI and an event log for dynamic arrays - data passed in event topics
        7. Decoding a custom ABI and an event log to test for warning with unsupported types - data passed in event data
        8. Decoding a custom ABI and an event log to test for warning with unsupported types - data passed in event topics
    """


    def test_string_in_augur(self):
        my_contract = ethdata.Contract("0x75228dce4d82566d93068a8d5d49435216551599")
        my_contract.query_range = {"start": "2019-04-01", "end": "2019-04-01"}
        df_test = my_contract.event_logs
        returned_data = df_test.iloc[55]

        assert returned_data.data_description == "How many listings will DeFi Pulse have by June 28th, 2019 ? "
        assert returned_data.data_extraInfo == ('{"longDescription":"DeFi Pulse tracks open finance applications '
                                                'and protocols built on ethereum.\\n\\nThis market involves the '
                                                'number of listings which can be found in the first column. '
                                                '","tags":[],"resolutionSource":"https://defipulse.com/"}')

    def test_bytes_and_string(self):
        my_contract = ethdata.Contract("0x75228dce4d82566d93068a8d5d49435216551599")
        my_contract.query_range = {"start": "2019-04-01", "end": "2019-04-01"}
        # overriding ABI with new values
        my_contract.abi = [{
                            'anonymous': False, 
                            'inputs': [    
                                        {'indexed': True, 'name': 'topic', 'type': 'bytes32'}, 
                                        {'indexed': True, 'name': 'universe', 'type': 'address'},
                                        {'indexed': True, 'name': 'marketCreator', 'type': 'address'}, 
                                        {'indexed': False, 'name': 'description', 'type': 'bytes'}, 
                                        {'indexed': False, 'name': 'extraInfo', 'type': 'string'},
                                        {'indexed': False, 'name': 'market', 'type': 'address'}, 
                                        {'indexed': False, 'name': 'outcomes', 'type': 'bytes32[]'},
                                        {'indexed': False, 'name': 'marketCreationFee', 'type': 'uint256'}, 
                                        {'indexed': False, 'name': 'minPrice', 'type': 'int256'}, 
                                        {'indexed': False, 'name': 'maxPrice', 'type': 'int256'},
                                        {'indexed': False, 'name': 'marketType', 'type': 'uint8'}],
                            'name': 'MarketCreated', 'type': 'event'}]

        # test data to inject into DataFrame for testing
        test_data = "".join([
                    "0x",
                    "0000000000000000000000000000000000000000000000000000000000000100",
                    "0000000000000000000000000000000000000000000000000000000000000160",
                    "0000000000000000000000007fb0a15484f52ef282901a67c07c946d753e4c3e",
                    "0000000000000000000000000000000000000000000000000000000000000280",
                    "0000000000000000000000000000000000000000000000000098912c1958a9cf",
                    "0000000000000000000000000000000000000000000000008ac7230489e80000",
                    "000000000000000000000000000000000000000000000001158e460913d00000",
                    "0000000000000000000000000000000000000000000000000000000000000002",
                    "000000000000000000000000000000000000000000000000000000000000003c",
                    "486f77206d616e79206c697374696e67732077696c6c20446546692050756c73",
                    "652068617665206279204a756e6520323874682c2032303139203f2000000000",
                    "00000000000000000000000000000000000000000000000000000000000000f0",
                    "7b226c6f6e674465736372697074696f6e223a22446546692050756c73652074",
                    "7261636b73206f70656e2066696e616e6365206170706c69636174696f6e7320",
                    "616e642070726f746f636f6c73206275696c74206f6e20657468657265756d2e",
                    "5c6e5c6e54686973206d61726b657420696e766f6c76657320746865206e756d",
                    "626572206f66206c697374696e67732077686963682063616e20626520666f75",
                    "6e6420696e2074686520666972737420636f6c756d6e2e20222c227461677322",
                    "3a5b5d2c227265736f6c7574696f6e536f75726365223a2268747470733a2f2f",
                    "6465666970756c73652e636f6d2f227d00000000000000000000000000000000",
                    "0000000000000000000000000000000000000000000000000000000000000000"])

        # create DataFrame for testing
        df = pd.DataFrame({
            'transaction_hash': '0x762a85b8f67862f3a9558c586f12bc668774cc0789bf66920a1b399684ed4ae9', 
            'block_timestamp': pd.Timestamp('2018-11-15 00:01:59'), 
            'address': '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208', 
            'topics_0': list(my_contract.events.keys())[0], 
            'topics_1': "0x7665696c00000000000000000000000000000000000000000000000000000000", 
            'topics_2': "0x000000000000000000000000e991247b78f937d7b69cfc00f1a487a293557677", 
            'topics_3': "0x000000000000000000000000d953e24b1433fbcce94b5f5b282aa67b7e6d59fb",
            'topics_4': None, 
            'transaction_data': test_data
            }, index=[0])

        # cleaning data in appropriate format
        returned_data = ethdata.clean_event_logs_df(df, my_contract).to_dict("r")[0]
        for key in ("transaction_hash", "block_timestamp", "event_name", "address"):
            returned_data.pop(key)

        expected_data = {
                        'topic_topic': '0x7665696c00000000000000000000000000000000000000000000000000000000', 
                        'topic_universe': '0xe991247b78f937d7b69cfc00f1a487a293557677', 
                        'topic_marketCreator': '0xd953e24b1433fbcce94b5f5b282aa67b7e6d59fb', 
                        'data_description': ('486f77206d616e79206c697374696e67732077696c6c20446546692050756c7'
                                            '3652068617665206279204a756e6520323874682c2032303139203f20'), 
                        'data_extraInfo': ('{"longDescription":"DeFi Pulse tracks open finance applications '
                                           'and protocols built on ethereum.\\n\\nThis market involves the '
                                           'number of listings which can be found in the first column. '
                                           '","tags":[],"resolutionSource":"https://defipulse.com/"}'), 
                        'data_market': '0x7fb0a15484f52ef282901a67c07c946d753e4c3e', 
                        'data_outcomes': [], 
                        'data_marketCreationFee': 4.29438150498493e+16, 
                        'data_minPrice': 1e+19, 
                        'data_maxPrice': 2e+19, 
                        'data_marketType': 2.0
                        }

        for key in returned_data:
            assert returned_data[key] == expected_data[key]

    def test_static_array_handling_with_data(self):
        my_contract = ethdata.Contract("0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208")
        my_contract.query_range = {"start": "2018-11-15", "end": "2018-11-15"}

        # overriding ABI with new values
        my_contract.abi = [{
                            'anonymous': False, 
                            'inputs': [
                                        {'indexed': False, 'name': 'token', 'type': 'address[5]'}, 
                                        {'indexed': False, 'name': 'user', 'type': 'int8[2]'}, 
                                        {'indexed': False, 'name': 'amount', 'type': 'bool[2]'}, 
                                        {'indexed': False, 'name': 'balance', 'type': 'bytes32[3]'}
                                      ], 
                            'name': 'Withdraw', 'type': 'event'}]

        # test data to inject into DataFrame for testing (this data does not represent ABI's types)
        test_data = "".join(["0x",
                           "0000000000000000000000000000000000000000000000000000000000000000", # [0000000000000000000000000000000000000000,
                           "0000000000000000000000004e15361fd6b4bb609fa63c81a2be19d873717870",  # 4e15361fd6b4bb609fa63c81a2be19d873717870,
                           "0000000000000000000000004E484D658700BA6642d075b1Ad1303A049fa23E8",  # 4E484D658700BA6642d075b1Ad1303A049fa23E8,
                           "00000000000000000000000052bc44d5378309EE2abF1539BF71dE1b7d7bE3b5",  # 52bc44d5378309EE2abF1539BF71dE1b7d7bE3b5,
                           "0000000000000000000000001042320137d4e923711e33d8f758e512a235ef8e",  # 1042320137d4e923711e33d8f758e512a235ef8e]
                           "000000000000000000000000000000000000000000000000000000000000001c",  # [28,
                           "000000000000000000000000000000000000000000000000000000000000001c",  # 28]
                           "0000000000000000000000000000000000000000000000000000000000000000",  # [False,
                           "0000000000000000000000000000000000000000000000000000000000000001",  # True]
                           "58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30",  # [58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30
                           "0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80",  # 0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80,
                           "57dfd7bcba48c39f982a3a967da57ed42c1219c1d38169f035d84c1a5086a733"])  # 57dfd7bcba48c39f982a3a967da57ed42c1219c1d38169f035d84c1a5086a733])

        # create DataFrame for testing
        df = pd.DataFrame({
            'transaction_hash': '0x762a85b8f67862f3a9558c586f12bc668774cc0789bf66920a1b399684ed4ae9', 
            'block_timestamp': pd.Timestamp('2018-11-15 00:01:59'), 
            'address': '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208', 
            'topics_0': list(my_contract.events.keys())[0], 
            'topics_1': None, 
            'topics_2': None, 
            'topics_3': None, 
            'transaction_data': test_data
            }, index=[0])

        returned_data = ethdata.clean_event_logs_df(df, my_contract).to_dict("r")[0]
        for key in ("transaction_hash", "block_timestamp", "event_name", "address"):
            returned_data.pop(key)

        expected_data = {'data_token': [
                                        "0x0000000000000000000000000000000000000000",
                                        "0x4e15361fd6b4bb609fa63c81a2be19d873717870",
                                        "0x4E484D658700BA6642d075b1Ad1303A049fa23E8",
                                        "0x52bc44d5378309EE2abF1539BF71dE1b7d7bE3b5",
                                        "0x1042320137d4e923711e33d8f758e512a235ef8e"], 
                        'data_user': [28.0, 28.0], 
                        'data_amount': [False, True], 
                        'data_balance': [
                                        '58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30', 
                                        '0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80', 
                                        '57dfd7bcba48c39f982a3a967da57ed42c1219c1d38169f035d84c1a5086a733']
                        }
                        

        for key in returned_data:
            assert returned_data[key] == expected_data[key]

    def test_static_array_handling_with_topics(self):
        eg_array_types = ["address[2]", "int8[3]", "bytes32[4]", "bool[2]"]
        my_contract = ethdata.Contract("0x6690819cb98c1211a8e38790d6cd48316ed518db")

        # overriding ABI with new values
        my_contract.abi = [{
                            'anonymous': False, 
                            'inputs': [    
                                        {'indexed': True, 'name': 'token', 'type': 'address[2]'}, 
                                        {'indexed': True, 'name': 'user', 'type': 'int8[3]'},
                                        {'indexed': True, 'name': 'amount', 'type': 'bytes32[4]'}, 
                                        {'indexed': True, 'name': 'mount', 'type': 'bool[2]'} 
                                      ], 
                            'name': 'Withdraw', 'type': 'event'}]

        # create DataFrame for testing
        df = pd.DataFrame({
            'transaction_hash': '0x762a85b8f67862f3a9558c586f12bc668774cc0789bf66920a1b399684ed4ae9', 
            'block_timestamp': pd.Timestamp('2018-11-15 00:01:59'), 
            'address': '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208', 
            'topics_0': list(my_contract.events.keys())[0], 
            'topics_1': "0000000000000000000000000000000000000000000000000000000000000040", 
            'topics_2': "00000000000000000000000000000000000000000000000000000000000000A0", 
            'topics_3': "0000000000000000000000000000000000000000000000000000000000000002",
            'topics_4': "0000000000000000000000000000000000000000000000000000000000000040", 
            'transaction_data': None
            }, index=[0])

        with pytest.warns(UserWarning) as record:
            ethdata.clean_event_logs_df(df, my_contract)
        record = [warning for warning in record if warning.category == UserWarning]
        for n, warning in enumerate(record):
            assert str(warning.message) == f"{eg_array_types[n]} is not yet supported passed as topic"

    def test_dynamic_array_handling_with_data(self):
        my_contract = ethdata.Contract("0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208")
        my_contract.query_range = {"start": "2018-11-15", "end": "2018-11-15"}

        # overriding ABI with new values
        my_contract.abi = [{
                            'anonymous': False, 
                            'inputs': [
                                        {'indexed': False, 'name': 'token', 'type': 'address[]'}, 
                                        {'indexed': False, 'name': 'user', 'type': 'int8[]'}, 
                                        {'indexed': False, 'name': 'amount', 'type': 'bool[]'}, 
                                        {'indexed': False, 'name': 'balance', 'type': 'bytes32[]'}
                                      ], 
                            'name': 'Withdraw', 'type': 'event'}]

        # test data to inject into DataFrame for testing (this data does not represent ABI's types)
        test_data = "".join(["0x",
                           "0000000000000000000000000000000000000000000000000000000000000040",  # address
                           "00000000000000000000000000000000000000000000000000000000000000A0",  # int
                           "0000000000000000000000000000000000000000000000000000000000000002",
                           "0000000000000000000000000000000000000000000000000000000000000000",  # [0000000000000000000000000000000000000000,
                           "0000000000000000000000004e15361fd6b4bb609fa63c81a2be19d873717870",  # 4e15361fd6b4bb609fa63c81a2be19d873717870]
                           "0000000000000000000000000000000000000000000000000000000000000002",
                           "000000000000000000000000000000000000000000000000000000000000001c",  # [28,
                           "000000000000000000000000000000000000000000000000000000000000001c",  # 28]
                           "0000000000000000000000000000000000000000000000000000000000000140",  # bytes32
                           "00000000000000000000000000000000000000000000000000000000000001A0",  # bool
                           "0000000000000000000000000000000000000000000000000000000000000002",
                           "58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30",  # [58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30
                           "0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80",  # 0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80]
                           "0000000000000000000000000000000000000000000000000000000000000002", 
                           "0000000000000000000000000000000000000000000000000000000000000000",  # [False,
                           "0000000000000000000000000000000000000000000000000000000000000001"])  # True]

        # create DataFrame for testing
        df = pd.DataFrame({
            'transaction_hash': '0x762a85b8f67862f3a9558c586f12bc668774cc0789bf66920a1b399684ed4ae9', 
            'block_timestamp': pd.Timestamp('2018-11-15 00:01:59'), 
            'address': '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208', 
            'topics_0': list(my_contract.events.keys())[0], 
            'topics_1': None, 
            'topics_2': None, 
            'topics_3': None, 
            'transaction_data': test_data
            }, index=[0])

        returned_data = ethdata.clean_event_logs_df(df, my_contract).to_dict("r")[0]
        for key in ("transaction_hash", "block_timestamp", "event_name", "address"):
            returned_data.pop(key)

        # create DataFrame for testing
        expected_data = {'data_token': ['0x0000000000000000000000000000000000000000', 
                                         '0x4e15361fd6b4bb609fa63c81a2be19d873717870'], 
                        'data_user': [28.0, 28.0], 
                        'data_amount': ['58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30', 
                                        '0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80'], 
                        'data_balance': [False, True]
                        }            

        for key in returned_data:
            assert returned_data[key] == expected_data[key]

    def test_dynamic_array_handling_with_topics(self):
        eg_array_types = ["address[]", "int8[]", "bytes32[]", "bool[]"]
        my_contract = ethdata.Contract("0x6690819cb98c1211a8e38790d6cd48316ed518db")

        # overriding ABI with new values
        my_contract.abi = [{
                            'anonymous': False, 
                            'inputs': [    
                                        {'indexed': True, 'name': 'token', 'type': 'address[]'}, 
                                        {'indexed': True, 'name': 'user', 'type': 'int8[]'},
                                        {'indexed': True, 'name': 'amount', 'type': 'bytes32[]'}, 
                                        {'indexed': True, 'name': 'mount', 'type': 'bool[]'} 
                                      ], 
                            'name': 'Withdraw', 'type': 'event'}]

        # create DataFrame for testing
        df = pd.DataFrame({
            'transaction_hash': '0x762a85b8f67862f3a9558c586f12bc668774cc0789bf66920a1b399684ed4ae9', 
            'block_timestamp': pd.Timestamp('2018-11-15 00:01:59'), 
            'address': '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208', 
            'topics_0': list(my_contract.events.keys())[0], 
            'topics_1': "0000000000000000000000000000000000000000000000000000000000000040", 
            'topics_2': "00000000000000000000000000000000000000000000000000000000000000A0", 
            'topics_3': "0000000000000000000000000000000000000000000000000000000000000002",
            'topics_4': "0000000000000000000000000000000000000000000000000000000000000040", 
            'transaction_data': None
            }, index=[0])

        with pytest.warns(UserWarning) as record:
            ethdata.clean_event_logs_df(df, my_contract)
        record = [warning for warning in record if warning.category == UserWarning]
        for n, warning in enumerate(record):
            assert str(warning.message) == f"{eg_array_types[n]} is not yet supported passed as topic"

    def test_unsupported_array_types_with_topics(self):
        eg_unsupported_types = ['address[][]', 'int8[2][]', 'bytes32[][3]', 'bool[2][3]', 'string[2]', 'string[]']
        my_contract = ethdata.Contract("0x6690819cb98c1211a8e38790d6cd48316ed518db")

        # overriding ABI with new values
        my_contract.abi = [{
                    'anonymous': False, 
                    'inputs': [    
                                {'indexed': True, 'name': 'token', 'type': 'address[][]'}, 
                                {'indexed': True, 'name': 'user', 'type': 'int8[2][]'},
                                {'indexed': True, 'name': 'amount', 'type': 'bytes32[][3]'}, 
                                {'indexed': True, 'name': 'mount', 'type': 'bool[2][3]'}, 
                                {'indexed': True, 'name': 'roken', 'type': 'string[2]'},
                                {'indexed': True, 'name': 'taunt', 'type': 'string[]'},
                              ], 
                    'name': 'Withdraw', 'type': 'event'}]

        # create DataFrame for testing
        df = pd.DataFrame({
            'transaction_hash': '0x762a85b8f67862f3a9558c586f12bc668774cc0789bf66920a1b399684ed4ae9', 
            'block_timestamp': pd.Timestamp('2018-11-15 00:01:59'), 
            'address': '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208', 
            'topics_0': list(my_contract.events.keys())[0], 
            'topics_1': "0000000000000000000000000000000000000000000000000000000000000040", 
            'topics_2': "00000000000000000000000000000000000000000000000000000000000000A0", 
            'topics_3': "0000000000000000000000000000000000000000000000000000000000000002",
            'topics_4': "0000000000000000000000000000000000000000000000000000000000000040", 
            'topics_5': "00000000000000000000000000000000000000000000000000000000000000A0", 
            'topics_6': "0000000000000000000000000000000000000000000000000000000000000002", 
            'topics_7': "00000000000000000000000000000000000000000000000000000000000000A0", 
            'topics_8': "0000000000000000000000000000000000000000000000000000000000000002", 
            'transaction_data': None
            }, index=[0])

        with pytest.warns(UserWarning) as record:
            ethdata.clean_event_logs_df(df, my_contract)
        record = [warning for warning in record if warning.category == UserWarning]
        for n, warning in enumerate(record):
            assert str(warning.message) == f"{eg_unsupported_types[n]} is not yet supported"

    def test_unsupported_array_types_with_data(self):
        eg_unsupported_types = ['address[][]', 'int8[2][]', 'bytes32[][3]', 'bool[2][3]', 'string[2]', 'string[]']
        my_contract = ethdata.Contract("0x6690819cb98c1211a8e38790d6cd48316ed518db")

        # overriding ABI with new values
        my_contract.abi = [{
                            'anonymous': False, 
                            'inputs': [    
                                        {'indexed': False, 'name': 'token', 'type': 'address[][]'}, 
                                        {'indexed': False, 'name': 'user', 'type': 'int8[2][]'},
                                        {'indexed': False, 'name': 'amount', 'type': 'bytes32[][3]'}, 
                                        {'indexed': False, 'name': 'mount', 'type': 'bool[2][3]'}, 
                                        {'indexed': False, 'name': 'roken', 'type': 'string[2]'},
                                        {'indexed': False, 'name': 'taunt', 'type': 'string[]'}
                                      ], 
                            'name': 'Withdraw', 'type': 'event'}]

        # test data to inject into DataFrame for testing (this data does not represent ABI's types)
        test_data = "".join([
               "0000000000000000000000000000000000000000000000000000000000000040",
               "00000000000000000000000000000000000000000000000000000000000000A0",
               "0000000000000000000000000000000000000000000000000000000000000002",
               "000000000000000000000000000000000000000000000000000000000000032d",
               "0000000000000000000000004e15361fd6b4bb609fa63c81a2be19d873717870",
               "0000000000000000000000000000000000000000000000000000000000000002",
               "000000000000000000000000000000000000000000000000000000000000001c",
               "0000000000000000000000000000000000000000000000000000000000000001"])

        # create DataFrame for testing
        df = pd.DataFrame({
            'transaction_hash': '0x762a85b8f67862f3a9558c586f12bc668774cc0789bf66920a1b399684ed4ae9', 
            'block_timestamp': pd.Timestamp('2018-11-15 00:01:59'), 
            'address': '0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208', 
            'topics_0': list(my_contract.events.keys())[0], 
            'topics_1': None, 
            'topics_2': None, 
            'topics_3': None, 
            'transaction_data': test_data
            }, index=[0])

        with pytest.warns(UserWarning) as record:
            ethdata.clean_event_logs_df(df, my_contract)
        record = [warning for warning in record if warning.category == UserWarning]
        for n, warning in enumerate(record):
            assert str(warning.message) == f"{eg_unsupported_types[n]} is not yet supported"
