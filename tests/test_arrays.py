import pytest
from ethdata import ethdata
import datetime as dt
import json
import pkgutil
import pandas as pd


class TestArrays:
	"""Test cases:
        1. Decoding an array from Bancor smart contract
        2. Decoding a custom ABI and transactions for static arrays
        3. Decoding a custom ABI and transactions for dynamic arrays
        4. Decoding a custom ABI and transactions for 2D arrays and types bytes and string[]
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
		clean_df = ethdata.clean_transaction_receipts_df(df, my_contract)
		clean_df = clean_df.iloc[0].to_dict()
		for key in list(clean_df.keys())[:6]:  # remove unnecessary keys
			clean_df.pop(key)

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

		for key in clean_df:
			assert clean_df[key] == expected_result[key]

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
		clean_df = ethdata.clean_transaction_receipts_df(df, my_contract)
		clean_df = clean_df.iloc[0].to_dict()
		for key in list(clean_df.keys())[:6]:  # remove unnecessary keys
			clean_df.pop(key)

		expected_result = {
		'param__jack': ['0x0000000000000000000000000000000000000000', '0x4e15361fd6b4bb609fa63c81a2be19d873717870'], 
		'param__price': [28.0, 28.0], 
		'param__yuri': ['58195c401a8d174bd01f666948a1a2400c2bc01f5a8a80aa5d1559a192d89a30', 
		'0206a07fec44b8720a37debd0c251824daf65a3725de7aa58fd0d6222f7acf80'], 
		'param__soap': [False, True]
		 }

		for key in clean_df:
			assert clean_df[key] == expected_result[key]

	def test_unsupported_array_types(self):
		eg_unsupported_types = ['address[][]', 'int8[2][]', 'bytes32[][3]', 'bool[2][3]', 'bytes', 'string[2]', 'string[]']
		my_contract = ethdata.Contract("0x6690819cb98c1211a8e38790d6cd48316ed518db")

		# overriding ABI with new values
		my_contract.abi = [{
							'constant': False, 
							'inputs': [{'name': '_jack', 'type': 'address[][]'}, {'name': '_price', 'type': 'int8[2][]'},
									   {'name': '_yuri', 'type': 'bytes32[][3]'}, {'name': '_soap', 'type': 'bool[2][3]'},
									   {'name': '_kelly', 'type': 'bytes'}, {'name': '_greg', 'type': 'string[2]'},
									   {'name': '_thomas', 'type': 'string[]'}], 
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
			   "0000000000000000000000000000000000000000000000000000000000000000",
			   "0000000000000000000000004e15361fd6b4bb609fa63c81a2be19d873717870",
			   "0000000000000000000000000000000000000000000000000000000000000002",
			   "000000000000000000000000000000000000000000000000000000000000001c"]

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
		assert len(record) == len(eg_unsupported_types)
		for n, warning in enumerate(record):
			assert str(warning.message) == f"{eg_unsupported_types[n]} is not yet supported"
