import pytest
from ethdata import ethdata
import datetime as dt
import json
import pkgutil
import os


class Test_Exceptions:

	my_account = ethdata.Account("0xeb9951021698b42e4399f9cbb6267aa35f82d59d")
	path = "../eth_data_tools.egg-info/"

	def test_file_creation(self):
		assert ("exceptions_list.json" in os.listdir(Test_Exceptions.path)) == True
		os.remove(Test_Exceptions.path + "exceptions_list.json")

	# def test_scraping(self):
	# 	assert 

	# def test_add_exception(self):
 #        my_account = ethdata.Account("0xbb9bc244d798123fde783fcc1c72d3bb8c189413")
 #        with open(path + "exceptions_list.json") as r_file:
 #        	exceptions_list = json.load(r_file)
 #    	assert "0xbb9bc244d798123fde783fcc1c72d3bb8c189413" in exceptions_list.keys() == True
 #    	assert exceptions_list["0xbb9bc244d798123fde783fcc1c72d3bb8c189413"]["name"] == "TheDAO"
 #    	assert exceptions_list["0xbb9bc244d798123fde783fcc1c72d3bb8c189413"]["symbol"] == "TheDAO"
 #    	assert exceptions_list["0xbb9bc244d798123fde783fcc1c72d3bb8c189413"]["decimals"] == 16.0
 #    	exceptions_list.remove("0xbb9bc244d798123fde783fcc1c72d3bb8c189413")