import datetime as dt
import requests
import json
import os
import warnings
import string
from Crypto.Hash import keccak
from google.cloud import bigquery
import pandas as pd
from math import ceil
from collections import OrderedDict

# BigQuery Public Ethereum Datasets
public_dataset = {
    "blocks": "bigquery-public-data.crypto_ethereum.blocks"
    ,"contracts": "bigquery-public-data.crypto_ethereum.contracts"
    ,"logs": "bigquery-public-data.crypto_ethereum.logs"
    ,"token_transfers": "bigquery-public-data.crypto_ethereum.token_transfers"
    ,"tokens": "bigquery-public-data.crypto_ethereum.tokens"
    ,"traces": "bigquery-public-data.crypto_ethereum.traces"
    ,"transactions": "bigquery-public-data.crypto_ethereum.transactions"
}

# Exception list
exception_list = {
    "0xeb9951021698b42e4399f9cbb6267aa35f82d59d": {
        "name": "Lif",
        "symbol": "LIF",
        "decimals": 18.0
    },
    "0xe0b7927c4af23765cb51314a0e0521a9645f0e2a": {
        "name": "DGD",
        "symbol": "DGD",
        "decimals": 9.0
    },
    "0x78bac62f2a4cd3a7cb7da2991affc7b11590f682": {
        "name": "Uniswap V1",
        "symbol": "UNI-V1",
        "decimals": 0.0
    },
    "0x09cabec1ead1c0ba254b09efb3ee13841712be14": {
        "name": "Uniswap V1",
        "symbol": "UNI-V1",
        "decimals": 0.0
    },
    "0xbb9bc244d798123fde783fcc1c72d3bb8c189413": {
        "name": "TheDAO",
        "symbol": "TheDAO",
        "decimals": 16.0
    }
}

#################
# ACCOUNT CLASS #
#################

class Account():
    
    def __init__(self, address):
        self.address = address
        self.transaction_receipts = None
        self.query_range = {}
    
    @property
    def address(self):
        return self.__address
    
    @property
    def transaction_receipts(self):
        if self.__transaction_receipts is None:
            my_bigquery = BigQuery()
            self.__transaction_receipts = my_bigquery.get_transaction_receipts(self)
        return self.__transaction_receipts
    
    @property
    def query_range(self):
        return self.__query_range
    
    @address.setter
    def address(self, val):
        if val[:2] == "0x" and len(val) == 42 and all(c in string.hexdigits for c in val[2:]):
            self.__address = val.lower()
        else:
            raise ValueError("Invalid Ethereum address")
    
    @transaction_receipts.setter
    def transaction_receipts(self, val):
        self.__transaction_receipts = val
        
    @query_range.setter
    def query_range(self, dict):
        if 'start' in dict and 'end' in dict:
            self.__query_range = {"start": dict['start'], "end": dict['end']}
        elif 'start' in dict:
            self.__query_range = {"start": dict['start']}
        elif 'end' in dict:
            self.__query_range = {"end": dict['end']}
        else:
            self.__query_range = {}

##################       
# CONTRACT CLASS #
##################
        
class Contract(Account):
    
    def __init__(self, address):
        Account.__init__(self, address)
        
        self.abi = None
        self.creation_date = None
        self.event_logs = None
        self.functions = None
        self.events = None
        
    @property
    def abi(self):
        """Attempts to find the ABI the first time the property is called.
        
        The ABI used will be the first match in:
            1. ABI for `address` from Etherscan
            2. A blank list - []
        """
        if self.__abi is None:
            my_etherscan = Etherscan()
            self.__abi = my_etherscan.get_abi(self.address)
        return self.__abi
    
    @property
    def creation_date(self):
        """Attempts to find the contract deployment date the first time the property is called.
        
        Returns a date formatted as a "YYYY-MM-DD" string.
        
        The date used will be the first match in:
            1. Normal transaction dataset
            2. Internal transaction dataset
        """
        if self.__creation_date is None:
            my_bigquery = BigQuery()
            self.__creation_date = my_bigquery.get_contract_creation_date(self)
        return self.__creation_date
    
    @property
    def event_logs(self):
        if self.__event_logs is None:
            my_bigquery = BigQuery()
            self.__event_logs = my_bigquery.get_event_logs(self)
        return self.__event_logs
    
    @abi.setter
    def abi(self, val):
        self.__abi = val
        
    @creation_date.setter
    def creation_date(self, val):
        self.__creation_date = val
        
    @event_logs.setter
    def event_logs(self, val):
        self.__event_logs = val

    @property
    def functions(self):
        if self.__functions is None:
            self.__functions = {}
            for item in self.abi:
                if item['type'] == 'function':
                    function_name = item['name']
                    input_types = []
                    data = OrderedDict()

                    for input_ in item['inputs']:
                        input_types.append(input_['type'])
                        data.update({input_['name']: input_['type']})

                    function_prehash = "{0}({1})".format(function_name, ",".join(input_types))
                    function_signature = get_function_signature(function_prehash)

                    self.__functions[function_signature] = {
                        "function_name": function_name,
                        "data": data
                    }
        return self.__functions

    @functions.setter
    def functions(self, val):
        self.__functions = val
    
    @property
    def events(self):
        if self.__events is None:
            self.__events = {}
            for item in self.abi:
                if item['type'] == 'event':
                    event_name = item['name']
                    input_types = []
                    topics = OrderedDict()
                    data = OrderedDict()
                    anonymous = item['anonymous']

                    for input_ in item['inputs']:
                        input_types.append(input_['type'])

                        if input_['indexed']:
                            topics.update({input_['name']: input_['type']})
                        else:
                            data.update({input_['name']: input_['type']})

                    if not anonymous:
                        event_prehash = "{0}({1})".format(event_name, ",".join(input_types))
                        event_hash = get_event_hash(event_prehash)
                    else:
                        event_hash = "Anonymous"

                    self.__events[event_hash] = {
                        "event_name": event_name,
                        "topics": topics,
                        "data": data
                    }
        return self.__events

    @events.setter
    def events(self, val):
        self.__events = val

###############
# TOKEN CLASS #
###############

class Token(Contract):

    def __init__(self, address):
        """Initializes a new `Token` object. Subclass of `Contract`.
        
        Properties are set from an external source the first time they are needed. After that, they are stored on the object:
          * name
          * symbol
          * decimals
          * total_supply
        """
        
        Contract.__init__(self, address)
        
        self.name = None
        self.symbol = None
        self.decimals = None
        self.total_supply = None

    @property
    def name(self):
        """Attempts to find a name for the token the first time the property is called.
        
        The name used will be the first match in:
            1. Entry in `exception_list`
            2. Contract data for `name()` function
            3. A blank string
        """
        
        if self.__name is None:
            try:
                self.__name = exception_list[self.address]['name']
            except:
                my_infura = Infura()
                result = my_infura.eth_call(self.address, "name()")
                try:
                    name = hex_to_string(result[-64:])
                    self.__name = name
                except:
                    warnings.warn("Could not find name for {}. A fallback value of \"\" has been applied.".format(self.address))
                    self.__name = ""
        return self.__name

    @property
    def symbol(self):
        """Attempts to find symbol for the token the first time the property is called.
        
        The symbol will be the first match in:
            1. Entry in `exception_list`
            2. Contract data for `symbol()` function
            3. A blank string
        """
        
        if self.__symbol is None:
            try:
                self.__symbol = exception_list[self.address]['symbol']
            except:
                my_infura = Infura()
                result = my_infura.eth_call(self.address, "symbol()")
                try:
                    symbol = hex_to_string(result[-64:])
                    self.__symbol = symbol
                except:
                    warnings.warn("Could not find symbol for {}. A fallback value of \"\" has been applied.".format(self.address))
                    self.__symbol = ""
        return self.__symbol

    @property
    def decimals(self):
        """Attempts to find decimals for the token the first time the property is called.
        
        The decimals will be the first match in:
            1. Entry in `exception_list`
            2. Contract data for `decimals()` function
            3. 18
        """
        
        if self.__decimals is None:
            try:
                self.__decimals = exception_list[self.address]['decimals']
            except:
                my_infura = Infura()
                result = my_infura.eth_call(self.address, "decimals()")
                try:
                    decimals = hex_to_float(result)
                    self.__decimals = decimals
                except:
                    warnings.warn("Could not find decimals for {}. A fallback value of 18.0 has been applied.".format(self.address))
                    self.__decimals = 18
        return self.__decimals

    @property
    def total_supply(self):
        """Attempts to find total supply for the token the first time the property is called.
        
        `total_supply` returns an amount of tokens, based on the token's `decimals` property.
        
        The total supply will be the first match in:
            1. Entry in `exception_list`
            2. Contract data for `totalSupply()` function
            3. 0
        """
        
        if self.__total_supply is None:
            try:
                self.__total_supply = exception_list[self.address]['total_supply']
            except:
                my_infura = Infura()
                result = my_infura.eth_call(self.address, "totalSupply()")
                try:
                    total_supply = hex_to_float(result, decimals=self.decimals)
                    self.__total_supply = total_supply
                except:
                    warnings.warn("Could not find total_supply for {}. A fallback value of 0.0 has been applied.".format(self.address))
                    self.__total_supply = 0
        return self.__total_supply

    @name.setter
    def name(self, val):
        self.__name = val

    @symbol.setter
    def symbol(self, val):
        self.__symbol = val

    @decimals.setter
    def decimals(self, val):
        self.__decimals = val

    @total_supply.setter
    def total_supply(self, val):
        self.__total_supply = val

### INFURA ###

class Infura():
    
    def __init__(self):
        self.project_id = os.environ.get("INFURA_PROJECT_ID")
    
    @property
    def url(self):
        return "https://mainnet.infura.io/v3/{0}".format(self.project_id)
    
    @property
    def project_id(self):
        return self.__project_id
    
    @project_id.setter
    def project_id(self, val):
        self.__project_id = val
    
    def eth_call(self, to, function):
        """Calls `function` on address `to`. Functions should be input as plain text (e.g. "name()").
    
        Any data that is returned will be in hex format and will need to be converted by the user as needed.
        """
        data = get_function_signature(function)
        url = self.url
        data = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": to,
                        "data": data},
                        "latest"],
            "id": 1
        }
        r = requests.post(url, json=data)
        try:
            if r.json()['result'] == '0x':
                return None
            return r.json()['result']
        except:
            warnings.warn("Request to Infura failed. Check your API key.")
            return None
    
    def eth_blockNumber(self):
        """Calls `eth_blockNumber`"""
        
        url = self.url
        data = {
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1
        }
        r = requests.post(url, json=data)
        return r
        

### ETHERSCAN ###

class Etherscan():
    
    def __init__(self):
        self.api_key = os.environ.get("ETHERSCAN_API_KEY")
    
    @property
    def api_key(self):
        return self.__api_key
    
    @api_key.setter
    def api_key(self, val):
        self.__api_key = val
        
    def get_abi(self, address):
        """Requests ABI from Etherscan for `address`"""
        r = self.get_abi_response(address)
        try:
            if r.json()['status'] == '1':
                return json.loads(r.json()['result'])
            else:
                warnings.warn("No ABI found on Etherscan for {0}.".format(address))
                return []
        except:
            warnings.warn("Request to Etherscan failed. Check your API key.")
            return []

    def get_abi_response(self, address):
        """Requests ABI from Etherscan for `address`"""
        url = "https://api.etherscan.io/api?module=contract&action=getabi&address={0}&apikey={1}".format(address, self.api_key)
        r = requests.post(url)
        return r

### BIGQUERY ###

class BigQuery():

    def __init__(self):
        pass
    
    def run_query(self, sql, location='US'):
        """Queries BigQuery with provided the SQL.
    
        Location must match the dataset(s) being queried.
        """
    
        #logging.debug("Location:\n{0}\n".format(location))
        #logging.debug("SQL Query:\n{0}\n".format(sql))

        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig()
        job_config.use_query_cache = True # Cache available for 24 hours on identical queries
        query_job = client.query(
            sql,
            location=location,      # Location must match that of the dataset(s)
            job_config=job_config)  # API request
    
        df = query_job.to_dataframe()
        return df
    
    def get_contract_creation_date(self, contract):
        """Looks for a contract's creation date using two queries:
        1. Checks normal transactions.
        2. Checks internal transactions.
    
        If a creation date cannot be found, a default value of January 1, 1970 will be used.
        """
        sql = """
SELECT block_timestamp
FROM `{0}`
WHERE address = "{1}"
LIMIT 1""".format(public_dataset['contracts'], contract.address)
    
        result = self.run_query(sql)
    
        try:
            return make_tz_naive(result.iloc[0]).strftime("%Y-%m-%d")
        except:
            # Not found in normal transactions
            # Look for internal transactions (more costly search)
            sql = """
SELECT block_timestamp
FROM `{0}`
WHERE to_address = "{1}"
  AND trace_type = "create"
LIMIT 1""".format(public_dataset['traces'], contract.address)
            
        result = self.run_query(sql)
            
        try:
            return make_tz_naive(result.iloc[0]).strftime("%Y-%m-%d")
        except:
            warnings.warn("Contract creation date not found for {}".format(contract.address))
            return dt.datetime(1970,1,1).strftime("%Y-%m-%d")
    
    def get_event_logs(self, contract):
        """Returns a Pandas dataframe of the contract's event logs with columns:
        * transaction_hash
        * block_timestamp (UTC)
        * address
        * topics (formatted, if possible)
        * data (formatted, if possible)
        
        If a `query_range` is found on the `contract`, the results will be limited to that timeframe.
        """
        
        date_sql = ""
        if 'start' in contract.query_range:
            date_sql += "AND block_timestamp >= \"{0}\"".format(contract.query_range['start'])
        if 'end' in contract.query_range:
            date_sql += "\nAND block_timestamp < \"{0}\"".format(contract.query_range['end'])
            
        sql = """
SELECT
  transaction_hash
  ,block_timestamp
  ,address
  ,(SELECT topic FROM UNNEST(topics) topic WITH OFFSET pos WHERE pos = 0) as topics_0
  ,(SELECT topic FROM UNNEST(topics) topic WITH OFFSET pos WHERE pos = 1) as topics_1
  ,(SELECT topic FROM UNNEST(topics) topic WITH OFFSET pos WHERE pos = 2) as topics_2
  ,(SELECT topic FROM UNNEST(topics) topic WITH OFFSET pos WHERE pos = 3) as topics_3
  ,data as transaction_data
FROM `{0}`
WHERE address = "{1}"
{2}
        """.format(public_dataset['logs'], contract.address, date_sql)
        
        result = self.run_query(sql)
        if result.shape[0] > 0:
            result = CleanDf().clean_event_logs_df(result, contract)
        return result
    
    def get_transaction_receipts(self, account):
        """Returns a Pandas dataframe of the account's succesful 
        transactions.

        Columns:
        * transaction_hash
        * block_timestamp (UTC)
        * from_address
        * to_address
        * value
        * function_signature
        * function_data
        
        For `Contract` and `Token` objects with `abi`, the function
        names and parameters will be returns instead of signature
        and data.
        
        If a `query_range` is found on the `account`, the results 
        will be limited to that timeframe.
        """
        
        date_sql = ""
        if 'start' in account.query_range:
            date_sql += "AND block_timestamp >= \"{0}\"\n".format(account.query_range['start'])
        if 'end' in account.query_range:
            date_sql += "AND block_timestamp < \"{0}\"".format(account.query_range['end'])
            
        sql = """
SELECT
  `hash` as transaction_hash,
  block_timestamp,
  from_address,
  to_address,
  value,
  SUBSTR(input, 1, 10) as function_signature,
  SUBSTR(input, 11) as function_data
FROM `{0}`
WHERE (to_address = "{1}"
  OR from_address = "{1}")
  AND receipt_status = 1
{2}
        """.format(public_dataset['transactions'], account.address, date_sql)
        
        result = self.run_query(sql)
        if result.shape[0] > 0:
            result = CleanDf().clean_transaction_receipts_df(result, account)
        return result
        
### HELPERS ###

def get_function_signature(val):
    """Encodes `val` to 4-byte function signature."""
    keccak_hash = keccak.new(digest_bits=256)
    keccak_hash.update(val.encode('utf-8'))
    return "0x" + keccak_hash.hexdigest()[:8]

def get_event_hash(val):
    """Encodes `val` to event hash."""
    keccak_hash = keccak.new(digest_bits=256)
    keccak_hash.update(val.encode('utf-8'))
    return "0x" + keccak_hash.hexdigest()

def hex_to_string(val):
    """Converts hex string to utf-8 string.
    
    Accepts padded or unpadded values.
    """
    if val is None:
        return ""
    s = val.strip('0x').rstrip('0')
    if len(s) % 2 == 1:
        s += "0"
    return bytes.fromhex(s).decode('utf-8')

def hex_to_float(val, decimals = 0):
    """Converts hex string, int or float to float. Accepts `decimals`.
    
    Returns a float or error (which should be handled in situ).
    """

    try:
        val = int(val, 16)
    except:
        val = val
        
    return float(val / 10 ** decimals)

def hex_to_address(val):
    """Converts hex string to a clean Ethereum address.
    
    Accepts padded or unpadded values.
    
    Returns a 0x formatted address (string).
    """
    
    return "0x{}".format(val[-40:])

def hex_to_bool(val):
    """Converts hex string or boolean integer to a clean boolean value.
    
    Returns a boolean data type True or False
    """
    
    if type(val) == str:
        val = val.strip("0x").rstrip("0")
        
    if val:
        return True
    else:
        return False
    
def make_tz_naive(val):
    """Converts datetime to timezone naive datetime (rounded down to nearest date)."""
    
    return dt.datetime(val.year, val.month, val.day)

def clean_hex_data(val, val_type):
    if val_type == "address":          # convert to address
        return hex_to_address(val)
    elif val_type.startswith("uint") or val_type.startswith("int"):  # convert to float
        return hex_to_float(val)
    elif val_type == "string":
        return hex_to_string(val)
    elif val_type.startswith("bytes"): # keep as hex
        return val
    elif val_type == "bool":
        return hex_to_bool(val)
    else:                              # warn and keep as hex
        warnings.warn("Could not convert data type {0}".format(val_type))
        return val


class CleanDf:
    """
    Converts hexadecimal data from transactions and events into a readable format according to contract ABI
    
    Attributes:
        df(Pandas object - DataFrame): Holds all hexadecimal data to be converted
        count(int): Helps methods with determining the index of a row that is being processed
        raw_rows(list): Holds hexadecimal data for a specific transaction or event
        data_type(string): Type determines how is a row processed
    """

    def __init__(self):
        self.df = None
        self.count = 0
        self.raw_rows = []
        self.data_type = None

    def clean_transaction_receipts_df(self, df, contract):
        """
        Cleans transaction receipts dataframe and adds columns with formatted data if possible.
        Args:
            df(Pandas object - DataFrame): Holds all hexadecimal data to be converted
            contract(Contract object): Holds information to convert DataFrame
        Returns:
            df: Converted DataFrame
        """

        self.df = df
        self.naive_timestamp()

        # Clean empty inputs
        for row in self.df.itertuples():
            if row.function_signature == '0x':
                self.df.at[row.Index, 'function_signature'] = None
            if row.function_data == '':
                self.df.at[row.Index, 'function_data'] = None
        
        if (contract.__class__.__name__ == "Contract" or contract.__class__.__name__ == "Token") and len(contract.functions) > 0:
            # Create new column for function_name
            self.df['function_name'] = None
            for row in self.df.itertuples():
                if row.function_signature is not None and len(row.function_signature) == 10:
                    self.df.at[row.Index, 'function_name'] = contract.functions[row.function_signature]['function_name']
            
            # Fill columns for function_data
            for function_signature in contract.functions:
                for data_name in contract.functions[function_signature]['data']:
                    self.df['param_' + data_name] = None
            
            # Iterate through all of the rows(transactions)
            for row in self.df.itertuples():
                try:
                    data = contract.functions[row.function_signature]['data']
                except KeyError:
                    continue

                self.count = 0
                raw_rows_string = self.df.at[row.Index, "function_data"]
                self.raw_rows = [raw_rows_string[i: i + 64] for i in range(0, len(raw_rows_string), 64)]
                # Iterates through all of the data for a specific row(transaction)
                for data_name in data:
                    self.data_type = contract.functions[row.function_signature]['data'][data_name]
                    # Checking if row is empty
                    if self.raw_rows[self.count] is None:
                        self.count += 1
                        continue
                    result = self.iterate_data()
                    self.df.at[row.Index, 'param_' + data_name] = result

            # Delete raw data & empty columns
            self.df.drop(columns=["function_signature", "function_data"], inplace=True)
            self.df.dropna(axis='columns', how='all', inplace=True)

        return self.df


    def clean_event_logs_df(self, df, contract):
        """
        Cleans event logs dataframe and tries to add columns with formatted data.
        Args:
            df(Pandas object - DataFrame): Holds all hexadecimal data to be converted
            contract(Contract object): Holds information to convert DataFrame
        Returns:
            df: Converted DataFrame
        """

        self.df = df
        self.naive_timestamp()

        if len(contract.events) > 0:
            # Count number of anonymous events
            anon_events = []
            for item in contract.events:
                if item == 'Anonymous':
                    anon_events.append(contract.events[item])
            
            # Fill new column for event_name
            self.df['event_name'] = None
            for row in self.df.itertuples():
                try:
                    self.df.at[row.Index, 'event_name'] = contract.events[row.topics_0]['event_name']
                except KeyError:
                    if len(anon_events) == 1:
                        self.df.at[row.Index, 'event_name'] = contract.events['Anonymous']['event_name']
                    else:
                        warnings.warn("Could not find event_name for {}.".format(row.topics_0))
                        self.df.at[row.Index, 'event_name'] = None
            
            # Fill columns for other data  
            for event_hash in contract.events:
                for topic_name in contract.events[event_hash]['topics']:
                    self.df['topic_' + topic_name] = None
                for data_name in contract.events[event_hash]['data']:
                    self.df['data_' + data_name] = None
            
            # Iterates through all of the data for a specific row(event)
            for row in self.df.itertuples():
                try:
                    topics = contract.events[row.topics_0]['topics']
                    data = contract.events[row.topics_0]['data']
                    t = 1 # Start iteration at topic_1
                except KeyError:
                    topics = contract.events['Anonymous']['topics']
                    data = contract.events['Anonymous']['data']
                    t = 0 # Start iteration at topic_0
                
                # Iterate through topics
                for topic_name in topics:
                    topic_type = topics[topic_name]
                    source = self.df.at[row.Index, "topics_{}".format(t)]

                    # Checking for unsupported type
                    stat1 = "[" in  topic_type
                    stat2 = topic_type == "bytes" or "string" in topic_type
                    if stat1 or stat2:
                        warnings.warn("{} is not yet supported passed as topic".format(topic_type))
                        self.df.at[row.Index, 'data_' + topic_name] = source
                        t += 1
                        continue
                    
                    self.df.at[row.Index, 'topic_' + topic_name] = clean_hex_data(source, topic_type)
                    t += 1
                
                self.count = 0
                raw_rows_string = self.df.at[row.Index, "transaction_data"]
                if raw_rows_string:
                    self.raw_rows = [raw_rows_string[2 + i: i + 66] for i in range(0, len(raw_rows_string), 64)]

                # Iterate through data(events)
                for data_name in data:
                    self.data_type = data[data_name]
                    # Checking if row is empty
                    if self.raw_rows[self.count] is None:
                        self.count += 1
                        continue
                    self.df.at[row.Index, 'data_' + data_name] = self.iterate_data()

            # Delete raw data & empty columns
            self.df.drop(columns=["topics_0", "topics_1", "topics_2", "topics_3", "transaction_data"], inplace=True)
            self.df.dropna(axis='columns', how='all', inplace=True)
        
        return self.df

    def iterate_data(self):
        """
        Iterates through rows and process them according to ABI
        
        Returns:
            result(str/list/int): Converted row
        """

        raw_row = self.raw_rows[self.count]
        # Checking if it is a static array
        stat1 = self.data_type[-2].isdigit()
        # Checking if it is an array
        stat2 = "[" in self.data_type
        # String and bytes are dynamic arrays
        stat3 = "string" == self.data_type or "bytes" ==  self.data_type
        
        # Static array
        if stat1 and stat2:
            result = self.static_array(self.count)
        # Dynamic array
        elif stat2 or stat3:
            array_offset = int(hex_to_float(raw_row)/32)  # offset of the first element in the array 
            result = self.dynamic_array(array_offset)
            self.raw_rows[array_offset] = None
        # Not an array  
        else:
            result = clean_hex_data(raw_row, self.data_type)

        self.count += 1
        return result

    def dynamic_array(self, ind):
        """
        Handles dynamic arrays
        
        Args:
            ind(int): Position of the row that is being processed
        Returns:
            result(list): Processed data
        """

        array_length = int(clean_hex_data(self.raw_rows[ind], "int"))
        data_type = self.data_type
        
        # String or Bytes
        if data_type == "string" or data_type == "bytes":
            num_rows = int(ceil(array_length/32))
            cut_null = None if (array_length % 32) == 0 else (32 - array_length % 32)*2*-1
            # Iterating through rows of elements and creating a string with them
            result = clean_hex_data("".join([self.raw_rows[i + ind + 1] for i in range(num_rows)])[:cut_null], data_type) 
            for row_index in range(ind, num_rows + 1 + ind):
                self.raw_rows[row_index] = None
            return result
        # Dynamic-Dynamic array
        elif "[][]" in data_type or data_type == "string[]":
            self.data_type = data_type[:-2]
            result = [self.dynamic_array(int(hex_to_float(self.raw_rows[ind + 1 +i])/32) + ind + 1) for i in range(array_length)]
        # Dynamic-Static array
        elif "][]" in data_type:
            self.data_type = data_type.replace("[]", "")
            inner_array_size = int(data_type[data_type.index("[") + 1: data_type.index("][")])
            result = [self.static_array(i*inner_array_size + ind + 1) for i in range(array_length)]
        # 1D dynamic array
        else:
            # Iterating through rows of elements and creating a list with them
            result = [clean_hex_data(self.raw_rows[i + 1 + ind],
                      data_type.rstrip("[]")) for i in range(array_length)]
        
        # Making used rows empty
        for row_index in range(ind, ind + array_length + 1):
            self.raw_rows[row_index] = None
        return result

    def static_array(self, ind):
        """
        Handles dynamic arrays
        
        Args:
            ind(int): Position of the row that is being processed
        Returns:
            result(list): Processed data
        """

        data_type = self.data_type
        
        if data_type.count("[") == 1:
            array_size = int(data_type[data_type.index("[") + 1: -1])
        else:
            array_size = int(data_type[data_type.index("][") + 2: -1])

        # Static-Dynamic array
        if "[]" in data_type or "string" in data_type or "bytes[" in data_type:
            self.data_type = data_type.replace("[{}]".format(array_size), "")
            result = [self.dynamic_array(int(hex_to_float(self.raw_rows[ind + i])/32) + ind) for i in range(array_size)]
        # Static-Static array
        elif data_type.count("[") == 2:
            self.data_type = data_type = data_type.replace("[{}]".format(array_size), "")
            inner_array_size = int(data_type[data_type.index("[") + 1: -1])
            result = [self.static_array(i*(inner_array_size) + ind) for i in range(array_size)]
        # 1D static array
        else:
            result = [clean_hex_data(self.raw_rows[ind + i], data_type.rstrip(
                      "[{}]".format(array_size))) for i in range(int(array_size))]
        
        # Making used rows empty
        for row_index in range(ind, ind + array_size):
            self.raw_rows[row_index] = None
        return result

    def naive_timestamp(self):
        '''Make timestamp tz naive & re-order by timestamp'''
        self.df['block_timestamp'] = self.df['block_timestamp'].dt.tz_localize(None)
        self.df.sort_values(by=['block_timestamp'], ascending=True, inplace=True)
        self.df.reset_index(drop=True, inplace=True)

    
