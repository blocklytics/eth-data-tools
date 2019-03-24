import pytest
from ethdata import ethdata
import datetime as dt

def get_data(account):
    bq = ethdata.BigQuery()
    date_sql = ""
    if 'start' in account.query_range:
        date_sql += "AND block_timestamp >= \"{0}\"\n".format(dt.datetime.strptime(account.query_range['start'], '%Y-%m-%d'))
    if 'end' in account.query_range:
        date_sql += "AND block_timestamp < \"{0}\"".format(dt.datetime.strptime(account.query_range['end'], '%Y-%m-%d') + dt.timedelta(days=1))
        
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
        """.format(ethdata.public_dataset['transactions'], account.address, date_sql)
    result = bq.run_query(sql)
    return result

class TestDatetimeError:

    def test_no_tx(self):
        my_account = ethdata.Account("0x1cB424cB77B19143825004d0bd0a4BEE2c5e91A8")
        my_account.query_range = {"start":"2019-01-01", "end":"2019-01-01"}
        # no transactions
        data = get_data(my_account)
        assert data.shape[0] == 0

    def test_tx(self):
        my_account = ethdata.Account("0xa2381223639181689cd6c46d38a1a4884bb6d83c")
        my_account.query_range = {"start":"2019-01-29", "end":"2019-01-29"}
        # On this date, there were 14 normal tx and 2 internal tx
        data = get_data(my_account)
        assert data.shape[0] > 0
