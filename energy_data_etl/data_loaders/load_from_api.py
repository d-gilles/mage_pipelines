import io
import pandas as pd
import requests
from entsoe import EntsoePandasClient
import os

# parameter definitions
client = EntsoePandasClient(api_key='6c399639-c897-4a77-8225-fc0525da549d')
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    execution_date = kwargs['execution_date']
    date = int(execution_date.strftime("%Y%m%d"))
    

    
    # extract year, month, day
    year = date // 10000
    month = (date - year * 10000) // 100
    day = date - year * 10000 - month * 100

    print(f'Processing {year}/{month}/{day}')

    interval_start = pd.Timestamp(str(date), tz='Europe/Berlin')
    interval_end = pd.Timestamp(str(date +1 ), tz='Europe/Berlin')
    country_code = 'DE'  # Germany

    type_marketagreement_type = 'A01' # daily
    contract_marketagreement_type = "A01" # daily
    process_type = 'A16' # realized

    df = client.query_generation(country_code, start=interval_start, end=interval_end, psr_type=None)

    # save data
    if not os.path.exists(f'data/{year}/{month}'):
        os.makedirs(f'data/{year}/{month}')
    df.to_parquet(f'data/{year}/{month}/{day}.parquet')
    print(f'Saved data for {year}/{month}/{day}')
    print('-----------------------------------')
    print('-----------------------------------')

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
