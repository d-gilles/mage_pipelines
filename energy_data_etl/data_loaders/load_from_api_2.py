import io
import pandas as pd
from entsoe import EntsoePandasClient
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

client = EntsoePandasClient(api_key='6c399639-c897-4a77-8225-fc0525da549d')


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    execution_date = kwargs['execution_date']
    date = int(execution_date.strftime("%Y%m%d")) -1
    
    # extract year, month, day
    year = date // 10000
    month = (date - year * 10000) // 100
    day = (date - year * 10000 - month * 100)

    # set time interval to yesterday
    interval_start = pd.Timestamp(str(date), tz='Europe/Berlin')
    interval_end = pd.Timestamp(str(date +1), tz='Europe/Berlin')
    country_code = 'DE'  # Germany

    #define data for api call
    type_marketagreement_type = 'A01' # daily
    contract_marketagreement_type = "A01" # daily
    process_type = 'A16' # realized

    path = f'energy_data_etl/data/{year}/{month}'
    file = f'{path}/{day}.parquet'

    print(f'Processing {year}/{month}/{day}')

    if os.path.exists(file):
        print('file already exists')
        return [0, file]

    if not os.path.exists(path):
        print(f'creating path {path}')
        os.makedir(path)
    
    df = client.query_generation(country_code, start=interval_start, end=interval_end, psr_type=None)
    df.to_parquet(file)
    print(f'saved locally: {file}')

    return [0, file]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    
    path = args[0]
    assert os.path.exists(path), 'File is not local available'
