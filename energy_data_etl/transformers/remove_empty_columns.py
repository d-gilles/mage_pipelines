from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame
import pandas as pd
import os

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Execute Transformer Action: ActionType.REMOVE

    Docs: https://docs.mage.ai/guides/transformer-blocks#remove-columns
    """
    file = df[1]
    df = pd.read_parquet(file)
    

    # columnnames to drop
    to_drop = [col for col in df.columns if col[1] == 'Actual Consumption']
    df = df.drop(columns=to_drop)

    df.columns = df.columns.droplevel(1)

    print(df.shape)

    return [df, file]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    columns = ['Biomass', 'Fossil Brown coal/Lignite', 'Fossil Gas',
       'Fossil Hard coal', 'Fossil Oil', 'Geothermal', 'Hydro Pumped Storage',
       'Hydro Run-of-river and poundage', 'Hydro Water Reservoir', 'Other',
       'Other renewable', 'Solar', 'Waste', 'Wind Offshore', 'Wind Onshore']

    assert output.shape == (96, 15), "export data frame doesn't have the right shape"
    assert list(output.columns) == columns, "export data frame doesn't have the right column names"
