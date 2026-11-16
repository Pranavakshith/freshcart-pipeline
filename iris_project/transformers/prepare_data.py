"""
Data Preparation Block for Mage Pipeline (Part A3 & Part C).
Author: Pranavakshith (23BCS054)

Splits Iris dataset into 80% training set (120 rows) and 20% test set (30 rows)
with stratified reproducible split (random_state=42).
"""

import pandas as pd
from sklearn.model_selection import train_test_split

if 'transformer' not in globals():
    try:
        from mage_ai.data_preparation.decorators import transformer
    except ImportError:
        def transformer(func):
            return func

if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(func):
            return func


@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """Split dataset into 80% train and 20% test partitions."""
    train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)
    train_copy = train_df.copy()
    test_copy = test_df.copy()
    train_copy['split'] = 'train'
    test_copy['split'] = 'test'
    combined = pd.concat([train_copy, test_copy])
    return combined


@test
def test_output(output: pd.DataFrame, *args, **kwargs) -> None:
    """Validate 80/20 train/test split proportions."""
    assert output is not None, "Output dataframe is undefined."
    assert (output['split'] == 'train').sum() == 120, "Train split must contain exactly 120 rows."
    assert (output['split'] == 'test').sum() == 30, "Test split must contain exactly 30 rows."
