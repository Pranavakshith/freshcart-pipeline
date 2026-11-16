"""
Data Loader Block for Mage Pipeline (Part C).
Author: Pranavakshith (23BCS054)

Loads the canonical Fisher's Iris dataset as a Pandas DataFrame.
"""

import pandas as pd
from sklearn.datasets import load_iris as fetch_iris

if 'data_loader' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_loader
    except ImportError:
        def data_loader(func):
            return func

if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(func):
            return func


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """Load the 150-sample Iris dataset."""
    iris_dataset = fetch_iris(as_frame=True)
    df = iris_dataset.frame
    print(f"[load_iris] Loaded {len(df)} samples across {df.shape[1]} columns.")
    return df


@test
def test_output(output: pd.DataFrame, *args, **kwargs) -> None:
    """Verify that dataset is loaded and has expected dimensions."""
    assert output is not None, "Loaded dataset is undefined."
    assert len(output) == 150, f"Expected 150 rows, got {len(output)}."
    assert 'target' in output.columns, "Target column is missing from loaded dataset."
