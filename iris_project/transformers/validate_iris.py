"""
Custom Data Quality and Schema Validation Block for Mage Pipeline (Part C4).
Author: Pranavakshith (23BCS054)

Validates incoming Iris feature vectors:
1. Ensures all expected feature columns are present.
2. Checks that there are no missing (NaN/null) values in feature columns.
3. Enforces that all feature columns are numeric.
4. Verifies that the target column exists and contains exactly 3 distinct species classes.
"""

from typing import List
import pandas as pd

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

EXPECTED_FEATURES: List[str] = [
    'sepal length (cm)',
    'sepal width (cm)',
    'petal length (cm)',
    'petal width (cm)'
]
EXPECTED_CLASS_COUNT: int = 3
EXPECTED_ROW_COUNT: int = 150


@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Execute data quality validation checks on the dataset.
    Returns the dataframe unchanged if all validation assertions hold.
    """
    # 1. Feature presence and null checks
    for feature in EXPECTED_FEATURES:
        if feature not in df.columns:
            raise KeyError(f"[Validation Error] Required feature column '{feature}' is missing.")
        if df[feature].isnull().any():
            raise ValueError(f"[Validation Error] Feature column '{feature}' contains NULL/NaN values.")
        if not pd.api.types.is_numeric_dtype(df[feature]):
            raise TypeError(f"[Validation Error] Feature column '{feature}' is not numeric.")

    # 2. Target column presence and class diversity check
    if 'target' not in df.columns:
        raise KeyError("[Validation Error] Mandatory 'target' column is missing from dataset.")

    distinct_classes = df['target'].nunique()
    if distinct_classes != EXPECTED_CLASS_COUNT:
        raise ValueError(
            f"[Validation Error] Expected {EXPECTED_CLASS_COUNT} classes in target, found {distinct_classes}."
        )

    print(f"[validate_iris] Quality gates passed: {len(df)} rows, 0 nulls, {distinct_classes} balanced classes.")
    return df


@test
def test_output(output: pd.DataFrame, *args, **kwargs) -> None:
    """Mage test asserting valid output dimensions, class diversity, and integrity."""
    assert output is not None, "Pipeline output must not be None."
    assert len(output) == EXPECTED_ROW_COUNT, f"Expected {EXPECTED_ROW_COUNT} rows, got {len(output)}."
    assert output['target'].nunique() == EXPECTED_CLASS_COUNT, f"Target must have {EXPECTED_CLASS_COUNT} classes."
    assert not output.isnull().any().any(), "Dataset contains unexpected missing values."
