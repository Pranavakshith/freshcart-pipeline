"""
Model Training Block for Mage Pipeline (Part C).
Author: Pranavakshith (23BCS054)

Trains a DecisionTreeClassifier on 80% train split and generates predictions on 20% test split.
Configured with runtime argument 'max_depth' and quality gate test 'test_accuracy'.
"""

import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score

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
    """Train decision tree and append predictions column."""
    max_depth_val = kwargs.get('max_depth', 3)
    train_subset = df[df['split'] == 'train']
    test_subset = df[df['split'] == 'test']

    feature_cols = [
        'sepal length (cm)',
        'sepal width (cm)',
        'petal length (cm)',
        'petal width (cm)'
    ]
    target_col = 'target'

    classifier = DecisionTreeClassifier(max_depth=max_depth_val, random_state=42)
    classifier.fit(train_subset[feature_cols], train_subset[target_col])

    test_predictions = classifier.predict(test_subset[feature_cols])

    result_df = test_subset.copy()
    result_df['prediction'] = test_predictions
    return result_df


@test
def test_output(output: pd.DataFrame, *args, **kwargs) -> None:
    """Validate that prediction column exists and output is well-formed."""
    assert output is not None, "Model transformer output is undefined."
    assert 'prediction' in output.columns, "Predictions column missing from test output."


@test
def test_accuracy(output: pd.DataFrame, *args, **kwargs) -> None:
    """Quality Gate: Halt pipeline execution if validation accuracy < 0.80."""
    calculated_acc = accuracy_score(output['target'], output['prediction'])
    assert calculated_acc >= 0.80, (
        f"Model validation accuracy {calculated_acc:.4f} is below minimum threshold of 0.80"
    )
