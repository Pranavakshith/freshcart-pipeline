"""
Model Evaluation and Performance Diagnostics Block for Mage Pipeline (Part C3).
Author: Pranavakshith (23BCS054)

Calculates evaluation metrics (accuracy, macro precision, macro recall, and macro F1)
along with the multi-class confusion matrix for the trained Decision Tree classifier.
"""

from typing import Dict, Any
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix
)

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
def transform(df: pd.DataFrame, *args, **kwargs) -> Dict[str, Any]:
    """
    Compute comprehensive classification metrics and confusion matrix.
    Prints formatted diagnostic summaries to pipeline logs.
    """
    y_actual = df['target']
    y_pred = df['prediction']

    metrics = {
        'accuracy': float(accuracy_score(y_actual, y_pred)),
        'precision_macro': float(precision_score(y_actual, y_pred, average='macro')),
        'recall_macro': float(recall_score(y_actual, y_pred, average='macro')),
        'f1_macro': float(f1_score(y_actual, y_pred, average='macro')),
    }
    cm = confusion_matrix(y_actual, y_pred)

    print("\n" + "=" * 45)
    print("      MAGE ML MODEL PERFORMANCE REPORT       ")
    print("=" * 45)
    for metric_name, val in metrics.items():
        print(f"  {metric_name:<20}: {val:.4f}")

    print("\n--- Confusion Matrix ---")
    distinct_labels = sorted(y_actual.unique())
    cm_df = pd.DataFrame(
        cm,
        index=[f"Actual_{lbl}" for lbl in distinct_labels],
        columns=[f"Pred_{lbl}" for lbl in distinct_labels]
    )
    print(cm_df.to_string())
    print("=" * 45 + "\n")

    return {'metrics': metrics, 'confusion_matrix': cm}


@test
def test_output(output: Dict[str, Any], *args, **kwargs) -> None:
    """Mage test asserting valid metrics and confusion matrix output."""
    assert output is not None, "Evaluation output must not be None."
    assert 'metrics' in output, "Missing 'metrics' key in evaluation output."
    assert 'confusion_matrix' in output, "Missing 'confusion_matrix' key in evaluation output."
    assert output['metrics']['accuracy'] >= 0.80, (
        f"Validation accuracy ({output['metrics']['accuracy']:.3f}) fell below the 0.80 threshold!"
    )
