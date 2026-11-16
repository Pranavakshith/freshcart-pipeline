"""
Data Exporter Block for Mage Pipeline (Part C).
Author: Pranavakshith (23BCS054)

Exports test predictions to iris_predictions.csv in the project root.
Only executes if all upstream blocks and @test quality assertions pass.
"""

import os
import pandas as pd

if 'data_exporter' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_exporter
    except ImportError:
        def data_exporter(func):
            return func


@data_exporter
def export_data(df: pd.DataFrame, *args, **kwargs) -> None:
    """Export predictions dataframe to iris_predictions.csv."""
    output_destination = os.path.abspath(os.path.join(os.getcwd(), 'iris_predictions.csv'))
    df.to_csv(output_destination, index=False)
    print(f"[save_results] Successfully exported {len(df)} predictions to: {output_destination}")
