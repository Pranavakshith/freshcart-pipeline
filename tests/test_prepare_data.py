"""
Pytest unit test for Mage prepare_data block (Part A3 & Part C).
Author: Pranavakshith (23BCS054)
"""

import unittest
from sklearn.datasets import load_iris
from iris_project.transformers.prepare_data import transform


def test_split_is_80_20():
    """Verify that transform() splits the 150-row Iris dataset into 80% train and 20% test."""
    df = load_iris(as_frame=True).frame
    out = transform(df)
    assert (out["split"] == "train").sum() == 120
    assert (out["split"] == "test").sum() == 30


class TestPrepareDataBlock(unittest.TestCase):
    def test_split_proportions(self):
        test_split_is_80_20()


if __name__ == "__main__":
    unittest.main()
