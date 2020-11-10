"""Testing users' cleaning"""

import pytest
import pandas as pd
from src.FileManager import FileManager

@pytest.fixture
def users_sample():
    return pd.read_csv('./data/sample_users.csv')

def test_clean_sex(users_sample):
    df = FileManager._clean_sex(users_sample)
    assert df["sex"].iloc[0] == "F"
