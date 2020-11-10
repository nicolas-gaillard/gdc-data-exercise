"""Testing ads' cleaning"""

import pytest
import pandas as pd
from src.FileManager import FileManager

@pytest.fixture
def ads_sample():
    return pd.read_csv('./data/sample_ads.csv')

def test_clean_real_estate(ads_sample):
    df = FileManager._clean_real_estate(ads_sample)
    assert df["category"].iloc[0] == "real_estate"
    assert df["category"].iloc[1] == "real_estate"
    assert df["category"].iloc[2] == "real_estate"
    assert df["category"].iloc[3] == "real_estate"
