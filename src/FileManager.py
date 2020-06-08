import ast
import datetime as dt
import logging
import re
import numpy as np
import pandas as pd
import sys
from sqlalchemy import create_engine
from sqlalchemy import types

class FileManager():
    """Class used to read, clean and insert CSVs in database"""
    DATA_FOLDER_PATH = './data'
    ADS_TRANSACTION = 'ads_transaction.csv'
    ADS = 'ads.csv'
    REFERRALS = 'referrals.csv'
    USERS = 'users.csv'
    SCHEMA = 'gdc'

    def __init__(
            self,
            db_user: str,
            db_pwd: str,
            db_name: str,
            provider: str,
            port: str
    ) -> None:
        self.__engine = create_engine(
            f"{provider}://{db_user}:{db_pwd}@db:{port}/{db_name}"
            # f"{provider}://{db_user}:{db_pwd}@localhost:{port}/{db_name}"
        )

        self.__date_parse = lambda x: np.datetime64('NaT') if pd.isnull(x) else dt.datetime.strptime(x, '%Y-%m-%d')
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    def clean_users(self, nrows: int = None) -> pd.DataFrame:
        logging.info(f"--> Cleaning : {self.USERS}")
        path = f"{self.DATA_FOLDER_PATH}/{self.USERS}"
        df = pd.read_csv(
            path,
            usecols=[
                'id', 'age', 'birthdate', 'city', 'created_at', 'sex', 'lat',
                'long', 'password', 'utm_source', 'utm_medium', 'utm_campaign',
                'firstname', 'lastname', 'user_agent', 'misc'
            ],
            parse_dates=['birthdate', 'created_at'],
            date_parser=self.__date_parse,
            nrows=nrows
        )

        columns = {
            "id": "user_id"
        }
        df.rename(columns=columns, inplace=True)
        sex_replacements = {
            'sex': {
                r'([mM]\.?$|[mM]r\.?$|[mM]ister\.?$)': 'M',
                r'([mM]rs\.?$|[mM]s\.?$|[mM]iss\.?$)': 'F'
            }
        }
        df.replace(sex_replacements, regex=True, inplace=True)

        # Parsing misc
        df = df.join(df['misc'].apply(lambda x: ast.literal_eval(x)).apply(pd.Series))
        df['phone_number'] = df['phone_number'].apply(lambda x: self.__format_phone(x))
        user_connections = df[['user_id', 'connections']]
        self.__create_users_connection(user_connections)
        df = df.drop(['misc', 'connections'], axis=1)

        logging.info("End of cleaning, insertion start")
        self.__insert(df, 'users', dtype=None)
        logging.info("End of insertion")

        return df

    def clean_ads(self, nrows: int = None) -> pd.DataFrame:
        logging.info(f"--> Cleaning : {self.ADS}")
        path = f"{self.DATA_FOLDER_PATH}/{self.ADS}"

        df = pd.read_csv(
            path,
            usecols=[
                'owner_id', 'title', 'category', 'price', 'city', 'created_at',
                'deleted_at', 'id'
            ],
            parse_dates=['created_at', 'deleted_at'],
            date_parser=self.__date_parse,
            nrows=nrows
        )

        real_estate_regex = r're?a?l[\w\s]?estate'
        df['category'] = df['category'].str.lower().replace(
            real_estate_regex,
            'real_estate',
            regex=True
        )
        columns = {
            "id": "ad_id"
        }
        df.rename(columns=columns, inplace=True)
        cols = list(df.columns)
        cols = [cols[-1]] + cols[:-1]
        df = df[cols]

        logging.info("End of cleaning, insertion start")
        self.__insert(df, 'ads', None)
        logging.info("End of insertion")

        return df

    def clean_ads_transaction(self, nrows: int = None) -> pd.DataFrame:
        logging.info(f"--> Cleaning : {self.ADS_TRANSACTION}")
        path = f"{self.DATA_FOLDER_PATH}/{self.ADS_TRANSACTION}"
        df = pd.read_csv(
            path,
            parse_dates=['created_at'],
            date_parser=self.__date_parse,
            nrows=nrows
        )

        columns = {
            "id": "ad_transaction_id"
        }
        df.rename(columns=columns, inplace=True)
        df['sold_price'] = df['sold_price'].round(2)

        logging.info("End of cleaning, insertion start")
        self.__insert(df, "ads_transaction", None)
        logging.info("End of insertion")

        return df

    def clean_referrals(self, nrows: int = None) -> pd.DataFrame:
        path = f"{self.DATA_FOLDER_PATH}/{self.REFERRALS}"
        df = pd.read_csv(
            path,
            parse_dates=['created_at', 'deleted_at'],
            date_parser=self.__date_parse,
            nrows=nrows
        )

        columns = {
            "id": "referral_id"
        }
        df.rename(columns=columns, inplace=True)
        df.deleted_at = df.deleted_at.fillna(np.datetime64('NaT'))

        logging.info("End of cleaning, insertion start")
        self.__insert(df, "referrals", None)
        logging.info("End of insertion")

        return df

    def __insert(
            self,
            df: pd.DataFrame,
            table_name: str,
            dtype: dict,
            chunksize: int = 1000,
            mode: str = 'replace'
    ) -> None:
        df.to_sql(
            name=table_name,
            con=self.__engine,
            index=False,
            schema=self.SCHEMA,
            if_exists=mode,
            dtype=dtype,
            chunksize=chunksize,
            method='multi'
        )

    def __create_users_connection(self, df: pd.DataFrame) -> None:
        logging.info("Creating users_connection")
        users_connection = df.explode('connections')
        users_connection['connections'] = pd.to_datetime(users_connection['connections'], unit='s')

        self.__insert(users_connection, 'users_connection', None)
        logging.info("User connections is created")

    @staticmethod
    def __format_phone(phone_number: str) -> str:
        regex = r'(\(?\+33\)?|\(|\)|\s)'
        cleaned_pn = re.sub(regex, "", phone_number)
        return '{0:0>10}'.format(cleaned_pn)
