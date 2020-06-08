from src.FileManager import FileManager
from src.ReportManager import ReportManager
from dotenv import load_dotenv
import os
import argparse

if __name__ == "__main__":
    # Argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", action="store_true")

    args = parser.parse_args()
    report = args.report

    # Env file
    load_dotenv()
    db_user = os.getenv("POSTGRES_USER", os.getenv("DB_USER"))
    db_pwd = os.getenv("POSTGRES_PASSWORD", os.getenv("DB_PWD"))
    db_name = os.getenv("POSTGRES_DB", os.getenv("DB_NAME"))
    provider = os.getenv("PROVIDER", "postgresql")
    port = os.getenv("port", "5432")

    # Cleaning & inserting
    fm = FileManager(db_user, db_pwd, db_name, provider, port)
    users = fm.clean_users()
    ads = fm.clean_ads()
    referrals = fm.clean_referrals()
    ads_transaction = fm.clean_ads_transaction()

    # Report
    if report:
        rm = ReportManager(
            users=users,
            ads=ads,
            referrals=referrals,
            ads_transaction=ads_transaction
        )
        rm.process()
