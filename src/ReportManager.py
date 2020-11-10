import pandas as pd
import matplotlib.pyplot as plt

class ReportManager():
    """Class used to build the report"""

    def __init__(
            self,
            users: pd.DataFrame,
            ads: pd.DataFrame,
            referrals: pd.DataFrame,
            ads_transaction: pd.DataFrame,
    ) -> None:
        self.users = users
        self.ads = ads
        self.referrals = referrals
        self.ads_transaction = ads_transaction

    def process(self) -> None:
        ads_with_transaction = pd.merge(
            left=self.ads_transaction,
            right=self.ads,
            left_on='ad_id',
            right_on='ad_id'
        )
        average_nb_of_ad_per_day = self.ads.groupby('created_at')['ad_id'].agg(
            lambda x: x.nunique()/x.count()
        ).mean()
        ads_with_transaction['time_to_be_sold'] = ads_with_transaction['created_at_x'] - ads_with_transaction['created_at_y']

        transaction_per_selling_time = ads_with_transaction.groupby(['category', 'time_to_be_sold'])['ad_id'].count().reset_index(name="count")
        transaction_per_selling_time = pd.pivot_table(
            transaction_per_selling_time,
            values='count',
            columns=['category'],
            index="time_to_be_sold"
        )

        transaction_per_selling_time.plot(kind="bar")
        plt.show()
