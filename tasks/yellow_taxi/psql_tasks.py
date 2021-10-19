import os
import csv
from typing import List

import luigi
import requests
import pandas as pd
from luigi.contrib.postgres import CopyToTable
from luigi.util import requires


def get_filename(year: int, month: int) -> str:
    return f'yellow_tripdata_{year}-{month:02}.csv'


def download_dataset(filename: str) -> requests.Response:
    url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/{filename}'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response


def group_by_pickup_date(
    file_object, group_by='pickup_date', metrics: List[str] = None
) -> pd.DataFrame:
    if metrics is None:
        metrics = ['tip_amount', 'total_amount']

    df = pd.read_csv(file_object)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['pickup_date'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d')
    df = df.groupby(group_by)[metrics].sum().reset_index()
    return df


class DownloadTaxiTripTask(luigi.Task):
    date = luigi.MonthParameter()

    @property
    def filename(self):
        return get_filename(self.date.year, self.date.month)

    def run(self):
        self.output().makedirs()  # in case path does not exist
        response = download_dataset(self.filename)

        with self.output().open(mode='w') as f:
            for chunk in response.iter_lines():
                f.write('{}\n'.format(chunk.decode('utf-8')))

    def output(self):
        return luigi.LocalTarget(os.path.join('yellow-taxi-data', self.filename))


@requires(DownloadTaxiTripTask)
class AggregateTaxiTripTask(luigi.Task):

    def run(self):
        with self.input().open() as input, self.output().open('w') as output:
            self.output().makedirs()
            df = group_by_pickup_date(input)
            df.to_csv(output, index=False)

    def output(self):
        filename = get_filename(self.date.year, self.date.month)[:-4]
        return luigi.LocalTarget(
            os.path.join('yellow-taxi-data', f'{filename}-agg.csv')
        )


@requires(AggregateTaxiTripTask)
class CopyTaxiTripData(CopyToTable):
    host = 'localhost'
    user = 'luigi'
    database = 'luigi_demo'
    password = 'luigi_passwd'

    table = 'nyc_trip_agg_data'

    columns = [
        ('pickup_date', 'date'),
        ('tip_amount', 'numeric'),
        ('total_amount', 'numeric'),
    ]

    def rows(self):
        with self.input().open() as csv_file:
            reader = csv.DictReader(csv_file)
            rows = [row.values() for row in reader]
            return rows

    @property
    def update_id(self):
        return get_filename(self.date.year, self.date.month)
