import os
import csv

import luigi
import requests
import pandas as pd
from luigi.contrib.postgres import CopyToTable


def get_filename(year: int, month: int) -> str:
    return f'yellow_tripdata_{year}-{month:02}.csv'


class DownloadTaxiTripTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    @property
    def filename(self):
        return f'yellow_tripdata_{self.year}-{self.month:02}.csv'

    def run(self):
        url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/{self.filename}'

        self.output().makedirs()  # in case path does not exist
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with self.output().open(mode='w') as f:
            for chunk in response.iter_lines():
                f.write('{}\n'.format(chunk.decode('utf-8')))

    def output(self):
        return luigi.LocalTarget(os.path.join('yellow-taxi-data', self.filename))


class AggregateTaxiTripTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    def requires(self):
        return DownloadTaxiTripTask(year=self.year, month=self.month)

    def run(self):
        with self.input().open() as input, self.output().open('w') as output:
            df = pd.read_csv(input)
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['pickup_date'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d')
            df = df.groupby('pickup_date')['tip_amount', 'total_amount'].sum().reset_index()
            self.output().makedirs()
            df.to_csv(output, index=False)

    def output(self):
        filename = get_filename(self.year, self.month)[:-4]
        return luigi.LocalTarget(os.path.join('yellow-taxi-data', f'{filename}-agg.csv'))


class CopyTaxiTripData(CopyToTable):
    year = luigi.IntParameter()
    month = luigi.IntParameter()

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

    def requires(self):
        return AggregateTaxiTripTask(year=self.year, month=self.month)

    def rows(self):
        with self.input().open() as csv_file:
            reader = csv.DictReader(csv_file)
            rows = [row.values() for row in reader]
            return rows

    @property
    def update_id(self):
        return get_filename(self.year, self.month)
