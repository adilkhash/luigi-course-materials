import pandas as pd
from luigi import IntParameter
from luigi.contrib.sqla import CopyToTable
from sqlalchemy import Numeric, Date

from tasks.yellow_taxi.psql_tasks import AggregateTaxiTripTask


class CopyTaxiTripData2SQLite(CopyToTable):
    year = IntParameter()
    month = IntParameter()

    table = 'nyc_trip_agg_data'
    connection_string = 'sqlite:///sqlite.db'

    columns = [
        (['pickup_date', Date()], {}),
        (['tip_amount', Numeric(2)], {}),
        (['total_amount', Numeric(2)], {}),
    ]

    def requires(self):
        return AggregateTaxiTripTask(year=self.year, month=self.month)

    def rows(self):
        with self.input().open() as csv_file:
            # use pandas not to deal with type conversions
            df = pd.read_csv(csv_file, parse_dates=['pickup_date'])
            rows = df.to_dict(orient='split')['data']
            return rows
