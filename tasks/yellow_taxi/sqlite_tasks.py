import luigi
import pandas as pd
from luigi.contrib.sqla import CopyToTable
from sqlalchemy import Numeric, Date
from luigi.util import requires
from dateutil.relativedelta import relativedelta

from tasks.yellow_taxi.psql_tasks import AggregateTaxiTripTask


@requires(AggregateTaxiTripTask)
class CopyTaxiTripData2SQLite(CopyToTable):
    table = 'nyc_trip_agg_data'
    connection_string = 'sqlite:///sqlite.db'

    columns = [
        (['pickup_date', Date()], {}),
        (['tip_amount', Numeric(2)], {}),
        (['total_amount', Numeric(2)], {}),
    ]

    def rows(self):
        with self.input().open() as csv_file:
            # use pandas not to deal with type conversions
            df = pd.read_csv(csv_file, parse_dates=['pickup_date'])
            rows = df.to_dict(orient='split')['data']
            return rows


class YellowTaxiDateRangeTask(luigi.WrapperTask):
    start = luigi.MonthParameter()
    stop = luigi.MonthParameter()

    def requires(self):
        current_month = self.start
        while current_month <= self.stop:
            yield CopyTaxiTripData2SQLite(date=current_month)
            current_month += relativedelta(months=1)
