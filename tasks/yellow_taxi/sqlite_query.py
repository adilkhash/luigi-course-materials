"""
скрипт делает запрос в sqlite.db и получает день в котором было больше всего чаевых
"""
import sqlite3


if __name__ == '__main__':
    conn = sqlite3.connect('sqlite.db')
    cur = conn.cursor()
    for row in cur.execute(
        """
        select pickup_date,
               tip_amount,
               total_amount
        from nyc_trip_agg_data
        where tip_amount = (select max(tip_amount) from nyc_trip_agg_data);
        """
    ):
        print(row)
    conn.close()
