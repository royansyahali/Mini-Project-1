import connection
import os
import sqlparse
import pandas as pd

if __name__ == "__main__":
    # Connection to data source
    conf = connection.config("marketplace_prod")
    conn, engine = connection.get_conn(conf, "Data Source")
    cursor = conn.cursor()

    # Connection to dwh
    conf_dwh = connection.config("dwh")
    conn_dwh, engine_dwh = connection.get_conn(conf_dwh, "DWH")
    cursor_dwh = conn_dwh.cursor()


    # Get Query String
    path_query = os.getcwd() + "/Query/"
    query = sqlparse.format(
        open(path_query + "Query.sql", "r").read(), strip_comments=True
    ).strip()

    dwh_design = sqlparse.format(
        open(path_query + "QueryDWH.sql", "r").read(), strip_comments=True
    ).strip()
    print(dwh_design)
  
    try:
        # Get data
        print("[INFO] service etl is running....")
        df = pd.read_sql(query, engine)

        #  Create Schema DWH
        cursor_dwh.execute(dwh_design)
        conn_dwh.commit()

        # Insert data to DWH
        df.to_sql(
            "dim_orders_royan",
            engine_dwh,
            schema="public",
            if_exists="replace",
            index=False,
        )

        print("[INFO] service etl is success....")
    except Exception as e:
        print("[INFO] service etl is failed!!!")
        print(str(e))
