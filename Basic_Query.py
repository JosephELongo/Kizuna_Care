import pandas as pd
import psycopg2


conn = psycopg2.connect("dbname=kizuna_care user=joyju")

cur = conn.cursor()

#query = "insert into meta_reporting_data values ('a', 'b', 'c', 'd', 10, 10, 10, 10);"

#cur.execute(query)
cur.execute("delete from meta_reporting_data")
cur.execute("select * from meta_reporting_data")

records = cur.fetchall()

print(records)

conn.commit()

cur.close()
conn.close()