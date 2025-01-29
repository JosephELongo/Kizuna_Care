import pandas as pd
from sqlalchemy import create_engine

conn_string = 'postgresql://joyju:kizuna_care@127.0.0.1/kizuna_care'
db = create_engine(conn_string)
conn = db.connect()


df = pd.read_sql('select * from reporting_view;', conn)

df.to_csv("C:/Users/joyju/Kizuna_Care/Data_Files/Reporting_View_Results.csv", index=False)
