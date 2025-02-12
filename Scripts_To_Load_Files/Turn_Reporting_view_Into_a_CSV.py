import pandas as pd
from sqlalchemy import create_engine
import config

db = create_engine(config.conn_string)
conn = db.connect()


df = pd.read_sql('select * from reporting_view;', conn)

df.to_csv(r"Data_Files\Reporting_View_Results.csv", index=False)
