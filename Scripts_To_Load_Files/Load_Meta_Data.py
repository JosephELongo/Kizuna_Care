import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import config

df = pd.read_csv(config.meta)

#For Meta files, we have to remove the totals row that shows up in the first line. We also need to remove the dollar sign from cost, the double quote from impressions, and the comma from impressions. We also cast cost, imps, and clicks to floats
#Lastly, we replace NaN's with 0s
df = df[df["Day"].notna()]

#It's possible that Meta will format a file with $ in the cost column. If that's the case, remove them. The same is true for commas in the impressions column
try:
    df['Amount spent (USD)'] = df['Amount spent (USD)'].str.replace('$', '').astype(float)
except:
    pass

try:
    df['Impressions'] = df['Impressions'].str.replace('\"', '').str.replace(',', '').astype(float)
except:
    pass

df['Link clicks'] = df['Link clicks'].astype(float)
df.fillna(0, inplace=True)

#Then, rename the columns to match what we'll store in Postgres
df.columns = ['day', 'campaign_name', 'ad_set_name', 'ad_name', 'spend', 'impressions', 'link_clicks', 'family_form_submissions']

#We also need to create a connection to postgres that Pandas can use (sqlalchemy based)
db = create_engine(config.conn_string)
conn = db.connect()

#And our connection via psycopg2
conn_2 = psycopg2.connect(config.cursor_string)
cur = conn_2.cursor()

#To merge data together, we're going to take a dataframe and load it into our database temporarily
#After we merge the two tables, we will drop the table that we loaded our df into
df.to_sql('meta_reporting_data_temp', conn, if_exists = 'replace')

merge_query = '''
merge into meta_reporting_data base
using meta_reporting_data_temp new
on
base.day = new.day
and
base.campaign_name = new.campaign_name
and
base.ad_set_name = new.ad_set_name
and
base.ad_name = new.ad_name
when matched then
    update set
        day = new.day,
        campaign_name = new.campaign_name,
        ad_set_name = new.ad_set_name,
        ad_name = new.ad_name,
        spend = new.spend,
        impressions = new.impressions,
        link_clicks = new.link_clicks,
        family_form_submissions = new.family_form_submissions
when not matched then
    insert (day, campaign_name, ad_set_name, ad_name, spend, impressions, link_clicks, family_form_submissions)
    values (new.day, new.campaign_name, new.ad_set_name, new.ad_name, new.spend, new.impressions, new.link_clicks, new.family_form_submissions)
;
'''

cur.execute(merge_query)
cur.execute("drop table if exists meta_reporting_data_temp")


conn_2.commit()
cur.close()
conn_2.close()