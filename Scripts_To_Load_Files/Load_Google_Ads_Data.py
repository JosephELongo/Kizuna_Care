import pandas as pd
import psycopg2
from sqlalchemy import create_engine


#For Google ads files, we need to skip the first two rows of the file
df = pd.read_csv(r"C:\Users\joyju\Kizuna_Care\Data_Files\Google_Ads_Data.csv", skiprows = 2)

#Then, we need to replace every instance of -- with NaNs
df['Events / session (GA4)'] = df['Events / session (GA4)'].replace(to_replace={'--': pd.NA})

#Then, rename the columns to match what we'll store in Postgres
df.columns = ['day', 'campaign', 'ad_group', 'impressions', 'clicks', 'ctr', 'currency_code', 'cost', 'conversions', 'conversion_rate', 'events_per_ga4_session']

#We also need to create a connection to postgres that Pandas can use (sqlalchemy based)
conn_string = 'postgresql://joyju:kizuna_care@127.0.0.1/kizuna_care'
db = create_engine(conn_string)
conn = db.connect()

#And our connection via psycopg2
conn_2 = psycopg2.connect("dbname=kizuna_care user=joyju")
cur = conn_2.cursor()

#To merge data together, we're going to take a dataframe and load it into our database temporarily
#After we merge the two tables, we will drop the table that we loaded our df into
df.to_sql('google_ads_reporting_data_temp', conn, if_exists = 'replace')

merge_query = '''
merge into google_ads_reporting_data base
using google_ads_reporting_data_temp new
on
base.day = new.day
and
base.campaign = new.campaign
and
base.ad_group = new.ad_group
when matched then
    update set
        day = new.day,
        campaign = new.campaign,
        ad_group = new.ad_group,
        impressions = new.impressions,
        clicks = new.clicks,
        ctr = new.ctr,
        currency_code = new.currency_code,
        cost = new.cost,
        conversions = new.conversions,
        conversion_rate = new.conversion_rate,
        events_per_ga4_session = new.events_per_ga4_session
when not matched then
    insert (day, campaign, ad_group, impressions, clicks, ctr, currency_code, cost, conversions, conversion_rate, events_per_ga4_session)
    values (new.day, new.campaign, new.ad_group, new.impressions, new.clicks, new.ctr, new.currency_code, new.cost, new.conversions, new.conversion_rate, new.events_per_ga4_session)
;
'''

cur.execute(merge_query)
cur.execute("drop table if exists google_ads_reporting_data_temp")


conn_2.commit()
cur.close()
conn_2.close()