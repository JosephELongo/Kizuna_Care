import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import config

#For GA4 session files, we have to skip rows 0-5 and then also skip the total row at row 7 (zero-indexed)
df = pd.read_csv(config.ga4_search_session, skiprows = [x for x in range(0,8) if x!=6])

#Then, rename the columns to match what we'll store in Postgres. Additionally, recast the date column to be a varchar rather than an int
df.columns = ['date', 'session_google_ads_account_name', 'session_google_ads_campaign', 'session_google_ads_ad_group_name', 'sessions', 'engaged_sessions']
df['date'] = df['date'].astype('str')

#We also need to create a connection to postgres that Pandas can use (sqlalchemy based)
db = create_engine(config.conn_string)
conn = db.connect()

#And our connection via psycopg2
conn_2 = psycopg2.connect(config.cursor_string)
cur = conn_2.cursor()

#To merge data together, we're going to take a dataframe and load it into our database temporarily
#After we merge the two tables, we will drop the table that we loaded our df into
df.to_sql('ga4_search_session_data_temp', conn, if_exists = 'replace')

merge_query = '''
merge into ga4_search_session_data base
using ga4_search_session_data_temp new
on
base.date = new.date
and
base.session_google_ads_account_name = new.session_google_ads_account_name
and
base.session_google_ads_campaign = new.session_google_ads_campaign
and
base.session_google_ads_ad_group_name = new.session_google_ads_ad_group_name
when matched then
    update set
        date = new.date,
        session_google_ads_account_name = new.session_google_ads_account_name,
        session_google_ads_campaign = new.session_google_ads_campaign,
        session_google_ads_ad_group_name = new.session_google_ads_ad_group_name,
        sessions = new.sessions,
        engaged_sessions = new.engaged_sessions

when not matched then
    insert (date, session_google_ads_account_name, session_google_ads_campaign, session_google_ads_ad_group_name, sessions, engaged_sessions)

    values (new.date, new.session_google_ads_account_name, new.session_google_ads_campaign, new.session_google_ads_ad_group_name, 
            new.sessions, new.engaged_sessions)
;
'''

cur.execute(merge_query)
cur.execute("drop table if exists ga4_search_session_data_temp")


conn_2.commit()
cur.close()
conn_2.close()