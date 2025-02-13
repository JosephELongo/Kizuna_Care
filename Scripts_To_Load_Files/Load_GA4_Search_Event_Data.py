import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import config

#For GA4 event files, we have to skip rows 0-6 and then also skip the total row at row 8 (zero-indexed)
#Additionally skip row 7 to get around the column name issue described more below
df = pd.read_csv(config.ga4_search_event_data, skiprows = [x for x in range(0,9) if x!=6])

#GA4 column names are not consistent when you pivot by a column. To get around this, we load in one row earlier in the file which includes the name of each event
#However, this means the first few columns do not have a given value. To handle that, hardcode them (they will not change as they are dimensions selected in a specific order)
new_col_names = []
for i in range(len(df.columns)):
    if i==0:
        new_col_names.append('date')
    elif i==1:
        new_col_names.append('session_google_ads_account_name')
    elif i==2:
        new_col_names.append('session_google_ads_campaign')
    elif i==3:
        new_col_names.append('session_google_ads_ad_group_name')
    else:
        new_col_names.append(df.columns[i].lower())

#Rename the columns to match what we'll store in Postgres. Additionally, recast the date column to be a varchar rather than an int
df.columns = new_col_names
df['date'] = df['date'].astype('str')

#We also need to create a connection to postgres that Pandas can use (sqlalchemy based)
db = create_engine(config.conn_string)
conn = db.connect()

#And our connection via psycopg2
conn_2 = psycopg2.connect(config.cursor_string)
cur = conn_2.cursor()

#To merge data together, we're going to take a dataframe and load it into our database temporarily
#After we merge the two tables, we will drop the table that we loaded our df into
df.to_sql('ga4_search_event_data_temp', conn, if_exists = 'replace')

merge_query = '''
merge into ga4_search_event_data base
using ga4_search_event_data_temp new
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
        page_view = new.page_view,
        session_start = new.session_start,
        user_engagement = new.user_engagement,
        first_visit = new.first_visit,
        scroll = new.scroll,
        redirect_to_caregiver_form = new.redirect_to_caregiver_form,
        redirect_to_family_form = new.redirect_to_family_form,
        caregiver_form_submission = new.caregiver_form_submission,
        thank_you_page = new.thank_you_page,
        family_form_submission = new.family_form_submission,
        totals = new.totals

when not matched then
    insert (date, session_google_ads_account_name, session_google_ads_campaign, session_google_ads_ad_group_name, page_view, 
            session_start, user_engagement, first_visit, scroll, redirect_to_caregiver_form, redirect_to_family_form, caregiver_form_submission, 
            thank_you_page, family_form_submission, totals)

    values (new.date, new.session_google_ads_account_name, new.session_google_ads_campaign, new.session_google_ads_ad_group_name, 
            new.page_view, new.session_start, new.user_engagement, new.first_visit, new.scroll, new.redirect_to_caregiver_form, 
            new.redirect_to_family_form, new.caregiver_form_submission, new.thank_you_page, new.family_form_submission, new.totals)
;
'''

cur.execute(merge_query)
cur.execute("drop table if exists ga4_search_event_data_temp")


conn_2.commit()
cur.close()
conn_2.close()