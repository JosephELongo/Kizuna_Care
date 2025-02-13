import pandas as pd
import psycopg2

#Create a connection to Postgres
conn = psycopg2.connect("dbname=kizuna_care user=admin")
cur = conn.cursor()

query = '''
create or replace view ga4_search_view as
    select
    to_date(coalesce(event.date, session.date), 'yyyymmdd') as date,
    coalesce(event.session_google_ads_account_name, session.session_google_ads_account_name) as session_google_ads_account_name,
    coalesce(event.session_google_ads_campaign, session.session_google_ads_campaign) as session_google_ads_campaign,
    coalesce(event.session_google_ads_ad_group_name, session.session_google_ads_ad_group_name) as session_google_ads_ad_group_name,
    page_view,
    session_start,
    user_engagement,
    first_visit,
    scroll,
    redirect_to_caregiver_form,
    redirect_to_family_form,
    caregiver_form_submission,
    thank_you_page,
    family_form_submission,
    totals,
    sessions,
    engaged_sessions
    
    from 
    (
    select
        date,
        session_google_ads_account_name,
        session_google_ads_campaign,
        session_google_ads_ad_group_name,
        page_view,
        session_start,
        user_engagement,
        first_visit,
        scroll,
        redirect_to_caregiver_form,
        redirect_to_family_form,
        caregiver_form_submission,
        thank_you_page,
        family_form_submission,
        totals
    from ga4_search_event_data) event

    full join

    (
    select
        date,
        session_google_ads_account_name,
        session_google_ads_campaign,
        session_google_ads_ad_group_name,
        sessions,
        engaged_sessions
    from ga4_search_session_data) session

    on
    event.date = session.date
    and
    event.session_google_ads_account_name = session.session_google_ads_account_name
    and
    event.session_google_ads_campaign = session.session_google_ads_campaign
    and
    event.session_google_ads_ad_group_name = session.session_google_ads_ad_group_name

'''

cur.execute(query)

conn.commit()
cur.close()
conn.close()