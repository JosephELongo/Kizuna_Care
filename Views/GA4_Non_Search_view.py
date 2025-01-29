import pandas as pd
import psycopg2

#Create a connection to Postgres
conn = psycopg2.connect("dbname=kizuna_care user=joyju")
cur = conn.cursor()

query = '''
create or replace view ga4_non_search_view as
    select
    to_date(coalesce(event.date, session.date), 'yyyymmdd') as date,
    coalesce(event.session_source_medium, session.session_source_medium) as session_source_medium,
    coalesce(event.session_campaign, session.session_campaign) as session_campaign,
    coalesce(event.session_manual_ad_content, session.session_manual_ad_content) as session_manual_ad_content,
    coalesce(event.session_manual_term, session.session_manual_term) as session_manual_term,
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
        session_source_medium,
        session_campaign,
        session_manual_ad_content,
        session_manual_term,
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
    from ga4_non_search_event_data) event

    full join

    (
    select
        date,
        session_source_medium,
        session_campaign,
        session_manual_ad_content,
        session_manual_term,
        sessions,
        engaged_sessions
    from ga4_non_search_session_data) session

    on
    event.date = session.date
    and
    event.session_source_medium = session.session_source_medium
    and
    event.session_campaign = session.session_campaign
    and
    event.session_manual_ad_content = session.session_manual_ad_content
    and
    event.session_manual_term = session.session_manual_term

'''

cur.execute(query)

conn.commit()
cur.close()
conn.close()