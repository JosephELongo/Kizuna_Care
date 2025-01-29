import pandas as pd
import psycopg2

#Create a connection to Postgres
conn = psycopg2.connect("dbname=kizuna_care user=joyju")
cur = conn.cursor()

query = '''
create or replace view reporting_view as

    select
        date,
        'Search' as channel,
        campaign,
        ad_group,
        'N/A' as ad,
        impressions,
        clicks,
        spend,
        platform_measured_conversions,
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
        total_events,
        sessions,
        engaged_sessions
    from search_reporting_view

    union

    select
        date,
        case
            when session_source_medium ilike '%facebook%' or session_source_medium ilike '%instagram%' then 'Social'
            else session_source_medium
        end as channel,
        campaign,
        ad_group,
        ad,
        impressions,
        clicks,
        spend,
        platform_measured_conversions,
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
        total_events,
        sessions,
        engaged_sessions
    from non_search_reporting_view
'''

cur.execute(query)

conn.commit()
cur.close()
conn.close()