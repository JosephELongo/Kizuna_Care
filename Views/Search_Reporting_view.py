import pandas as pd
import psycopg2

#Create a connection to Postgres
conn = psycopg2.connect("dbname=kizuna_care user=admin")
cur = conn.cursor()

query = '''
create or replace view search_reporting_view as
    with
    google_ads as
    (
        select
            to_date(day, 'yyyy-mm-dd') as date,
            campaign,
            ad_group,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(cost) as spend,
            sum(conversions) as platform_measured_conversions
        from google_ads_reporting_data
        group by 1,2,3),

    ga4 as
    (
        select
        --as there is only one google ads account linked to the ga4 property, google_ads_account_name should be the same for every row
        --however, to be safe, group by date, campaign, and ad_group to ensure 1:1 alignment with google_ads
            date,
            session_google_ads_campaign,
            session_google_ads_ad_group_name,
            sum(page_view) as page_view,
            sum(session_start) as session_start,
            sum(user_engagement) as user_engagement,
            sum(first_visit) as first_visit,
            sum(scroll) as scroll,
            sum(redirect_to_caregiver_form) as redirect_to_caregiver_form,
            sum(redirect_to_family_form) as redirect_to_family_form,
            sum(caregiver_form_submission) as caregiver_form_submission,
            sum(thank_you_page) as thank_you_page,
            sum(family_form_submission) as family_form_submission,
            sum(totals) as total_events,
            sum(sessions) as sessions,
            sum(engaged_sessions) as engaged_sessions
        from ga4_search_view
        group by 1,2,3)
    
    select 
        coalesce(google_ads.date, ga4.date) as date,
        coalesce(google_ads.campaign, ga4.session_google_ads_campaign) as campaign,
        coalesce(google_ads.ad_group, ga4.session_google_ads_ad_group_name) as ad_group,
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
    from google_ads

    full join ga4
    on
    google_ads.date = ga4.date
    and
    google_ads.campaign = ga4.session_google_ads_campaign
    and
    google_ads.ad_group = ga4.session_google_ads_ad_group_name

'''

cur.execute(query)

cur.execute('select * from search_reporting_view;')
print(cur.fetchall())

conn.commit()
cur.close()
conn.close()