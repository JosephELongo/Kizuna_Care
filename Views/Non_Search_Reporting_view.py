import pandas as pd
import psycopg2

#Create a connection to Postgres
conn = psycopg2.connect("dbname=kizuna_care user=joyju")
cur = conn.cursor()

query = '''
create or replace view non_search_reporting_view as
    with
    meta as
    (
        select
            to_date(day, 'yyyy-mm-dd') as date,
            lower(campaign_name) as campaign,
            lower(ad_set_name) as ad_set,
            lower(split_part(ad_name, '_', -1)) as ad, 
            sum(impressions) as impressions,
            sum(link_clicks) as clicks,
            sum(spend) as spend,
            sum(family_form_submissions) as platform_measured_conversions
        from meta_reporting_data
        group by 1,2,3,4),

    ga4 as
    (
        select
            date,
            lower(session_campaign) as session_campaign,
            lower(session_manual_ad_content) as session_manual_ad_content,
            lower(case
                when session_source_medium ilike '%facebook%' then session_campaign || '_fbfeed_' || split_part(session_manual_term, '_', -1)
                when session_source_medium ilike '%instagram%' then session_campaign || '_igfeed_' || split_part(session_manual_term, '_', -1)
                else session_manual_term
            end) as session_manual_term,
            string_agg(session_source_medium, '---') as session_source_medium,
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
        from ga4_non_search_view
        group by 1,2,3,4)
    
    select 
        coalesce(meta.date, ga4.date) as date,
        coalesce(meta.campaign, ga4.session_campaign) as campaign,
        coalesce(meta.ad_set, ga4.session_manual_ad_content) as ad_group,
        coalesce(meta.ad, ga4.session_manual_term) as ad,
        session_source_medium,
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
    from meta

    full join ga4
    on
    meta.date = ga4.date
    and
    meta.campaign = ga4.session_campaign
    and
    meta.ad_set = ga4.session_manual_term
    and
    meta.ad = ga4.session_manual_ad_content

'''

cur.execute(query)

conn.commit()
cur.close()
conn.close()