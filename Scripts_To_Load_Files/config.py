#Strings used to connect to postgres using psycopg2 and sqlalchemy
conn_string = r'postgresql://admin:password@127.0.0.1/kizuna_care'
cursor_string = r'dbname=kizuna_care user=admin'

#Strings pointing to file locations on your computer
meta = r'Data_Files\Meta_Data_Jan_And_Feb.csv'
google_ads = r'Data_Files\Google_Ads_Data_Jan_And_Feb.csv'
ga4_search_session = r'Data_Files\GA4_Search_Session_Data_Jan_And_Feb.csv'
ga4_search_event_data = r'Data_Files\GA4_Search_Event_Data_Jan_And_Feb.csv'
ga4_non_search_session = r'Data_Files\GA4_Non_Search_Session_Data_Jan_And_Feb.csv'
ga4_non_search_event_data = r'Data_Files\GA4_Non_Search_Event_Data_Jan_And_Feb.csv'
