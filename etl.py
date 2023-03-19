from connection import Connection
from extractor import ExtractorS3
from transformer import Transformer
from loader import Loader
import pandas as pd
import numpy as np

# Extracting from bucket to local
c = ExtractorS3('datalake-my-lucas-bucket')
c.download('data/customer_leads_funnel.csv',
            'datasets/customer_leads_funnel.csv')

fb = ExtractorS3('datalake-my-lucas-bucket')
fb.download('data/facebook_ads_media_costs.jsonl',
            'datasets/facebook_ads_media_costs.jsonl')

pv = ExtractorS3('datalake-my-lucas-bucket')
pv.download('data/pageview.txt',
            'datasets/pageview.txt')

ggl = ExtractorS3('datalake-my-lucas-bucket')
ggl.download('data/google_ads_media_costs.jsonl',
            'datasets/google_ads_media_costs.jsonl')

# Columns to dfs
cols2 = ['ips', 
         'device_id', 
         'refer']

cols1 = ['device_id', 
         'lead_id', 
         'registered_at', 
         'credit_decision',
        'credit_decision_at', 
        'signed_at', 
        'revenue']

# Reading donwloaded archives
df_customers = c.csv('datasets/customer_leads_funnel.csv', columns=cols1)
df_fb = fb.json('datasets/facebook_ads_media_costs.jsonl')
df_ggl = ggl.json('datasets/google_ads_media_costs.jsonl')
df_pv = pv.csv('datasets/pageview.txt', delimiter='|', columns= cols2, header=0)

# Cleaning columns
df_pv['ip'] = Transformer().extract_string(df_pv, 'ips', '\d{3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')
df_pv['device_id'] = Transformer().extract_string(df_pv, 'device_id', '\:\s.+')
df_pv['device_id'] = Transformer().extract_string(df_pv, 'device_id', '\s.+')
df_pv['click'] = Transformer().extract_string(df_pv, 'ips', 'http.+')
df_pv['referer'] = Transformer().extract_string(df_pv, 'refer', 'http.+')
df_pv['data'] = Transformer().extract_string(df_pv, 'ips', '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}')

# Deleting columns
delete_cols_pv = ['ips', 'refer']
for col in delete_cols_pv:
    df_pv = Transformer().delete_col(df_pv, col)

delete_cols_ggl = ['ad_creative_name', 'ad_creative_id', 'google_campaign_id']
for col in delete_cols_ggl:
    df_ggl = Transformer().delete_col(df_ggl, col)

df_fb = Transformer().delete_col(df_fb, 'facebook_campaign_id')

# Renaming columns
Transformer().rename(df_pv, 'data', 'data_click')
Transformer().rename(df_pv, 'click', 'campaign_link')
Transformer().rename(df_pv, 'referer', 'advertising')
Transformer().rename(df_fb, 'date', 'campaign_date')
Transformer().rename(df_fb, 'facebook_campaign_name', 'campaign_name')
Transformer().rename(df_ggl, 'date', 'campaign_date')
Transformer().rename(df_ggl, 'google_campaign_name', 'campaign_name')

# Assigning values ​​to the variables to populate the new DF
fbs = np.full(460, 'http://www.facebook.com')
ggls = np.full(5796, 'http://google.com.br')

df_fb = Transformer().fill(df_fb, 460, 'http://www.facebook.com')
df_ggl = Transformer().fill(df_ggl, 5796, 'http://google.com.br')

# Concatening DFS
advertisings = Transformer().concat(df_fb, df_ggl)
customers_data = Transformer().concat(df_pv, df_customers)
table = pd.concat([advertisings, customers_data])

print(table)

# Uploading DF to DB
table = Transformer().table_maker(table)
load = Loader().load_to_db('mydb', 'anuncios', table)