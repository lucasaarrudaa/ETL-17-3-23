from extractor import ExtractorS3
from transformer import Transformer
from loader import Loader
import pandas as pd

class Etl:
            
    def __init__(self):    
        
        leads = ExtractorS3('datalake-my-lucas-bucket').download('data/customer_leads_funnel.csv', 'datasets/customer_leads_funnel.csv')
        fb = ExtractorS3('datalake-my-lucas-bucket').download('data/facebook_ads_media_costs.jsonl', 'datasets/facebook_ads_media_costs.jsonl')
        pv = ExtractorS3('datalake-my-lucas-bucket').download('data/pageview.txt','datasets/pageview.txt')
        ggl = ExtractorS3('datalake-my-lucas-bucket').download('data/google_ads_media_costs.jsonl','datasets/google_ads_media_costs.jsonl')

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

        df_leads = pd.read_csv(leads,  names=cols1, header=0)
        df_pv = pd.read_csv(pv,  delimiter="|", names=cols2, header=0)
        df_fb = pd.read_json(fb, lines=True)
        df_ggl = pd.read_json(ggl, lines=True)

        transforms = [
            ('ip', 'ips', '\d{3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'),
            ('device_id', 'device_id', '\:\s.+'),
            ('device_id', 'device_id', '\s.+'),
            ('click', 'ips', 'http.+'),
            ('referer', 'refer', 'http.+'),
            ('data', 'ips', '\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2}\:\d{2}')]

        for new_col, old_col, regex in transforms:
            df_pv[new_col] = Transformer().extract_string(df_pv, old_col, regex)

        # Deleting columns
        delete_cols_pv = ['ips', 'refer']
        for col in delete_cols_pv:
            df_pv = Transformer().delete_col(df_pv, col)

        delete_cols_ggl = ['ad_creative_name', 'ad_creative_id', 'google_campaign_id']
        for col in delete_cols_ggl:
            df_ggl = Transformer().delete_col(df_ggl, col)

        df_fb = Transformer().delete_col(df_fb, 'facebook_campaign_id')

        renames = [
            (df_pv, 'data', 'data_click'),
            (df_pv, 'click', 'campaign_link'),
            (df_pv, 'referer', 'advertising'),
            (df_fb, 'date', 'campaign_date'),
            (df_fb, 'facebook_campaign_name', 'campaign_name'),
            (df_ggl, 'date', 'campaign_date'),
            (df_ggl, 'google_campaign_name', 'campaign_name')]

        for df, old_name, new_name in renames:
            Transformer().rename(df, old_name, new_name)

        df_fb = Transformer().fill(df_fb, 460, 'http://www.facebook.com')
        df_ggl = Transformer().fill(df_ggl, 5796, 'http://google.com.br')

        advertisings = Transformer().concat(df_fb, df_ggl)
        leads_data = Transformer().concat(df_pv, df_leads)
        table = pd.concat([advertisings, leads_data])
        table.fillna(0, inplace=True)

        table = Transformer().table_maker(table)

        load = Loader()
        load.connect()
        load.upload_dataframe(table)
        load.disconnect()
