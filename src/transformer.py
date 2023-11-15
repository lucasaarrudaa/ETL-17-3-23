import pandas as pd
import numpy as np

class Transformer:
        
    def table_maker(self, df):
        '''
        Changing types of columns in DF.        
        '''
        dtypes = {
            'campaign_date': 'datetime64',
            'campaign_name': 'object',
            'impressions': 'int32',
            'clicks': 'int32',
            'cost': 'float64',
            'advertising': 'object',
            'ip': 'object',
            'device_id': 'object',
            'campaign_link': 'object',
            'data_click': 'datetime64',
            'lead_id': 'object',
            'registered_at': 'datetime64',
            'credit_decision': 'object',
            'credit_decision_at': 'datetime64',
            'signed_at': 'datetime64',
            'revenue': 'float16'
        }
        df = df.astype(dtype=dtypes)
        return df
        
    def extract_string(self, df, column, regex):
        '''
        Extracting a string using REGEX.
        '''
        extracted = df[column].str.extract(regex)
        return extracted
    
    def delete_col(self, df, column):
        '''
        Deleting a column from DF.
        '''
        deleted = df.drop(columns=[column], errors='ignore')
        return deleted
    
    def rename(self, df, column1, new_name):
        '''
        Renaming a column from a DF.
        '''
        df.rename(columns={column1: new_name}, inplace=True)
        return df

    def concat(self, df1, df2, ignore_index=True):
        '''
        Concat dfs.
        '''
        concatenated = pd.concat([df1, df2], ignore_index=ignore_index)
        return concatenated
        
    def join(self, df1, df2):
        '''
        Join columns and attribute to new DF.
        '''
        new_df = df1.join(df2, rsuffix='_r', lsuffix='_l')
        return new_df
    
    def drop_duplicates(self, df, col):
        '''
        Delete duplicates in a specific column of a DF.
        '''
        dt = df.drop_duplicates(subset=col)
        return dt
    
    def fill(self, df, value, name_column):
        '''
        Generate values to insert in the rows of a specific column of DF.
        '''
        new_df = df.assign(**{name_column: value})
        return new_df
