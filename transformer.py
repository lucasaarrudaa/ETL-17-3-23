import pandas as pd
import numpy as np

class Transformer:    
        
    def table_maker(self, extract_list):
        '''
        Changing types of columns in DF.        
        '''
        dt=pd.DataFrame(extract_list)
        dtypes ={'campaign_date':'object',
                 'campaign_name':'object',
                 'impressions':'float64',
                 'clicks':'float64',
                 'cost':'float64',
                 'advertising':'object',
                 'ip':'object',
                 'device_id':'object',
                 'click':'object',
                 'data':'datetime64',
                 'lead_id':'object',
                 'registered_at':'datetime64',
                 'credit_decision':'object',
                 'credit_decision_at':'string',
                 'signed_at':'datetime64',
                 'revenue':'float64'}
        dt=dt.astype(dtype=dtypes)
        return dt
        
    def extract_string(self, table, column, regex):
        '''
        Extracting a string using REGEX.
        
        Parameters:
                column: name of column 
                regex: regex code
                new_col_name: name of the new column.
                
        Returns:
                Return what do you want to extract.
        '''
        extracted = table[f'{column}'].str.extract('({})'.format(regex))
        return extracted

    def delete_col(self, df, column):
        '''
        Deleting a column from DF.
        
        Parameters: 
                df(string): dataframe
                column: column to delete
        NOTE: you need to assign to a df. 
        '''
        return df.loc[:, ~df.columns.isin([f'{column}'])]
    
    def rename(self, df, column1, new_name):
        df = df.rename(columns = {f'{column1}':f'{new_name}'}, inplace = True)
        return df
        '''
        Renaming a column from a DF.
        '''

    def concat(self, df1, df2, ignore_index=True):
        '''
        Concat dfs.
        '''
        return pd.concat([df1, df2], ignore_index = ignore_index)

        
    def join(self, new_df, df1, df2):
        '''
        Join columns and attribute to new DF.
        '''
        new_df = df1.join(df2, rsuffix='_r', lsuffix='_l')
        return new_df
    
    def fill(self, df, rows, value):
        '''
        Generate values to insert in the rows of a specific column of DF.
        
        Parameters:
                df = name of df
                rows (int): number of rows
                value (type): value what do you want to insert (str, int, float...)
                name_column: name of new column to append in df
        Returns: 
                return a new df with new values in a column
        '''
        self.rows = np.full(rows, value)
        new_df = df.assign(advertising=value) #NOTE: you need to change 'advertising' to your preferenced name column
        return new_df
