import pandas as pd
import numpy as np

class Transformer:    
        
    def table_maker(self, extract_list):
        
        dt=pd.DataFrame(extract_list)
        dtypes ={'campaign_date':'string',
                 'campaign_name':'string',
                 'impressions':'string',
                 'clicks':'string',
                 'cost':'string',
                 'advertising':'string',
                 'ip':'string',
                 'device_id':'string',
                 'click':'string',
                 'data':'string',
                 'lead_id':'string',
                 'registered_at':'string',
                 'credit_decision':'string',
                 'credit_decision_at':'string',
                 'signed_at':'string',
                 'revenue':'string'}
        dt=dt.astype(dtype=dtypes)
        return dt
        
    def extract_string(self, table, column, regex, new_col_name):
        '''
        Extract a string using REGEX.
        
        Parameters:
                column: name of column 
                regex: regex code
                new_col_name: name of the new column.
                
        Returns:
                Return what do you want to extract.
        '''
        self.extracted = table[f'{column}'].str.extract('({})'.format(regex))
        table[new_col_name] = self.extracted
        return table[new_col_name]
    
    def delete(self, df, column):
        
        self.delete = df.loc[:, ~df.columns.isin([f'{column}'])]
    
    def rename(self, df, column1, column2):
        df = df.rename(columns = {f'{column1}':f'{column2}'}, inplace = True)
        return df

    def concat(self, new_df, column1, column2):
        '''
        concatening dfs
        '''
        new_df = pd.concat([column1, column2], ignore_index=True)
        return new_df
        
    def join(self, new_df, column1, column2):
        '''
        join columns and attribute to new df.
        '''
        new_df = column1.join(column2, rsuffix='_r', lsuffix='_l')
        return new_df
    
    def fill_numpy(self, df, rows, value, new_df):
        '''
        generate values to insert in the rows of a specific column of DF
        
        Parameters:
                df = name of df
                rows (int): number of rows
                value (type): value what do you want to insert
        Returns: 
                return a new df with new values in a column
        '''
        self.rows = np.full(rows, value)
        new_df = df.assign(advertising=value)
        return new_df
