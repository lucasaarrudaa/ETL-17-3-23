import pandas as pd 

class Extract():
    
    def __init__(self):
        '''
        Fill self.col with names of columns of the
        '''
        self.col = ['']
    
    def read_csv(self, file_path, cols, delimiter_str):
        '''
        Parameters:
                file_path (string): copy file path and paste here
                cols (list): Fill like: 'col1, col2' ...  
        
        Return: 
                return the dataframe, when call the method, include to a variable to see.
        '''
        return pd.read_csv(r'{}', delimiter=f'{delimiter_str}', names=cols, header=0).format(file_path)
    
    def read_json(self, file_path_path):
        
        return pd.read_json(r'{}', lines=True). format(file_path_path)
    