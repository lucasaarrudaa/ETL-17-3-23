import pandas as pd 
import boto3

class ExtractorLocal:
    '''
    Extract archive to df.
    '''
    def __init__(self):
        pass
    def csv(self, file_path, columns = None, header = None, delimiter = None):
        '''
        Read CSV
        
        Parameters:         
            file_path: copy file path and paste here
            columns (string): is optional, set the name of te columns
            header (string): is optional
            delimiter (string): is optional
        Returns: 
            return the dataframe, when call the method, include to a variable to view.
        '''
        return pd.read_csv(rf'{file_path}', names = columns, header = header, delimiter = delimiter)
        
    def json(self, file_path, lines_opt = True):
        '''
        Read json 

        Parameters:         
            file_path (string): copy file path
            lines_opt (bool): optional
        Returns: 
            return the dataframe, when call the method, include to a variable to view.

        '''
        return pd.read_json(f'{file_path}', lines = lines_opt)
    

class ExtractorS3:

    def __init__(self, name):
        '''
        Connecting to archive in s3 bucket.

            Parameters: 
                    name (string): The name of the bucket
                    archive (string): The location of the archive
        '''
        global s3_client
        s3_client = boto3.client('s3')
        self.bucket_name = name

    def download(self, s3_path, local_path):
        '''
        Downloading the archive from bucket to local path.

            Parameters: 
                local_path (string): Your path without the last ' / '
                name (string): The name of archive
                type (string): The type of the archive (csv, json, xml...)

            Returns: 
                    the file from S3 to your destination folder.
        '''
        try:
            s3_client.download_file(
                f'{self.bucket_name}', f'{s3_path}', f'{local_path}')
        except:
            print("Was not posible to download archive from bucket")

        return s3_client.download_file(f'{self.bucket_name}', f'{s3_path}', f'{local_path}')

    def upload(self, local_file_path, bucket_path):
        '''
        Upload archive from local path to s3 bucket.

        Parameters: 
                local_file_path (string) all file path (with archive)
                bucket (string): nme of the bucket s3
                bucket_path (string): destiny path of s3
                name (string): name of new archive on s3
                type (string): csv, json, xml...
        Returns: 
                returns the file from s3 to the folder you chose
        '''
        try:
            s3_client.upload_file(f'{local_file_path}',
                                  f'{self.bucket_name}', f'{bucket_path}')
        except:
            print("Was not posible to upload archive to bucket")

            return s3_client.upload_file(f'{local_file_path}', f'{self.bucket_name}', f'{bucket_path}')

    def csv(self, file_path, columns = None, header = None, delimiter = None):
        '''
        Open the archive (CSV) downloaded from s3 with a DF
        
        Parameter:         
            optional (string): specific delimiter
            optional2 (string): header = 0 or None (default)
            file_path (string): copy file path imported from s3
            columns (string): is optional, set the name of te columns
        Returns: 
            return the dataframe, when call the method, include to a variable to view.
        '''
        return pd.read_csv(rf'{file_path}', names = columns, header = header, delimiter = delimiter)

    def json(self, file_path, lines_opt = True):
        '''
        Open the archive (JSON) downloaded from s3 with a DF

        Parameter:         
            file_path (string): copy file path imported from s3
        Returns: 
            return the dataframe, when call the method, include to a variable to view.
        '''
        return pd.read_json(f'{file_path}', lines = lines_opt)
    

        
    