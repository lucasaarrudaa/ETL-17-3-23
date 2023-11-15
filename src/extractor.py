import os
import pandas as pd
import boto3

class ExtractorLocal:
    '''
    Extract archive from local to df.
    '''

    def __init__(self):
        pass

    def csv(self, file_path, columns=None, header=None, delimiter=None):
        '''
        Read CSV

        Parameters:         
            file_path (string): The file path
            columns (list): List of column names
            header (int): Row number to use as the column names
            delimiter (string): The delimiter used in the CSV file
        Returns: 
            DataFrame: Returns the DataFrame
        '''
        csv = pd.read_csv(file_path, names=columns, header=header, delimiter=delimiter)
        return csv 

    def json(self, file_path, lines_opt=True):
        '''
        Read JSON

        Parameters:         
            file_path (string): The file path
            lines_opt (bool): If True, reads the file as a JSON lines format
        Returns: 
            DataFrame: Returns the DataFrame
        '''
        json = pd.read_json(file_path, lines=lines_opt)
        return json


class ExtractorS3:
    '''
    Extract archive from s3 to df.
    '''

    def __init__(self, name):
        self.s3_client = boto3.client('s3')
        self.bucket_name = name

    def list_files(self, path):
        '''
        List all files in a path from s3 bucket
        '''
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket_name)
        files = []
        
        for obj in bucket.objects.filter(Prefix=path):
            files.append(obj.key)
            
        return files

    def download(self, s3_path, local_path):
        '''
        Downloading the archive from bucket to local path.

        Parameters: 
            s3_path (string): The path of the file in S3
            local_path (string): The local path to save the downloaded file
        Returns: 
            bool: Returns True if the download was successful, else False
        '''
        try:
            self.s3_client.download_file(self.bucket_name, s3_path, local_path)
            return True
        except Exception as e:
            print(f"Was not possible to download archive from bucket: {e}")
            return False

    def upload(self, local_file_path, bucket_path):
        '''
        Upload archive from local path to s3 bucket.

        Parameters: 
            local_file_path (string): File path in local storage
            bucket_path (string): Destination path in S3 bucket
        Returns: 
            bool: Returns True if the upload was successful, else False
        '''
        try:
            self.s3_client.upload_file(local_file_path, self.bucket_name, bucket_path)
            return True
        except Exception as e:
            print(f"Was not possible to upload archive to bucket: {e}")
            return False

    def read_file_as_df(self, file_path, file_type='csv', **kwargs):
        '''
        Read file from S3 and return as DataFrame.

        Parameters: 
            file_path (string): The file path in S3
            file_type (string): Type of file to read ('csv' or 'json')
            **kwargs: Additional arguments to pass to pd.read_csv or pd.read_json
        Returns: 
            DataFrame: Returns the DataFrame
        '''
        try:
            local_file = f"/tmp/{os.path.basename(file_path)}"
            if self.download(file_path, local_file):
                if file_type == 'csv':
                    return pd.read_csv(local_file, **kwargs)
                elif file_type == 'json':
                    return pd.read_json(local_file, **kwargs)
                else:
                    raise ValueError("Invalid file type. Supported types are 'csv' and 'json'.")
        except Exception as e:
            print(f"Error reading file as DataFrame: {e}")
            return pd.DataFrame()
