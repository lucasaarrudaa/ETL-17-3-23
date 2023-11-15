import os
from dotenv import load_dotenv
import psycopg2

class Loader:
    def __init__(self):
        load_dotenv()  
        self.host = os.getenv('DB_HOST')
        self.port = int(os.getenv('DB_PORT'))
        self.dbname = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.table_name = 'advertisings'
        self.conn = None
        self.cursor = None
        
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()
            print("Connected to database.")
        except psycopg2.Error as e:
            print(f"Error connecting to the database: {e}")
        
    def upload_dataframe(self, dataframe):
        try:
            print('Starting upload...')
            data = [tuple(row) for row in dataframe.to_numpy()]
            
            placeholders = ', '.join(['%s'] * len(data[0]))
            sql = f"INSERT INTO {self.table_name} VALUES ({placeholders})"
            
            self.cursor.executemany(sql, data)
            self.conn.commit()
            print("Upload successful!")
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Error uploading data: {e}")
        
    def disconnect(self):
        
        self.cursor.close()
        self.conn.close()
        print("Disconnected from database.")
