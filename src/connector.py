import os
from dotenv import load_dotenv
import psycopg2

class DatabaseConnector:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv('DB_HOST')
        self.port = int(os.getenv('DB_PORT'))
        self.dbname = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
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

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
            print("Disconnected from database.")
        except psycopg2.Error as e:
            print(f"Error disconnecting from the database: {e}")
