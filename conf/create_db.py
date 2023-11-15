from src.connector import DatabaseConnector
import psycopg2

def create_db():
    connector = DatabaseConnector()
    connector.connect()
    
    try:
        connector.cursor.execute("SELECT 1 FROM pg_database WHERE datname='mydatabase'")
        exists = connector.cursor.fetchone()
        
        if not exists:
            connector.cursor.execute("CREATE DATABASE mydatabase;")
            print("Database created successfully!")
        else:
            print("Database already exists.")
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")
    finally:
        connector.disconnect()

if __name__ == "__main__":
    create_db()
