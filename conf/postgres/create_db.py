import psycopg2

def create_db():
    # Connects to the Docker database
    conn = psycopg2.connect(
        host="localhost",
        port="15432",
        database="postgres",
        user="postgres",
        password="Postgres"
    )
    
    # Sets autocommit to True
    conn.autocommit = True
    
    # Checks if the database already exists
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname='mydatabase'")
    exists = cur.fetchone()
    
    # If the database doesn't exist, creates it
    if not exists:
        cur.execute("CREATE DATABASE mydatabase;")
        print("Database created successfully!")
    else:
        print("Database already exists.")
    
    # Closes the connection
    cur.close()
    conn.close()

if __name__ == "__main__":
    create_db()
