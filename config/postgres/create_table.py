import psycopg2

def create_table():
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="15432",
            database="mydatabase",
            user="postgres",
            password="Postgres"
        )
    except:
        print("Unable to connect to database")

    cur = conn.cursor()

    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS advertisings (
        campaign_date TEXT NULL,
        campaign_name TEXT NULL,
        impressions TEXT NULL,
        clicks TEXT NULL,
        cost TEXT NULL,
        advertising TEXT NULL,
        ip TEXT NULL,
        device_id TEXT NULL,
        campaign_link TEXT NULL,
        data_click TEXT NULL,
        lead_id TEXT NULL,
        registered_at TEXT NULL,
        credit_decision TEXT NULL,
        credit_decision_at TEXT NULL,
        signed_at TEXT NULL,
        revenue TEXT NULL
    )
    """)
        
    except:
        print("Unable to create table")

    conn.commit()

    cur.close()
    conn.close()
    print("Tables created successfully!")

if __name__ == "__main__":
    create_table()
