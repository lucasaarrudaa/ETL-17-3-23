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
        campaign_date TIMESTAMP NULL,
        campaign_name VARCHAR(50) NULL,
        impressions INTEGER NULL,
        clicks INTEGER NULL,
        cost FLOAT NULL,
        advertising VARCHAR(23) NULL,
        ip VARCHAR(12) NULL,
        device_id CHAR(11) NULL,
        campaign_link VARCHAR(255) NULL,
        data_click TIMESTAMP NULL,
        lead_id CHAR(9) NULL,
        registered_at TIMESTAMP NULL,
        credit_decision CHAR(2) NULL,
        credit_decision_at TIMESTAMP NULL,
        signed_at TIMESTAMP NULL,
        revenue FLOAT NULL
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
