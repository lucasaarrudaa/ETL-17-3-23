import psycopg2

def create_table():
    connector = DatabaseConnector()
    connector.connect()
    
    try:
        self.cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s", (self.dbname,))
        exists = self.cursor.fetchone()

        if exists:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS your_table_name (
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
            self.conn.commit()
            print("Table created successfully!")
        else:
            print("Database does not exist.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
    finally:
        connector.disconnect()
        
if __name__ == "__main__":
    connector.create_table()
