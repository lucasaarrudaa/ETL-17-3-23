import psycopg2

class Loader:
    def __init__(self, table_name='advertisings'):
        self.table_name = table_name
        self.connector = DatabaseConnector()

    def upload_dataframe(self, dataframe):
        self.connector.connect()
        try:
            print('Starting upload...')
            # Create list of tuples from dataframe
            data = [tuple(row) for row in dataframe.to_numpy()]

            placeholders = ', '.join(['%s'] * len(data[0]))

            # Create SQL command
            sql = f"INSERT INTO {self.table_name} VALUES ({placeholders})"

            # Run SQL command with data
            self.connector.cursor.executemany(sql, data)
            self.connector.conn.commit()

            print("Upload successful!")
        except psycopg2.Error as e:
            self.connector.conn.rollback()
            print(f"Error uploading data: {e}")
        finally:
            self.connector.disconnect()

