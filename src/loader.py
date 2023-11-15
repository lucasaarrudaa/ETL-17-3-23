import psycopg2

class Loader:
    def __init__(self, table_name='advertisings'):
        self.table_name = table_name
        self.connector = DatabaseConnector()

    def upload_dataframe(self, dataframe):
        self.connector.connect()
        try:
            print('Starting upload...')
            data = [tuple(row) for row in dataframe.to_numpy()]

            placeholders = ', '.join(['%s'] * len(data[0]))
            sql = f"INSERT INTO {self.table_name} VALUES ({placeholders})"

            self.connector.cursor.executemany(sql, data)
            self.connector.conn.commit()
            print("Upload successful!")
        except psycopg2.Error as e:
            self.connector.conn.rollback()
            print(f"Error uploading data: {e}")
        finally:
            self.connector.disconnect()

