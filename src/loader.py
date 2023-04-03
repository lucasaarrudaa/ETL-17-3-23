import psycopg2

class Loader:
    
    def __init__(self):
        
        self.host = 'localhost'
        self.port = 15432
        self.dbname = 'campaign'
        self.user = 'postgres'
        self.password = 'Postgres'
        self.table_name = 'advertisings'
        self.conn = None
        self.cursor = None

        
    def connect(self):
        
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        self.cursor = self.conn.cursor()
        print("Connected to database.")
        
    def upload_dataframe(self, dataframe):
        
        print('Starting upload...')
        # Cria lista de tuplas a partir do dataframe
        data = [tuple(row) for row in dataframe.to_numpy()]
        
        # Monta string de placeholders (?, ?, ?...)
        placeholders = ', '.join(['%s'] * len(data[0]))
        
        # Cria comando SQL
        sql = f"INSERT INTO {self.table_name} VALUES ({placeholders})"
        
        # Executa comando SQL com os dados
        self.cursor.executemany(sql, data)
        self.conn.commit()
        
        print("Upload successful!")
        
    def disconnect(self):
        
        self.cursor.close()
        self.conn.close()
        print("Disconnected from database.")