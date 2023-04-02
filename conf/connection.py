from sqlalchemy import create_engine

class Connection:
    '''
    Make connection with DB.
    '''
    def sql_engine(self):
        
        engine = create_engine(
            'mysql+pymysql://{}:{}@{}:{}/{}'.format(
            'root',
            '168843',
            'localhost',
            '3306',
            'mydb')
            )
        return engine
