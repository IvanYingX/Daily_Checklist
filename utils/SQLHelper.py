import os
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class SQLHelper:
    DATABASE_TYPE = 'postgresql'
    DBAPI = 'psycopg2'
    def __init__(self, credentials:dict, database_type:str='postgresql', db_api:str='psycopg2'):
        self.engine = create_engine(f"{database_type}+{db_api}://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}")
        self.engine.connect()
    

    def inspect_tables(self):
        inspector = inspect(self.engine)
        print(inspector.get_table_names())

    def execute(self, queryString:str):
        self.engine.execute(queryString)
    
    def fetch_all_from_table(self, table:str) -> list:
        return self.engine.execute(f'SELECT * FROM {table}').fetchall()

    def upsert_dict(self, table, upsert_dict, conflict_columns, condition, additional_params=""):
        queryString = f"""INSERT INTO {table}  ({','.join(dict.keys(upsert_dict))}) VALUES ({",".join(f"'{w}'" for w in dict.values(upsert_dict))}) ON CONFLICT ({','.join(conflict_columns)}) DO UPDATE SET {",".join(f"{k}='{v}'" for k, v in upsert_dict.items())} WHERE {condition} {additional_params}"""
        try:
            self.execute(queryString)
        except:
            print("bad query")
            self.connection.rollback()


    def df_from_table(self, table_name:str) -> pd.DataFrame:
        return pd.read_sql_table(table_name, self.engine)

    def df_from_query(self, queryString:str) -> pd.DataFrame:
        return pd.read_sql_query(queryString, self.engine)

    def write_table(self, df:pd.DataFrame, table_name:str) -> None:
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

    def insert_df(self, df:pd.DataFrame, table_name:str) -> None:
        df.to_sql(table_name, self.engine, if_exists='append', index=False)

    def _delete_table(self, table_name:str) -> None:
        self.engine.execute(f"DROP TABLE IF EXISTS {table_name}")
