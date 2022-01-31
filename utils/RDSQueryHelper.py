import psycopg2
import os


class RDSQueryHelper:
    def __init__(self):
        credentials = self.get_credentials()
        self.connection = self.init_connection(credentials)
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.close_connection()

    # Initialises connection to the database
    @staticmethod
    def init_connection(credentials):
        connection = psycopg2.connect(**credentials)
        return connection

    @staticmethod
    def get_credentials():
        return {
            "user": os.environ["RDS_USER"],
            "password": os.environ["RDS_PASSWORD"],
            "host": os.environ["RDS_HOST"],
            "port": os.environ["RDS_PORT"],
            "database": os.environ["RDS_DATABASE"],
        }

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    # Performs SELECT * SQL query then returns a list of dictionaries representing the fetched items
    def single_table_select_all(self, table, condition, additional_params=""):
        queryString = (
            f"""SELECT * FROM {table} WHERE {condition} {additional_params};"""
        )
        self.cursor.execute(queryString)
        column_names = [desc[0] for desc in self.cursor.description]
        items = self.cursor.fetchall()
        parsed_items = []
        for item in items:
            parsed_items.append(dict(zip(column_names, list(item))))
        return parsed_items

    # Performs SELECT column_names SQL query then returns a list of dictionaries representing the fetched items
    def single_table_select(self, table, condition, column_names=None, select_all=True, additional_params=""):
        if select_all:
            column_names = ['*']
        queryString = f"""SELECT {','.join(column_names)} FROM {table} WHERE {condition} {additional_params};"""
        self.cursor.execute(queryString)
        if select_all:
            column_names = [desc[0] for desc in self.cursor.description]
        items = self.cursor.fetchall()
        parsed_items = []
        for item in items:
            parsed_items.append(dict(zip(column_names, list(item))))
        return parsed_items

    def _escape_chars(self, dict):
        """Escapes characters in strings"""
        dict = {
            k: v.replace("'", "''") if type(v) == str else v for k, v in dict.items()
        }
        return dict

    # Insert a python dictionary into the specified table
    def insert_dict(self, table, insert_dict, additional_params=""):
        insert_dict = self._escape_chars(insert_dict)
        queryString = f"""INSERT INTO {table} ({','.join(dict.keys(insert_dict))}) VALUES ({",".join(f"'{w}'" for w in dict.values(insert_dict))}) {additional_params}"""
        self.cursor.execute(queryString)
        self.connection.commit()

    def update_dict(self, table, update_dict, condition, additional_params=""):
        update_dict = self._escape_chars(update_dict)
        queryString = f"""UPDATE {table} SET {",".join(f"{k}='{v}'" for k, v in update_dict.items())} WHERE {condition} {additional_params}"""
        self.cursor.execute(queryString)
        self.connection.commit()

    def upsert_dict(
        self, table, upsert_dict, conflict_columns, condition, additional_params=""
    ):
        queryString = f"""INSERT INTO {table}  ({','.join(dict.keys(upsert_dict))}) VALUES ({",".join(f"'{w}'" for w in dict.values(upsert_dict))}) ON CONFLICT ({','.join(conflict_columns)}) DO UPDATE SET {",".join(f"{k}='{v}'" for k, v in upsert_dict.items())} WHERE {condition} {additional_params}"""
        try:
            self.cursor.execute(queryString)
            self.connection.commit()
        except:
            print("bad query")
            self.connection.rollback()

    def delete(self, table, condition):
        queryString = f"""DELETE FROM {table} WHERE {condition}"""
        self.cursor.execute(queryString)
        self.connection.commit()

    def execute(self, queryString):
        self.cursor.execute(queryString)

    def fetchall(self):
        return self.cursor.fetchall()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()
