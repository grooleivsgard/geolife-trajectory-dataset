from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import mysql


class Database:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name: str, attributes: list, primary_key: str, foreign: dict = None, debug=False):
        query = f"CREATE TABLE IF NOT EXISTS {table_name}("
                
        # Add attributes
        for attribute in attributes:
            if attribute == attributes[-1]:
                query += f'{attribute},\n'
            else:
                query += f'\n{attribute},'

        # Primary 
        query += f'PRIMARY KEY ({primary_key})'
        
        # If foreign
        if foreign: 
            query += f',\nFOREIGN KEY ({foreign["key"]}) REFERENCES {foreign["references"]}'

        # End statement
        query += "\n);"

        if debug:
            print(f'\nTable created: {table_name}\n')
            print(f'Query: {query}\n')

        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()

    def drop(self, tables:list, debug=False):
        query = "DROP TABLE IF EXISTS"
        for table in tables:
            if table == tables[-1]:
                query += f' {table};'
            else:
                query += f" {table}, "

        if debug:
            print(query)

        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_batch(self, table_name, batch: list):
        try:
            self.db_connection.start_transaction()
            if 'meta' in batch[0].keys():
                for row in batch:
                    del row['meta']

            df = pd.DataFrame(batch)
            if 'meta' in batch[0].keys():
                df = df.drop(columns='meta')

            data = [tuple(row) for row in df.values]
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            self.cursor.executemany(query, data)
            self.db_connection.commit()
        except mysql.connector.Error as e:
            print(f"An error occurred: {e}")
            self.db_connection.rollback()

    def close_connection(self):
        try:
            self.connection.close_connection()
        except Exception as e:
            print("ERROR: Failed to close database:", e)