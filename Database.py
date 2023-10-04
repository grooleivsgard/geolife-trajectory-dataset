from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import mysql.connector


class Database:
    def __init__(self):
        """
        Initializes the Database object by establishing a database connection and creating a cursor.
        """
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name: str, attributes: list, primary_key: str, foreign: dict = None, debug=False):
        """
        Creates a table in the database.

        :param table_name: The name of the table to be created.
        :param attributes: A list of attributes for the table.
        :param primary_key: The primary key for the table.
        :param foreign: A dictionary containing foreign key details.
        :param debug: A flag to print debug information.
        """
        query = f"CREATE TABLE IF NOT EXISTS {table_name}("

        for attribute in attributes:
            if attribute == attributes[-1]:
                query += f'{attribute},\n'
            else:
                query += f'\n{attribute},'

        query += f'\nPRIMARY KEY ({primary_key})'

        if foreign:
            query += f',\nFOREIGN KEY ({foreign["key"]}) REFERENCES {foreign["references"]}'

        query += "\n);"

        if debug:
            print(f'\nTable created: {table_name}\n')
            print(f'Query: {query}\n')

        # Execute the query
        self.cursor.execute(query)
        self.db_connection.commit()

    def drop(self, tables: list, debug=False):
        """
        Drops the specified tables from the database.

        :param tables: A list of table names to be dropped.
        :param debug: A flag to print debug information.
        """
        query = "DROP TABLE IF EXISTS "

        for table in tables:
            query += f" {table}," if table != tables[-1] else f" {table};"

        if debug:
            print(query)

        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_batch(self, table_name: str, batch: list):
        """
        Inserts a batch of rows into the specified table.

        :param table_name: The name of the table to insert data into.
        :param batch: A list of dictionaries, each representing a row to be inserted.
        """
        try:
            self.db_connection.start_transaction()
            if 'meta' in batch[0].keys():
                for row in batch:
                    del row['meta']

            df = pd.DataFrame(batch)
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
        """
        Closes the database connection.
        """
        try:
            self.connection.close_connection()
        except Exception as e:
            print("ERROR: Failed to close database:", e)
