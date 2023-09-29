from DbConnector import DbConnector
from tabulate import tabulate


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

    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()
