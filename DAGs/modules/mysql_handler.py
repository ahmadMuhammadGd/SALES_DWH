import mysql.connector
import logging
import re
import sqlparse

class Mysql_connector:
    def __init__(self, host:str, user:str, password:str):
        self.host = host
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None
        self.establish_connection()


    def establish_connection(self):
        connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
        )
        self.connector_obj = connection
        self.cursor = connection.cursor()
        return connection
    


    def execute_sql(self, sql_statement: str, params: tuple = None, delimiter: str = ';'):
        statements = sqlparse.split(sql_statement)
        
        for statement in statements:
                statement = sqlparse.format(
                    sql= statement,
                    strip_comments = True,
                )
                logging.info(statement)
                if params and '%s' in statement:
                    self.cursor.execute(statement, params)
                else:
                    self.cursor.execute(statement)

        self.connector_obj.commit()

   
    def close_connection(self):
        if self.connector_obj:
            self.cursor.close()
            self.connector_obj.close()