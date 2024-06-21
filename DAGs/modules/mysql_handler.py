import mysql.connector
import logging
import re

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
        one_line_comment_pattern = r'^--.*+/g'
        statements = sql_statement.split(delimiter)
        
        for statement in statements:
            statement = statement.strip()
            if statement:  
                # remove one line comments
                statement = re.sub(one_line_comment_pattern, '', statement, flags=re.MULTILINE)
                # add delimiter
                statement = f'{statement}{delimiter}'
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