from contextlib import contextmanager
from modules.mysql_handler import Mysql_connector
from scripts.global_var import _SQL_TABLES_INIT, _SQL_STAGING, _SQL_TRANSFORM_LOAD, _SQL_UPDATE_VIEW
import json

@contextmanager
def mysql_connection(host: str, user: str, password: str):
    connector = Mysql_connector(host=host, user=user, password=password)
    try:
        yield connector
    finally:
        connector.close_connection()


def dwh_init_tables(host: str, user: str, password: str):
    with mysql_connection(host, user, password) as handler:
        handler.execute_sql(_SQL_TABLES_INIT)
        
        
def dwh_stage(cleaned_file_path: tuple, host: str, user: str, password: str):
    with mysql_connection(host, user, password) as handler:
        handler.execute_sql(_SQL_STAGING, cleaned_file_path)


def dwh_transform_load(params: dict, host: str, user: str, password: str):
    with mysql_connection(host, user, password) as handler:
        handler.execute_sql(_SQL_TRANSFORM_LOAD, params)


def dwh_update_view(host: str, user: str, password: str):
    with mysql_connection(host, user, password) as handler:
        handler.execute_sql(_SQL_UPDATE_VIEW)
        
if '__name__' == 'main':
    pass