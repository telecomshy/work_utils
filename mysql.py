import pymysql
from pymysql.cursors import DictCursor, SSDictCursor
from .pieces import select_column
from . import pieces


class TidyMysql:
    def __init__(self, *args, **kwargs):
        self.database = kwargs.get('database', None) or kwargs.get('db', None)
        if not self.database:
            raise ValueError('You must specify database.')

        cursor_class = kwargs.get('cursorclass', None)
        if cursor_class:
            if cursor_class not in [DictCursor, SSDictCursor]:
                raise ValueError('Cursor class must be DictCursor or SSDictCursor.')
        else:
            cursor_class = DictCursor
        kwargs['cursorclass'] = cursor_class

        self.database = kwargs.get('database', None) or kwargs.get('db', None)
        self.conn = pymysql.connect(*args, **kwargs)

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def select_db(self, db):
        self.database = db
        self.conn.select_db(db)

    def fetch_all(self, sql, args=None):
        with self.cursor() as cursor:
            cursor.execute(sql, args)
            return cursor.fetchall()

    def fetch_one(self, sql, args=None):
        with self.cursor() as cursor:
            cursor.execute(sql, args)
            return cursor.fetchone()

    def commit(self, sql, args=None):
        with self.cursor() as cursor:
            try:
                cursor.execute(sql, args)
            except TypeError:
                cursor.execute_many(sql, args)
            self.conn.commit()

    def get_table_header(self, table_name):
        sql = 'SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s AND ' \
              'TABLE_NAME = %s'
        rows = self.fetch_all(sql, (self.database, table_name))
        return select_column(rows, 'COLUMN_NAME')

    def query_to_file(self, sql, filename, header=None, include_header=False, file_type='csv'):
        rows = self.fetch_all(sql)
        save = getattr(pieces, f'write_to_{file_type}')
        save(filename, rows, header=header, include_header=include_header)

    def table_to_file(self, table_name, filename, include_header=False, file_type='csv'):
        sql = f'SELECT * FROM {table_name}'
        header = self.get_table_header(table_name)
        self.query_to_file(sql, filename, header=header, include_header=include_header, file_type=file_type)

    def get_table_last_modified_time(self, table_name):
        sql = 'SELECT UPDATE_TIME, CREATE_TIME FROM information_schema.TABLES WHERE TABLE_NAME = %s ' \
              'AND TABLE_SCHEMA = %s ORDER BY UPDATE_TIME DESC LIMIT 1'
        row = self.fetch_one(sql, (table_name, self.database))
        update_time, create_time = row['UPDATE_TIME'], row['CREATE_TIME']
        return update_time if update_time else create_time

    def get_table_names(self):
        sql = 'SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s'
        rows = self.fetch_all(sql, (self.database,))
        return select_column(rows, 'TABLE_NAME')

    def get_column(self, table_name, column, flatten=True):
        sql = f'SELECT {column} FROM {table_name}'
        rows = self.fetch_all(sql)
        return select_column(rows, column, flatten)

    def get_table(self, table_name):
        sql = f'SELECT * FROM {table_name}'
        return self.fetch_all(sql)

    def __getattr__(self, attr):
        return getattr(self.conn, attr)
