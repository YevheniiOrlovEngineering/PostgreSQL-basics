import postgresql_backend
from typing_types import *
from numpy import array


class ModelPostgreSQL:
    def __init__(self):
        self.__connection = postgresql_backend.connect_to_db()

    @property
    def conn(self):
        return self.__connection

    @property
    def tables(self):
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute("""SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'""")
                return array(array(cursor.fetchall())[:, 0])

    def create(self, table: str, attrs: str | Tuple[str, ...], values: str | Tuple[str, ...]) -> Tuple[str, float]:
        return postgresql_backend.insert(self.conn, table, attrs, values)

    def read(self, table: str, attrs: str = None) -> Tuple[List[Tuple[str, ...]], Tuple[str, ...], float]:
        return postgresql_backend.select(self.conn, table, attrs)

    def update(self, table: str, set_attrs: str | Tuple[str, ...], set_values: str | Tuple[str, ...],
               where_attrs: str, where_values: str) -> Tuple[str, float]:
        return postgresql_backend.update(self.conn, table, set_attrs, set_values, where_attrs, where_values)

    def delete(self, table: str, attr: str, value: str) -> Tuple[str, float]:
        return postgresql_backend.delete(self.conn, table, attr, value)

    def disconnect(self):
        postgresql_backend.disconnect_from_db(self.conn)

    @staticmethod
    def get_table_columns(conn: psql_conn, table: str) -> tuple[str, ...]:
        return postgresql_backend.get_table_columns(conn, table)
