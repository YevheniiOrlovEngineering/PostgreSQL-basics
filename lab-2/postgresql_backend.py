import psycopg2
import ex_hadler

from psycopg2 import ProgrammingError
from typing_types import *
from numpy import array
from time import time


def connect_to_db() -> psql_conn | None:
    try:
        connection = psycopg2.connect(
            database="music_test",
            user="postgres",
            password="20071944__K",
            host="127.0.0.1"
        )

        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT version();")

        return connection

    except Exception as _ex:
        raise ex_hadler.Error(_ex)


def disconnect_from_db(conn: psql_conn) -> None:
    if conn is not None:
        conn.close()


def connect(func: callable) -> callable:
    def inner_func(conn: psql_conn, *args, **kwargs) -> callable:
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version();")
        except (AttributeError, ProgrammingError) as _ex:
            conn = connect_to_db()
        return func(conn, *args, **kwargs)
    return inner_func


@connect
def insert(conn: psql_conn, table_name: str, attributes: str | Tuple[str, ...],
               values: str | Tuple[str, ...]) -> Tuple[str, float]:
    # single or multiple insert query handle
    sql = f"INSERT INTO {table_name} ({', '.join([attributes])}) VALUES (\'{values}\') " \
        if type(values) is not tuple \
        else f"INSERT INTO {table_name} ({', '.join(attributes)}) VALUES ({', '.join(['%s'] * len(values))})"
    try:
        with conn.cursor() as cursor:
            start = time()
            cursor.execute(sql, values)
            exec_time = time() - start
            status_msg = cursor.statusmessage
            conn.commit()
            return status_msg, exec_time
    except Exception as _ex:
        raise ex_hadler.Error(_ex)


@connect
def select(conn: psql_conn, table_name: str, attributes: str | Tuple[str, ...] = None) \
        -> Tuple[List[Tuple[str, ...]], Tuple[str, ...], float]:
    # selecting all table or specified rows
    base = f"SELECT * FROM {table_name}"
    if attributes is not None:
        # if attributed request -> check how many attributes are requested
        sql = f"SELECT {attributes} FROM {table_name}" \
            if type(attributes) is str \
            else f"SELECT ({', '.join(attributes)}) FROM {table_name}"
    else:
        sql = base

    try:
        with conn.cursor() as cursor:
            start = time()
            cursor.execute(sql)
            exec_time = time() - start
            conn.commit()
            return cursor.fetchall(), tuple(array(cursor.description)[:, 0]), exec_time
    except Exception as _ex:
        raise ex_hadler.Error(_ex)


# update on cascade handles parent-child relation
@connect
def update(conn: psql_conn, table_name: str,
           set_attrs:  str | Tuple[str, ...],
           set_values: str | Tuple[str, ...],
           where_attr: str, where_value: str) -> tuple[str, float] | None:

    batch = True if type(set_attrs) is tuple else False  # updating many attributes or one

    sql = f"UPDATE {table_name} SET {' = %s, '.join(set_attrs) + ' = %s'} WHERE {where_attr} = %s" \
        if batch \
        else f"UPDATE {table_name} SET {' = %s, '.join([set_attrs]) + ' = %s'} WHERE {where_attr} = %s"

    try:
        with conn.cursor() as cursor:
            start = time()
            cursor.execute(sql, (set_values + (where_value,))) if batch \
                else cursor.execute(sql, ((set_values,) + (where_value,)))
            exec_time = time() - start
            status_msg = cursor.statusmessage
            conn.commit()
            return status_msg, exec_time
    except Exception as _ex:
        raise ex_hadler.Error(_ex)


# delete on cascade handles parent-child relation
@connect
def delete(conn: psql_conn, table_name: str, attribute: str, value: str) -> tuple[str, float] | None:
    sql = f"DELETE FROM {table_name} WHERE {attribute} = %s"

    try:
        with conn.cursor() as cursor:
            start = time()
            cursor.execute(sql, (value, ))
            exec_time = time() - start
            status_msg = cursor.statusmessage
            conn.commit()
            return status_msg, exec_time
    except Exception as _ex:
        raise ex_hadler.Error(_ex)


def get_table_columns(conn: psql_conn, table: str) -> Tuple[str]:
    sql = f"SELECT * FROM {table};"
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()
            return tuple(array(cursor.description)[:, 0])
    except Exception as _ex:
        raise ex_hadler.Error(_ex)
