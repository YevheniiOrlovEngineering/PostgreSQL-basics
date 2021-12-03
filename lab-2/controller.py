from re import findall
import os

from model import ModelPostgreSQL
from view import View
from data_generation import SQLGenerator
from typing_types import *
from time import time
from pandas import DataFrame
from tabulate import tabulate
from numpy import array


def clear_screen() -> None:
    os.system("cls") if os.name == "nt" else os.system("clear")


class Validator:
    COMPREHENSIVE_MODES = dict([('SELECT', ["year range", "length range", "album size"])])

    @staticmethod
    def is_len_type(len_input: str) -> bool:
        return len(len_input[len_input.find(':'):]) == 6 and findall('[0-9]{2,}:[0-5][0-9]:[0-5][0-9]', len_input)

    @staticmethod
    def is_id_type(id_input: str) -> bool:
        return id_input.isdecimal() and id_input != '0'

    @staticmethod
    def is_comprehensive_mode(mode: str) -> bool:
        return mode in list(Validator.COMPREHENSIVE_MODES.keys())

    @staticmethod
    def is_comprehensive_option(mode: str, option: str) -> bool:
        return option in list(Validator.COMPREHENSIVE_MODES[mode])


class Controller(Validator):

    def __init__(self, is_empty: bool = False):
        self.__model = ModelPostgreSQL()
        self.__view = View()
        self.__data_generator = SQLGenerator(self.model, xml_source="./my_library.xml", is_empty=is_empty)
        self.__COMMANDS_GLOSSARY = {'1': self.insert, '2': self.select, '3': self.update, '4': self.delete,
                                    '5': self.comprehensive_select, '6': self.generate_data}

    @property
    def model(self):
        return self.__model

    @property
    def view(self):
        return self.__view

    @property
    def data_generator(self):
        return self.__data_generator

    @staticmethod
    def get_pretty_table(rows: List[Tuple[str, ...]], headers: Tuple[str, ...], return_df: bool = False) \
            -> str | Tuple[str, DataFrame]:
        rows, headers = [array(el) for el in rows], array(headers)
        df = DataFrame(rows, columns=headers)
        if not return_df:
            return tabulate(df, headers="keys", tablefmt="psql", showindex=False)
        return tabulate(df, headers="keys", tablefmt="psql", showindex=False), df

    def menu(self) -> None:
        rows = [("INSERT", '1'), ("SELECT", '2'), ("UPDATE", '3'), ("DELETE", '4'),
                ("COMPREHENSIVE", '5'), ("GENERATION", '6')]
        cols = ("Action", "Command")
        while True:
            table, df = self.get_pretty_table(rows, cols, return_df=True)
            self.view.display(table)
            command = input("Enter the action Command: ")
            if command in array(df["Command"]):
                try:
                    action = self.__COMMANDS_GLOSSARY[command]
                    clear_screen()
                    logs, exec_time = action()
                    clear_screen()
                    self.view.display(logs)
                    self.view.display(f"Execution time: {exec_time} seconds")
                    self.view.display_working_continuation("Do you want to continue? [Y/n] ")
                    answer = input()

                    if answer != 'Y':
                        break
                    clear_screen()
                except Exception as _ex:
                    clear_screen()
                    self.view.display_error(_ex)
            else:
                clear_screen()
                self.view.display_warning("Entered command is not available. Try again.\n")

    def insert(self):
        table = input(f"Enter table name from available {self.model.tables}: ")
        attr = tuple(map(str.strip, input(f"Enter attribute[s] name from available "
                                          f"{self.model.get_table_columns(self.model.conn, table)},"
                                          f"\nto insert data within the entered column[s]. "
                                          f"If you want to insert values by several attributes"
                                          f" -> separate attributes by comma \',\': ").split(', ')))

        val = tuple(map(str.strip, input(f"Enter attribute[s] name from available "
                                          f"{self.model.get_table_columns(self.model.conn, table)},"
                                          f"\nto insert data within the entered column[s]. "
                                          f"If you want to insert values by several attributes "
                                          f"-> separate values by comma \',\': ").split(', ')))

        status_msg, exec_time = self.model.create(table, attr[0], val[0]) \
            if len(attr) == 1 \
            else self.model.create(table, attr, val)
        return status_msg, exec_time

    def select(self) -> Tuple[str, float]:
        table = input(f"Enter table name from available {self.model.tables}: ")
        attr = input(f"Enter attribute[s] name from available {self.model.get_table_columns(self.model.conn, table)},"
                     f"\nif you want to select particular column[s]. If not -> press \'Enter\' otherwise: ")
        rows, cols, exec_time = self.model.read(table, attr) if attr != '' else self.model.read(table)
        return self.get_pretty_table(rows, cols), exec_time

    def comprehensive_select(self) -> Tuple[str, float]:
        mode = input(f"Choose from available modes {tuple(Validator.COMPREHENSIVE_MODES.keys())}: ")
        if not Validator.is_comprehensive_mode(mode):
            raise ValueError("Entered mode is not supported")
        option = input(f"Choose from available options {tuple(Validator.COMPREHENSIVE_MODES[mode])}: ")
        if not Validator.is_comprehensive_option(mode, option):
            raise ValueError("Entered option is not supported")
        with self.model.conn.cursor() as cursor:
            if "year range" == option:
                try:
                    min_year, max_year = input(
                        f"Enter minimal and maximum publishing years, separated by space\n\n").split()
                    min_year, max_year = int(min_year), int(max_year)
                except Exception as _ex:
                    raise ValueError("Invalid input. 2 parameters or <integer> type were not provided. Try again.")

                sql = f"""select artist.name as "Group Name",
                                 album.title as "Album Title",
                                 track.title as "Song Title",
                                 genre.name as "Genre",
                                 track.year as "Year"
                                 from artist inner join album
                                 on album.artist_id = artist.id inner join track
                                 on track.album_id = album.id inner join genre
                                 on track.genre_id = genre.id
                                 where track.year >= {min_year} 
                                 and track.year <= {max_year};
                """
            elif "length range" == option:
                while True:
                    try:
                        min_length, max_length = input(
                            f"Enter minimal and maximum song duration , in format 00:00:00, "
                            f"separated by space\n\n").split()
                        if Validator.is_len_type(min_length) and Validator.is_len_type(max_length):
                            break
                        self.view.display_warning("Entered length is not in format [0]00:00:00. Try again.")
                    except Exception as _ex:
                        raise ValueError("Is required 2 parameters. Try again.")

                sql = f"""select album.title as "Album Title",
                                 track.title as "Song Title",
                                 track.len as "Song Duration",
                                 track.number_within_album as "Number in Album",
                                 album.tracks_number as "Alsum Size"
                                 from album inner join track 
                                 on album.id = track.album_id
                                 where track.len <= \'{max_length}\'
                                 and track.len >= \'{min_length}\';
                    """

            elif "album size" == option:
                try:
                    min_size, max_size = input(
                        f"Enter minimal and maximum song number of songs in album, separated by space\n\n").split()
                    min_size, max_size = int(min_size), int(max_size)
                except Exception as _ex:
                    raise ValueError("Invalid input. 2 parameters or <integer> type were not provided. Try again.")

                sql = f"""select artist.name as "Group Name",
                                 album.title as "Album Title",
                                 album.tracks_number as "Album Size"
                                 from artist inner join album
                                 on album.artist_id = artist.id
                                 where album.tracks_number <= {max_size}
                                 and album.tracks_number >= {min_size};"""
            start = time()
            cursor.execute(sql)
            stop = time()
            logs = self.get_pretty_table(cursor.fetchall(), tuple(array(cursor.description)[:, 0]))
            return logs, stop - start

    def update(self) -> Tuple[str, float]:
        table = input(f"Enter table name from available {self.model.tables}: ")
        set_attr = tuple(map(str.strip, input(f"Enter attribute name to update\n"
                                              f"If you want to update several attributes"
                                              f" -> separate them by comma \',\': ").split(', ')))
        set_val = tuple(map(str.strip, input(f"Enter value to assign\nIf you are updating by several attributes "
                                             f"-> separate values by comma \',\': ").split(', ')))
        if "len" in set_attr:
            if not Validator.is_len_type(set_val[set_attr.index('len')]):
                raise ValueError("Invalid len type.")

        where_attr = input(f"Enter attribute name as an update filter: ")
        where_val = input(f"Enter value as an update filter: ")
        status_msg, exec_time = self.model.update(table, set_attr[0], set_val[0], where_attr, where_val) \
            if len(set_attr) == 1 \
            else self.model.update(table, set_attr, set_val, where_attr, where_val)
        return status_msg, exec_time

    def delete(self) -> Tuple[str, float]:
        table = input(f"Enter table name from available {self.model.tables}: ")
        attr = input(f"Enter attribute name from available {self.model.get_table_columns(self.model.conn, table)},"
                     f"\nif you want to delete row[s] by value in column.\nIf not -> press \'Enter\' otherwise: ")
        val = False if attr == '' else input("Enter value to delete by: ")
        status_msg, exec_time = self.model.delete(table, attr, val) if val else self.model.delete(table)
        return status_msg, exec_time

    def generate_data(self) -> Tuple[str, float]:
        table = input(f"Enter table name from available {self.model.tables}: ")
        n_rows = int(input(f"Enter rows number to generate: "))
        status_msg, exec_time = self.data_generator.generate_table(table, n_rows)
        return status_msg, exec_time

    def disconnect(self) -> None:
        self.model.disconnect()
