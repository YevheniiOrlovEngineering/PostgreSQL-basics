import typing

from tabulate import tabulate
import os


class View:
    def __init__(self, table, records):
        self.table = table
        self.records = records

    @staticmethod
    def cls():
        os.system('cls' if os.name == 'nt' else 'clear')

    @staticmethod
    def display_table_stdout(rows, headers):
        print(tabulate(rows, headers=headers, tablefmt="psql", showindex=False))

    @staticmethod
    def display_attr_mistype_stdout(col: str, req_type: type, entered_val: str):
        print(f"[ERROR] Entered value: ({entered_val}) of attribute ({col}) "
              f"does not not match required type ({req_type})")

    @staticmethod
    def print_stdout(msg: str) -> None:
        print(msg)

    @staticmethod
    def get_stdin(msg: str) -> typing.Any:
        return input(msg)
