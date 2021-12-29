import collections
import typing

from view import View
from constants import MENU_ROWS, MENU_COLUMNS
import model


class Validator:

    @staticmethod
    def validate_table_num_range(num_of_tables: int, choice: int) -> bool:
        return 0 <= choice <= num_of_tables - 1

    @staticmethod
    def validate_table_num_type(choice: str) -> bool:
        return choice.isdigit()

    @staticmethod
    def validate_input_items(req_types: typing.Dict[str, type], column: str, attr_val: str) -> bool:
        try:
            req_types[column](attr_val)
            return False
        except ValueError:
            View.display_attr_mistype_stdout(column, req_types[column], attr_val)
            return True


class Controller:
    __TABLES = {num: table_name for num, table_name in enumerate(model.get_table_names())}

    @staticmethod
    def table_num_input(num_of_tables: int) -> int:
        while True:
            View.display_table_stdout(
                rows=[
                    [command, action] for command, action in enumerate(
                        Controller.__TABLES.values())],
                headers=MENU_COLUMNS
            )
            num = View.get_stdin('Choose table number: ')
            if Validator.validate_table_num_type(num):
                table_num = int(num)
                if Validator.validate_table_num_range(num_of_tables, table_num):
                    return table_num
                else:
                    View.cls()
                    View.print_stdout('Incorrect input, try again.')
            else:
                View.cls()
                View.print_stdout('Incorrect input, try again.')

    @staticmethod
    def update_col_val_handle(table_name: str, table_columns: typing.List) -> typing.Tuple[str, ...]:
        req_types = collections.OrderedDict({column: col_type
                                             for column, col_type in model.get_table_attr_types(table_name)})
        tmp = dict()
        entered_all_columns = False

        id = View.get_stdin(f"Enter {table_columns[0]} to start updating: ")
        while Validator.validate_input_items(req_types, table_columns[0], id):
            id = View.get_stdin(f"Enter {table_columns[0]} to  start updating: ")

        tmp[table_columns[0]] = id
        while not entered_all_columns:
            upd_col = View.get_stdin(f"Choose updating column from {table_columns}: ")
            while upd_col not in table_columns:
                View.cls()
                View.print_stdout(f"column {upd_col} is not provided. Try again")
                upd_col = View.get_stdin(f"Choose updating column from {table_columns}: ")

            upd_val = View.get_stdin(f"Enter {upd_col} value: ")
            while Validator.validate_input_items(req_types, upd_col, upd_val):
                upd_val = View.get_stdin(f"Enter {upd_col} value: ")

            tmp[upd_col] = upd_val
            user_command = View.get_stdin(f"Press \'Y\' to enter more columns to update. "
                                          f"Press \'n\' otherwise.")
            if user_command != 'Y':
                entered_all_columns = True
            View.cls()
        entered_values = tuple(tmp[col] if col in tmp.keys() else None for col in table_columns)
        return entered_values

    @staticmethod
    def column_value_input(table_name: str, table_columns: typing.List,
                           only_id: bool = False) -> typing.OrderedDict | typing.Dict:

        req_types = collections.OrderedDict({column: col_type
                                             for column, col_type in model.get_table_attr_types(table_name)})
        entered_values = collections.OrderedDict()
        for column in table_columns:
            not_validated = True
            attr_val = ""
            while not_validated:
                attr_val = View.get_stdin(f"Enter {column} value: ")
                not_validated = Validator.validate_input_items(req_types, column, attr_val)
                View.cls()
            else:
                entered_values[column] = attr_val
            if only_id:
                return {column: int(attr_val)}
        return entered_values

    @staticmethod
    def is_continue(mode: str, end_mode: bool) -> bool:
        incorrect = True
        while incorrect:
            answer = View.get_stdin(f'Continue working with {mode}? Enter Yes or No ')
            if answer == 'No':
                end_mode = True
                incorrect = False
            elif answer == 'Yes':
                incorrect = False
                pass
            else:
                View.print_stdout('Please, enter Yes or No')

        return end_mode

    @staticmethod
    def menu():
        while True:

            View.display_table_stdout(rows=MENU_ROWS, headers=MENU_COLUMNS)
            choice = View.get_stdin("Choose an option: ")

            if choice == '0':
                View.cls()
                table_num = Controller.table_num_input(len(Controller.__TABLES.values()))

                View.display_table_stdout(model.get_rows(Controller.__TABLES[table_num]),
                                          model.get_table_columns(Controller.__TABLES[table_num]))

            elif choice == '1':
                View.cls()
                for table_num in Controller.__TABLES.keys():
                    View.display_table_stdout(model.get_rows(Controller.__TABLES[table_num]),
                                              model.get_table_columns(Controller.__TABLES[table_num]))
            elif choice == '2':
                View.cls()

                end_insert = False
                while not end_insert:
                    View.display_table_stdout(rows=MENU_ROWS, headers=MENU_COLUMNS)
                    table_num = Controller.table_num_input(len(Controller.__TABLES.values()))

                    entered_values = Controller.column_value_input(
                        table_name=Controller.__TABLES[table_num],
                        table_columns=model.get_table_columns(Controller.__TABLES[table_num]))

                    inserted_id = model.insert(table_num, entered_values)
                    View.display_table_stdout([[Controller.__TABLES[table_num], inserted_id, "inserted"]],
                                              headers=["table", "id", "action"])

                    end_insert = Controller.is_continue("insert", end_insert)

            elif choice == '3':
                end_delete = False
                while not end_delete:
                    View.display_table_stdout(rows=MENU_ROWS, headers=MENU_COLUMNS)
                    table_num = Controller.table_num_input(len(Controller.__TABLES.values()))

                    entered_values = Controller.column_value_input(
                        table_name=Controller.__TABLES[table_num],
                        table_columns=model.get_table_columns(Controller.__TABLES[table_num]),
                        only_id=True)

                    deletion_logs = model.delete(table_num, entered_values)

                    View.display_table_stdout(deletion_logs, headers=["table", "col. value", "column", "action"])

                    end_delete = Controller.is_continue("delete", end_delete)

            elif choice == '4':
                end_update = False
                while not end_update:
                    View.display_table_stdout(rows=MENU_ROWS, headers=MENU_COLUMNS)
                    table_num = Controller.table_num_input(len(Controller.__TABLES.values()))

                    entered_values = Controller.update_col_val_handle(
                        table_name=Controller.__TABLES[table_num],
                        table_columns=model.get_table_columns(Controller.__TABLES[table_num]))
# )
                    updating_logs = model.update(table_num, entered_values)
                    View.display_table_stdout(updating_logs, headers=["table", "id", "action"])

                    end_update = Controller.is_continue("update", end_update)
            elif choice == '5':
                break
            else:
                View.print_stdout('Please, enter valid number')

