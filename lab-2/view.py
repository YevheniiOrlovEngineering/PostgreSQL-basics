class View:

    @staticmethod
    def display_error(error: Exception):
        print("[ERROR] " + str(error))

    @staticmethod
    def display(data: str):
        print(data)

    @staticmethod
    def display_warning(warning: str):
        print("[WARNING] " + warning)

    @staticmethod
    def display_working_continuation(question: str):
        print("[INFO] " + question, end='')
