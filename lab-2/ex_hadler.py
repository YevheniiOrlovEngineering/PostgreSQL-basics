class Error(Exception):
    def __init__(self, msg: Exception):
        self.msg = msg
