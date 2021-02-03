class RequestFailedError(Exception):
    """Reserved for future use."""
    msg = ""

    def __init__(self, error_text):
        Exception.__init__(self)
        self.msg = error_text

    def __str__(self):
        return self.msg


class TableManagerError(Exception):
    """Reserved for future use."""
    msg = ""

    def __init__(self, error_text):
        Exception.__init__(self)
        self.msg = error_text

    def __str__(self):
        return self.msg


class LoginFailedError(Exception):
    """Reserved for future use."""
    msg = ""

    def __init__(self, error_text):
        Exception.__init__(self)
        self.msg = error_text

    def __str__(self):
        return self.msg


class TimeFrameError(Exception):
    """Reserved for future use."""
    msg = ""

    def __init__(self, error_text):
        Exception.__init__(self)
        self.msg = error_text

    def __str__(self):
        return self.msg
