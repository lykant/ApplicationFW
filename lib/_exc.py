class NoneInputError(TypeError):
    def __init__(self):
        self.message = f"Input cannot be None!"
        super().__init__(self.message)


class NoneAttributeError(AttributeError):
    def __init__(self, name: str):
        self.message = f"Attribute {name} cannot be None!"
        super().__init__(self.message)


class InvalidInputError(ValueError):
    def __init__(self, data=None):
        self.message = f"Invalid input! ({data})" if data is not None else "Invalid input!"
        super().__init__(self.message)


class InvalidValueError(ValueError):
    def __init__(self, value, valid_values):
        if isinstance(valid_values, list):
            self.message = f"Invalid value! {value} must be in {valid_values}"
        else:
            self.message = f"Invalid value! {value} must be {valid_values}"
        super().__init__(self.message)


class IncorrectTypeError(TypeError):
    def __init__(self, data, type):
        self.message = f"{data} has incorrect type. ('{type}')."
        super().__init__(self.message)


class NoneParameterError(TypeError):
    def __init__(self):
        self.message = "Parameter cannot be None."
        super().__init__(self.message)


class NoneResultError(ValueError):
    def __init__(self):
        self.message = "Result couldn't be taken."
        super().__init__(self.message)


class NoneValueError(ValueError):
    def __init__(self, data):
        self.message = f"{data} is None."
        super().__init__(self.message)


class NotEnoughDataError(ValueError):
    def __init__(self):
        self.message = "Not enough data!"
        super().__init__(self.message)


class NoMoreDataError(ValueError):
    def __init__(self):
        self.message = "No more data!"
        super().__init__(self.message)


class InstantiationError(RuntimeError):
    def __init__(self):
        self.message = "Call instance() instead."
        super().__init__(self.message)


class MarketDateError(ValueError):
    def __init__(self, date):
        self.message = f"{date} not in market calendar!"
        super().__init__(self.message)


class InappropriateDateError(AssertionError):
    def __init__(self, start_date_no: int, end_date_no: int):
        self.message = f"Dates inappropriate! ({start_date_no} X {end_date_no})"
        super().__init__(self.message)


class NotFinishedError(InterruptedError):
    def __init__(self, process: str, error: Exception):
        self.message = f"{process} not finished properly.\n{error}"
        super().__init__(self.message)


class UserInterrupt(KeyboardInterrupt):
    def __init__(self):
        self.message = "Interrupted by user!"
        super().__init__(self.message)


class Canceled(BaseException):
    def __init__(self):
        self.message = "Canceled!"
        super().__init__(self.message)
