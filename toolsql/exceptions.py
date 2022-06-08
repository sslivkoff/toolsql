
class InvalidOperationException(Exception):
    pass


class DoesNotExistException(Exception):
    pass


class TableNotFound(LookupError):
    pass
