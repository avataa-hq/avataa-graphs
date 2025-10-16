# NOT FOUND
class NotFound(Exception):
    pass


class DocumentNotFound(NotFound):
    pass


class StartNodeNotFound(NotFound):
    pass


class TraceNodeNotFound(NotFound):
    pass


# VALIDATION ERROR
class ValidationError(Exception):
    pass


class ProcessAlreadyStarted(ValidationError):
    pass


class StatusError(ValidationError):
    pass


class InappropriateStatus(ValidationError):
    pass


class GraphBuildingError(ValidationError):
    pass


class TimeOutError(ValidationError):
    pass
