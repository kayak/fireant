class SlicerException(Exception):
    pass


class QueryException(SlicerException):
    pass


class MissingTableJoinException(SlicerException):
    pass


class CircularJoinsException(SlicerException):
    pass


class RollupException(SlicerException):
    pass


class MetricRequiredException(SlicerException):
    pass


class ContinuousDimensionRequiredException(SlicerException):
    pass
