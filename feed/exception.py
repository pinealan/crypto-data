class DatafeedException(Exception):
    pass

class ConnectionClosed(DatafeedException):
    pass

class BadResponse(DatafeedException):
    pass

class BadEvent(DatafeedException):
    pass
