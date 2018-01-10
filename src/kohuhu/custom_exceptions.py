
class InvalidOperationError(Exception):
    """Indicates an invalid operation has occurred for the objects's current state"""
    pass


class MockError(Exception):
    """Used for testing only"""
    pass
