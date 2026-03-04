import traceback
import functools
import os

def handle_exceptions(func):
    """Decorator to handle exceptions with formatted error messages."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            tb = traceback.extract_tb(e.__traceback__)
            line_no = tb[-1].lineno if tb else "unknown"
            # Get the actual file name from the traceback where exception occurred
            file_name = os.path.basename(tb[-1].filename) if tb else "unknown"
            method_name = func.__name__
            exception_msg = f"{file_name}:{method_name}: {str(e)}:{line_no}"
            print(exception_msg)
    return wrapper
