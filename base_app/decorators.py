import functools
import traceback
import sys

def catch_iteration_errors(func):
    """
    Decorator to catch and report errors inside a loop iteration.
    Assumes the function being decorated accepts an 'index' argument.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Try to extract 'index' from args or kwargs
        index = kwargs.get('index', None)
        if index is None and len(args) > 0:
            index = args[0]  # assumes index is the first positional argument

        try:
            return func(*args, **kwargs)
        except Exception as e:
            tb = traceback.extract_tb(sys.exc_info()[2])[-1]
            lineno = tb.lineno
            error_line = tb.line
            print(f"❌ Error in iteration {index}: {e}")
            print(f" ❌ Line number: {lineno}")
            print(f" ❌Error line: {error_line}")
            print("    Closing any open connections...")
            try:
                driver.quit()
            except Exception as e:
                pass
            print(f'{"-|"*15}-')
            
    return wrapper
