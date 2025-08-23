import functools
import linecache
import traceback
import sys,os,time
import shutil
import tempfile
from functools import wraps
import psutil  # to check if process still running

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

def cleanup_selenium_instances(func):
    """
    A decorator to kill all chromedriver instances after a function executes.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Execute the decorated function
            return func(*args, **kwargs)
        finally:
            # This block will run no matter what happens in the try block
            print("\n--- Cleaning up all Selenium chromedriver instances... ---")
            
            # Use the appropriate command based on the operating system
            if sys.platform.startswith("win"):
                # Windows command
                os.system("taskkill /F /IM chromedriver.exe /T")
            else:
                # macOS and Linux command
                os.system("killall chromedriver")
            print("\n--- Cleaning up all Selenium & chromedriver cache ... ---")
            # print("\n--- Avoiding. Will run in one off case. Just uncomment ---")
            # cleanup_cache()
            print("--- Cleanup complete. ---")
            
    return wrapper

def cleanup_cache():
    temp_dir = tempfile.gettempdir()

    # Remove Selenium Wire cache
    sw_cache = os.path.join(temp_dir, "seleniumwire")
    if os.path.exists(sw_cache):
        shutil.rmtree(sw_cache, ignore_errors=True)

    # Remove ChromeDriver's temp profiles (scoped_dir_*)
    for item in os.listdir(temp_dir):
        if item.startswith("scoped_dir"):
            shutil.rmtree(os.path.join(temp_dir, item), ignore_errors=True)
    
def timed_retry(max_retries: int):
    """
    A decorator that retries a function with a specific delay schedule if it fails.
    
    Args:
        max_retries (int): The maximum number of times to retry the function.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Define the specific delay schedule in seconds
            delays = [2, 3, 10, 25]
            
            # The initial attempt + the number of retries
            for attempt in range(max_retries):
                try:
                    # Attempt to run the function
                    # print(f"Attempt {attempt + 1}/{max_retries + 1}...")
                    result = func(*args, **kwargs)
                    # print("✅ Function succeeded!")
                    return result
                except Exception as e:
                    # If the function fails
                    print(f"❌ Attempt {attempt + 1} failed: {e}")
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    lineno = exc_tb.tb_lineno
                    code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()
                
                    print("Exception:", e)
                    print("Type     :", exc_type.__name__)
                    print("File     :", fname)
                    print("Line No  :", lineno)
                    print("Code     :", code_line)
                    # Check if we have more retries left
                    if attempt < max_retries:
                        # Determine the sleep time
                        if attempt < len(delays):
                            sleep_time = delays[attempt]
                        else:
                            # For the 5th failure and onwards, wait 60s
                            sleep_time = 60
                        
                        print(f"Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                    else:
                        # If all retries are exhausted, raise the last exception
                        print("All retries failed. Raising the last exception.")
                        raise e
        return wrapper
    return decorator