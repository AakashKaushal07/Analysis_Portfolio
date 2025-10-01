def season_link_maker(worker_id,s_id) :
    import django
    django.setup()
    from base_app.models import ConfigItems
    from leagues.models import Season
    from base_app.decorators import cleanup_selenium_instances,timed_retry
    from django.db import connection, close_old_connections
    
    from seleniumwire import webdriver
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException,WebDriverException, StaleElementReferenceException
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    import os,sys,json,traceback,linecache,random,os,shutil
    from time import sleep
    import pandas as pd
    from datetime import datetime
    import gzip,json,brotli,zlib
    from io import BytesIO
    import logging,tempfile
    from contextlib import contextmanager
    
    def get_logger(name, log_dir="D:/runtime_logs"):
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f"{name}.log")
        lgr = logging.getLogger(name)
        lgr.setLevel(logging.INFO)
        
        # Avoid adding multiple handlers if logger is reused
        if not lgr.handlers:
            fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(processName)s] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            
            fh.setFormatter(formatter)
            lgr.addHandler(fh)

        return lgr

    @timed_retry(3)
    def get_driver(cache_dir) :
        os.makedirs("D:/temp_cache_chrome_driver", exist_ok=True)
        os.makedirs("D:/temp_cache_chrome_driver/profile", exist_ok=True)
        os.makedirs("D:/temp_cache_chrome_driver/cache", exist_ok=True)

        # cache_dir = os.path.join("D:/temp_cache_chrome_driver", f"profile_0_{worker_id}")
        # os.makedirs(cache_dir, exist_ok=True)
        # os.makedirs(os.path.join(cache_dir, 'cache'), exist_ok=True)

        options = webdriver.ChromeOptions()
        # CLEAN CONSOLE CHANGE 1
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_argument(f"--user-data-dir={cache_dir}")
        options.add_argument(f"--disk-cache-dir={os.path.join(cache_dir, 'cache')}")
        options.add_argument("--disk-cache-size=104857600")  # 100 MB
        driver = webdriver.Chrome(options=options)
        # logger.info("Returning Driver")
        return driver  

    @contextmanager
    def safe_driver(cache_dir) :
        driver = get_driver(cache_dir)
        try:
            yield driver  # Yield the driver to be used in the 'with' block
        finally:
            driver.quit()  # This runs automatically when the block is exited

    
    @timed_retry(3)
    def remove_cookie_dialog(driver):
        try:
            sleep(5)
            # accept_button = inner_wait.until(EC.element_to_be_clickable((By.ID, "onetrust-banner-sdk")))
            driver.execute_script("var obj = document.getElementById('onetrust-banner-sdk');if(obj){obj.remove()};")
            cookie = driver.find_element(By.CSS_SELECTOR, '#onetrust-banner-sdk')
            if cookie:
                raise Exception("Cookie banner not deleted. Retry ..")
        except NoSuchElementException as e :
            pass

    def random_wait(logger,min_s=3, max_s=14):
        wait_time = round(random.uniform(min_s, max_s), 2)
        logger.info(f"Waiting for {wait_time} seconds...")
        sleep(wait_time)

    def graceful_click_by_index(logger, driver, locator, index, max_retries=3):
        """
        Finds a list of elements, clicks one by its index, and retries on protocol error.

        :param driver: The WebDriver instance.
        :param locator: A tuple (By, "value") for the list of elements.
        :param index: The index of the element to click in the list.
        :param max_retries: The maximum number of times to retry upon failure.
        :return: True if the click was successful, False otherwise.
        """
        for attempt in range(max_retries):
            try:
                # 1. Wait for the list of elements to be present
                elements = WebDriverWait(driver, 60).until(
                    EC.presence_of_all_elements_located(locator)
                )
                # 2. Check if the index is valid
                if index >= len(elements):
                    logger.info(f"Error: Index {index} is out of bounds for list of size {len(elements)}.")
                    return False

                # 3. Get the specific element and click it

                item = elements[index]
                clicker = item.find_element(By.CSS_SELECTOR, 'td.Opta-Divider.Opta-Dash[title="View match"]')
                driver.execute_script("arguments[0].click();", clicker)
                # element_to_click = elements[index]
                # element_to_click.click()

                logger.info(f"Successfully clicked element at index {index}.")
                return True # Exit function on success

            except WebDriverException as e:
                # 4. If a protocol error occurs, reload and prepare to retry
                if "ERR_HTTP2_PROTOCOL_ERROR" in str(e):
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: Protocol error detected. Reloading page...")
                    driver.refresh()
                else:
                    # For other errors, log it and fail immediately
                    logger.info(f"An unhandled WebDriver error occurred: {e.__class__.__name__}")
                    return False

        logger.info(f"Failed to click element at index {index} after {max_retries} attempts.")
        return False

    # @cleanup_selenium_instances
    def get_event_url_games_of_season(logger, inner_driver,season):
        mw_data = []
        try:
            wait = WebDriverWait(inner_driver, 80)
            longer_wait = WebDriverWait(inner_driver,160)
            remove_cookie_dialog(inner_driver)
            fixture_list_locator = (By.CSS_SELECTOR, 'tbody.Opta-result.Opta-fixture')
            try :
                # wait.until(EC.visibility_of_element_located(fixture_list_locator))
                wait.until(EC.presence_of_element_located(fixture_list_locator))
            except TimeoutException as e:
                logger.info("Timed out to see fixture list. Reloading and waiting for longer.")
                inner_driver.refresh()
                sleep(20)
                # longer_wait.until(EC.visibility_of_element_located(fixture_list_locator))
                longer_wait.until(EC.presence_of_element_located(fixture_list_locator))

            # Get the total count of all fixtures visible on the page
            fixtures = inner_driver.find_elements(*fixture_list_locator)
            fixture_count = len(fixtures)
            logger.info(f"Found a total of {fixture_count} matches to process.")

            # Loop through each fixture by its index
            for f_index in range(fixture_count):
                logger.info(f"Working on Match {f_index+1}/{fixture_count}...")
                try:
                    if graceful_click_by_index(logger, inner_driver, fixture_list_locator, f_index) :
                        logger.info("Graceful Click Worked :)")
                        random_wait(logger,min_s=2,max_s=4)
                        temp = {}
                        ## TODO : Add a mechanisim so that in case this wait fails, reload the page and wait 80s
                        try :
                            # wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'ul.striplist')))
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.striplist')))
                        except TimeoutException as e :
                            logger.info("Previous Wait Expired to see 'ul.striplist'. Going in for a longer wait ...")
                            inner_driver.refresh()
                            sleep(20)
                            # longer_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'ul.striplist')))
                            longer_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.striplist')))

                        try :
                            # wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.Opta-Team.Opta-TeamName.Opta-Home')))
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.Opta-Team.Opta-TeamName.Opta-Home')))
                        except TimeoutException as e :
                            logger.info("Previous Wait Expired to find '.Opta-Team.Opta-TeamName.Opta-Home'. Going in for a longer wait ...")
                            inner_driver.refresh()
                            sleep(20)
                            # longer_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.Opta-Team.Opta-TeamName.Opta-Home')))
                            longer_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.Opta-Team.Opta-TeamName.Opta-Home')))

                        temp['home_team'] = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Team.Opta-TeamName.Opta-Home').text
                        temp['away_team'] = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Team.Opta-TeamName.Opta-Away').text
                        temp['home_score'] = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Score.Opta-Home').text
                        temp['away_score'] = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Score.Opta-Away').text
                        match_date = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Date').text
                        temp["datetime"] = datetime.strptime(match_date.strip(), "%d/%m/%Y %H:%M")
                        result_urls = [x.find_element(By.TAG_NAME,'a').get_property('href')
                                for x in inner_driver.find_element(By.CSS_SELECTOR,'ul.striplist').find_elements(By.TAG_NAME,'li') 
                                if x.text == 'PLAYER STATS']
                        if result_urls :
                            result_urls = result_urls[0]
                        temp['event_url'] = result_urls
                        mw_data.append(temp)
                    else :
                        logger.info("Graceful Click Failed :(")
                except Exception as e:
                    logger.info(f"Problematic URL : {inner_driver.current_url} ")
                    logger.info(f" - Error scraping details for match {f_index+1}")
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    lineno = exc_tb.tb_lineno
                    code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()
                    error_msg = f"""
                    EVENT ISSUE
                    {'-'*10}
                    Competition  => {season.competition.competition_name}
                    Season       => {season.name}/{season.name_fotmob}
                    URL          => {inner_driver.current_url}
                    Type         => {exc_type.__name__}
                    File         => {fname}
                    Line No      => {lineno}
                    Code         => {code_line}
                    {'-'*10}
                    """
                    with open(f"./Custom_Msg_{datetime.now().strftime('%d_%m_%Y')}.log","a") as f :
                        f.write(error_msg)
                    logger.error(error_msg)

                # Go back to the main fixture list
                inner_driver.execute_script("window.history.go(-1)")
                random_wait(logger, min_s=6,max_s=12)
                # IMPORTANT: Wait for the list to be present again before starting the next loop iteration
                # wait.until(EC.visibility_of_element_located(fixture_list_locator))
                wait.until(EC.presence_of_element_located(fixture_list_locator))

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.info(f"A critical error occurred: {e} in {fname} at line {exc_tb.tb_lineno}")
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()

            logger.info(f"Exception: {e}")
            logger.info(f"Type     : {exc_type.__name__}")
            logger.info(f"File     : {fname}")
            logger.info(f"Line No  : {lineno}")
            logger.info(f"Code     : {code_line}")
        finally:
            if inner_driver:
                inner_driver.quit()
        return mw_data

    # @cleanup_selenium_instances
    def get_shot_url_games_of_season(logger,event_driver,season):
        try:
            sleep(10)
            body = ""
            logger.info("Checking requests ... ")
            for req in event_driver.requests:
                if "https://www.fotmob.com/api/data/leagues" in req.url and "ccode" not in req.url :
                    # logger.info(req.url)
                    encoding = req.response.headers.get('Content-Encoding')
                    body = req.response.body
                    if encoding == 'gzip':
                        decompressed = gzip.GzipFile(fileobj=BytesIO(body)).read()
                        body = decompressed.decode('utf-8')

                    elif encoding == 'br':  # Brotli
                        decompressed = brotli.decompress(body)
                        body = decompressed.decode('utf-8')

                    elif encoding == 'deflate':
                        decompressed = zlib.decompress(body)
                        body = decompressed.decode('utf-8')
                    else:
                        try:
                            body = body.decode('utf-8')
                        except UnicodeDecodeError:
                            logger.info("UTF-8 decode failed, printing raw bytes")
                    if body != "" :
                        break
            try :
                return True,json.loads(body)
            except Exception as e :
                logger.info("Exception while comverting data to json. Returning raw body")
                return False,body
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()
            error_msg = f"""
            SHOT ISSUE
            {'-'*10}
            Competition  => {season.competition.competition_name}
            Season       => {season.name}/{season.name_fotmob}
            URL          => {event_driver.current_url}
            Type         => {exc_type.__name__}
            File         => {fname}
            Line No      => {lineno}
            Code         => {code_line}
            {'-'*10}
            """
            logger.error(error_msg)
            return False,[]

    def parse_fotmob_matches(logger, match_lists, season):
        try :
            base_url = ConfigItems.objects.get(key="FOTMOB_BASE_URL").value
            coll =[]
            for match in match_lists:
                temp = {}
                temp['home_team'] = match.get('home',{}).get('name',"")
                temp['home_score'] = match.get('status',{}).get('scoreStr',"").split("-")[0].strip(" ")

                temp['away_team'] = match.get('away',{}).get('name',"")
                temp['away_score'] = match.get('status',{}).get('scoreStr',"").split("-")[-1].strip(" ")

                temp['datetime'] = datetime.strptime(match.get('status',{}).get('utcTime',""),"%Y-%m-%dT%H:%M:%SZ")
                temp['shot_url'] = base_url+match.get('pageUrl')
                coll.append(temp)
            return coll
        except Exception as e :
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()
            error_msg = f"""
            PARSNG SHOT ISSUE
            {'-'*10}
            Competition  => {season.competition.competition_name}
            Season       => {season.name}/{season.name_fotmob}
            Type         => {exc_type.__name__}
            File         => {fname}
            Line No      => {lineno}
            Code         => {code_line}
            {'-'*10}
            """
            logger.error(error_msg)
            return []

    def fetch_data_and_save_locally(i,season_id):
        import django
        django.setup()
        from base_app.models import ConfigItems
        from leagues.models import Season
        # from base_app.decorators import cleanup_selenium_instances,timed_retry
        # from django.core.management.base import BaseCommand
        from django.db import connection, close_old_connections
        try :
            close_old_connections()
            season = Season.objects.select_related("competition").get(id=season_id)
            name = f"{str(season).replace('/','_').replace(' ','_')}"
            cache_dir = None
            
            BASE_META_DATA = r"D:/MetaData"
            os.makedirs("D:/alt_cache",exist_ok=True)
            cache_dir = tempfile.mkdtemp(prefix=f"selenium_cache_worker_{i}_", dir="D:/alt_cache")
            os.makedirs(cache_dir,exist_ok=True)
            os.environ["TEMP"] = cache_dir
            os.environ["TMP"] = cache_dir

            season_path = "NONE"
            event_path = "SA_NONE"
            shot_path = "FM_NONE"
            logger = None
            # init season vars
            s_name_sa = season.name
            s_name_fm = season.name_fotmob
            conf = season.competition.confederation
            country = season.competition.country
            name_sa = season.competition.name_scoresaway
            name_fm = season.competition.name_fotmob
            
            event_url = season.season_event_url
            shot_url = season.season_shot_url
            
            ev_driver = None
            st_driver = None
            
            print(f"Starting with {conf} - {country} - {name_fm} - {s_name_fm}")
        
            if "26" in s_name_fm or "26" in s_name_sa or "2025" == s_name_sa[:4] or "2025" == s_name_fm[:4]:
                print("Screw Current Season ... ",s_name_fm,s_name_sa)
                print()
                
                return

            ## Make Directories
            logger = get_logger(name)
            logger.info(f"{'-'*7} START OF LOG {'-'*7}")
            
            os.makedirs(BASE_META_DATA, exist_ok=True)
            os.makedirs(f"{BASE_META_DATA}/{conf}", exist_ok=True)
            os.makedirs(f"{BASE_META_DATA}/{conf}/{country}", exist_ok=True)
            season_path = f"{BASE_META_DATA}/{conf}/{country}"

            if event_url :
                if event_url.split("/")[-1] == 'fixtures':
                    event_url = event_url.replace("fixtures","results")
                ev_file = f'{name_sa.replace(" ","_").replace(".","")}_{s_name_sa.replace("/","_").replace(" - ","_").replace(" ","_").replace("-","_")}_events.xlsx'
                event_path = f"{season_path}/{ev_file}"
                if os.path.exists(event_path):
                    logger.info(f"'{ev_file}' present already. Skipping ...")
                    return

                # ev_driver = get_driver(cache_dir)
                # ev_driver.get(event_url)
                with safe_driver(cache_dir) as ev_driver :
                    ev_driver.get(event_url)
                    event_data = get_event_url_games_of_season(logger,ev_driver,season)
                    if event_data:
                        pd.DataFrame(event_data).to_excel(event_path,index=False)
                        logger.info(f"'{event_path}' is created.")
                
            if shot_url :
                st_file = f'{name_fm.replace(" ","_").replace(".","")}_{s_name_fm.replace("/","_").replace(" - ","_").replace(" ","_").replace("-","_")}_shots.xlsx'
                shot_path = f"{season_path}/{st_file}"
                if os.path.exists(shot_path):
                    logger.info(f"'{st_file}' present already. Skipping ...")
                    return

                url = shot_url
                if 'overview' in url and "fotmob" in url :
                    url = url.replace("/overview/","/matches/")
                st_driver = get_driver(cache_dir)
                with safe_driver(cache_dir) as st_driver : 
                    st_driver.get(url)

                    status,match_data = get_shot_url_games_of_season(logger,st_driver,season)
                    if status is False :

                        logger.info("*"*15)
                        logger.info("Failed Somewhere for : ")
                        logger.info(shot_path)
                        logger.info("*"*15)

                        return

                    parsed_matches = parse_fotmob_matches(logger,match_data.get('matches',{}).get('allMatches',[]),season)
                    if parsed_matches :
                        pd.DataFrame(parsed_matches).to_excel(shot_path,index=False)
                    logger.info(f"'{shot_path}' is created successfully.")

            print(f"Done with with {conf} - {country} - {name_fm} - {s_name_fm} /n")
            print()
            logger.info(f"{'-'*7} END OF LOG {'-'*7}")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()
            if logger :
                logger.info("-"*5)
                logger.info("-"*5)
                logger.info(f"Season Path : {season_path}")
                logger.info(f"Event Path  : {event_path}")
                logger.info(f"Shot Path   : {shot_path}")
                logger.info(f"Exception   : {e}")
                logger.info(f"Type        : {exc_type.__name__}")
                logger.info(f"File        : {fname}")
                logger.info(f"Line No     : {lineno}")
                logger.info(f"Code        : {code_line}")    
                logger.info("-"*5)
                logger.info("-"*5)
            
        finally:
            # Cleanup this process's cache dir
            try:
                shutil.rmtree(cache_dir)
                if logger :
                    logger.info(f"[Proc {i}] Removed cache dir")
            except Exception as cleanup_err:
                if logger :
                    logger.error(f"[Proc {i}] Cleanup failed: {cleanup_err}")

    try : 
        fetch_data_and_save_locally(worker_id,s_id)
    except Exception as e :
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()

            print("-"*5)
            print("-"*5)
            print(f"Exception   : {e}")
            print(f"Type        : {exc_type.__name__}")
            print(f"File        : {fname}")
            print(f"Line No     : {lineno}")
            print(f"Code        : {code_line}")   

def get_event_links_only(worker_id,s_id):
    import django
    django.setup()
    from base_app.models import ConfigItems
    from leagues.models import Season
    from base_app.decorators import cleanup_selenium_instances,timed_retry
    from django.db import connection, close_old_connections
    
    from seleniumwire import webdriver
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException,WebDriverException, StaleElementReferenceException
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    import os,sys,json,traceback,linecache,random,os,shutil
    from time import sleep
    import pandas as pd
    from datetime import datetime
    import gzip,json,brotli,zlib
    from io import BytesIO
    import logging,tempfile
    from contextlib import contextmanager
    
    def get_logger(name, log_dir="D:/runtime_logs"):
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f"{name}.log")
        lgr = logging.getLogger(name)
        lgr.setLevel(logging.INFO)
        
        # Avoid adding multiple handlers if logger is reused
        if not lgr.handlers:
            fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(processName)s] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            
            fh.setFormatter(formatter)
            lgr.addHandler(fh)

        return lgr

    @timed_retry(3)
    def get_driver(cache_dir) :
        os.makedirs("D:/temp_cache_chrome_driver", exist_ok=True)
        os.makedirs("D:/temp_cache_chrome_driver/profile", exist_ok=True)
        os.makedirs("D:/temp_cache_chrome_driver/cache", exist_ok=True)

        # cache_dir = os.path.join("D:/temp_cache_chrome_driver", f"profile_0_{worker_id}")
        # os.makedirs(cache_dir, exist_ok=True)
        # os.makedirs(os.path.join(cache_dir, 'cache'), exist_ok=True)

        options = webdriver.ChromeOptions()
        # CLEAN CONSOLE CHANGE 1
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_argument(f"--user-data-dir={cache_dir}")
        options.add_argument(f"--disk-cache-dir={os.path.join(cache_dir, 'cache')}")
        options.add_argument("--disk-cache-size=104857600")  # 100 MB
        driver = webdriver.Chrome(options=options)
        # logger.info("Returning Driver")
        return driver  

    @contextmanager
    def safe_driver(cache_dir) :
        driver = get_driver(cache_dir)
        try:
            yield driver  # Yield the driver to be used in the 'with' block
        finally:
            driver.quit()  # This runs automatically when the block is exited
    
    @timed_retry(3)
    def remove_cookie_dialog(driver):
        try:
            sleep(5)
            # accept_button = inner_wait.until(EC.element_to_be_clickable((By.ID, "onetrust-banner-sdk")))
            driver.execute_script("var obj = document.getElementById('onetrust-banner-sdk');if(obj){obj.remove()};")
            cookie = driver.find_element(By.CSS_SELECTOR, '#onetrust-banner-sdk')
            if cookie:
                raise Exception("Cookie banner not deleted. Retry ..")
        except NoSuchElementException as e :
            pass

    def random_wait(logger,min_s=3, max_s=14):
        wait_time = round(random.uniform(min_s, max_s), 2)
        logger.info(f"Waiting for {wait_time} seconds...")
        sleep(wait_time)

    def graceful_click_by_index(logger, driver, locator, index, max_retries=3):
        """
        Finds a list of elements, clicks one by its index, and retries on protocol error.

        :param driver: The WebDriver instance.
        :param locator: A tuple (By, "value") for the list of elements.
        :param index: The index of the element to click in the list.
        :param max_retries: The maximum number of times to retry upon failure.
        :return: True if the click was successful, False otherwise.
        """
        for attempt in range(max_retries):
            try:
                # 1. Wait for the list of elements to be present
                elements = WebDriverWait(driver, 60).until(
                    EC.presence_of_all_elements_located(locator)
                )
                # 2. Check if the index is valid
                if index >= len(elements):
                    logger.info(f"Error: Index {index} is out of bounds for list of size {len(elements)}.")
                    return False

                # 3. Get the specific element and click it

                item = elements[index]
                clicker = item.find_element(By.CSS_SELECTOR, 'td.Opta-Divider.Opta-Dash[title="View match"]')
                driver.execute_script("arguments[0].click();", clicker)
                # element_to_click = elements[index]
                # element_to_click.click()

                logger.info(f"Successfully clicked element at index {index}.")
                return True # Exit function on success

            except WebDriverException as e:
                # 4. If a protocol error occurs, reload and prepare to retry
                if "ERR_HTTP2_PROTOCOL_ERROR" in str(e):
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: Protocol error detected. Reloading page...")
                    driver.refresh()
                else:
                    # For other errors, log it and fail immediately
                    logger.info(f"An unhandled WebDriver error occurred: {e.__class__.__name__}")
                    return False

        logger.info(f"Failed to click element at index {index} after {max_retries} attempts.")
        return False

    # @cleanup_selenium_instances
    def get_event_url_games_of_season(logger, inner_driver,season,event_df):
        mw_data = []
        try:
            wait = WebDriverWait(inner_driver, 80)
            longer_wait = WebDriverWait(inner_driver,160)
            remove_cookie_dialog(inner_driver)
            fixture_list_locator = (By.CSS_SELECTOR, 'tbody.Opta-result.Opta-fixture')
            try :
                wait.until(EC.presence_of_element_located(fixture_list_locator))
            except TimeoutException as e:
                logger.info("Timed out to see fixture list. Reloading and waiting for longer.")
                inner_driver.refresh()
                sleep(20)
                try :
                    longer_wait.until(EC.presence_of_element_located(fixture_list_locator))
                except Exception as ex :
                    logger.error(f"Unable to load the Fixture list. Longer Wait timed out. Exception  : {ex}")
                    return []
            # Get the total count of all fixtures visible on the page
            fixtures = inner_driver.find_elements(*fixture_list_locator)
            fixture_count = len(fixtures)
            logger.info(f"Found a total of {fixture_count} matches to process.")
            ## ADDING CHECK IF FILES EXISTS
            event_df['event_url'] = event_df['event_url'].astype(str)
            pattern = r'^(http|https|ftp)://[^\s]+$'

            # Use df.query with @ to reference the pattern variable
            valid_links_df = event_df.query("event_url.str.match(@pattern)")
            # Use a regex to match common URL patterns
            valid_present_fixtures = valid_links_df.shape[0]
            
            if valid_present_fixtures == fixture_count :
                logger.info(f"Existing file already has the same number of fixtures with event links i.e. {fixture_count}. Hence skipping this file")
                return None
                
            # Loop through each fixture by its index
            for f_index in range(fixture_count):
                logger.info(f"Working on Match {f_index+1}/{fixture_count}...")
                try:
                    #####
                    ##### PUT HERE ITEMS TO CHECK OR IN GRACEFUL_CLICK
                    #####
                    #####
                    
                    if graceful_click_by_index(logger, inner_driver, fixture_list_locator, f_index) :
                        logger.info("Graceful Click Worked :)")
                        random_wait(logger,min_s=2,max_s=4)
                        temp = {}
                        try :
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.striplist')))
                        except TimeoutException as e :
                            logger.info("Previous Wait Expired to see 'ul.striplist'. Going in for a longer wait ...")
                            inner_driver.refresh()
                            sleep(20)
                            longer_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.striplist')))

                        try :
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.Opta-Team.Opta-TeamName.Opta-Home')))
                        except TimeoutException as e :
                            logger.info("Previous Wait Expired to find '.Opta-Team.Opta-TeamName.Opta-Home'. Going in for a longer wait ...")
                            inner_driver.refresh()
                            sleep(20)
                            longer_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.Opta-Team.Opta-TeamName.Opta-Home')))

                        temp['home_team'] = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Team.Opta-TeamName.Opta-Home').text
                        temp['away_team'] = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Team.Opta-TeamName.Opta-Away').text
                        temp['home_score'] = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Score.Opta-Home').text
                        temp['away_score'] = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Score.Opta-Away').text
                        match_date = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Date').text
                        temp["datetime"] = datetime.strptime(match_date.strip(), "%d/%m/%Y %H:%M")
                        result_urls = [x.find_element(By.TAG_NAME,'a').get_property('href')
                                for x in inner_driver.find_element(By.CSS_SELECTOR,'ul.striplist').find_elements(By.TAG_NAME,'li') 
                                if x.text == 'PLAYER STATS']
                        if result_urls :
                            result_urls = result_urls[0]
                        temp['event_url'] = result_urls
                        mw_data.append(temp)
                    else :
                        logger.info("Graceful Click Failed :(")
                except Exception as e:
                    logger.info(f"Problematic URL : {inner_driver.current_url} ")
                    logger.info(f" - Error scraping details for match {f_index+1}")
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    lineno = exc_tb.tb_lineno
                    code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()
                    error_msg = f"""
                    EVENT ISSUE
                    {'-'*10}
                    Competition  => {season.competition.competition_name}
                    Season       => {season.name}/{season.name_fotmob}
                    URL          => {inner_driver.current_url}
                    Type         => {exc_type.__name__}
                    File         => {fname}
                    Line No      => {lineno}
                    Code         => {code_line}
                    {'-'*10}
                    """
                    with open(f"./Custom_Msg_{datetime.now().strftime('%d_%m_%Y')}.log","a") as f :
                        f.write(error_msg)
                    logger.error(error_msg)

                # Go back to the main fixture list
                inner_driver.execute_script("window.history.go(-1)")
                random_wait(logger, min_s=6,max_s=12)
                # IMPORTANT: Wait for the list to be present again before starting the next loop iteration
                # wait.until(EC.visibility_of_element_located(fixture_list_locator))
                wait.until(EC.presence_of_element_located(fixture_list_locator))
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1] # type: ignore
            logger.info(f"A critical error occurred: {e} in {fname} at line {exc_tb.tb_lineno}")
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()

            logger.info(f"Exception: {e}")
            logger.info(f"Type     : {exc_type.__name__}")
            logger.info(f"File     : {fname}")
            logger.info(f"Line No  : {lineno}")
            logger.info(f"Code     : {code_line}")
        finally:
            if inner_driver:
                inner_driver.quit()
        return mw_data

    def fetch_data_and_save_locally(i,season_id):
        import django
        django.setup()
        from base_app.models import ConfigItems
        from leagues.models import Season
        # from base_app.decorators import cleanup_selenium_instances,timed_retry
        # from django.core.management.base import BaseCommand
        from django.db import connection, close_old_connections
        try :
            close_old_connections()
            season = Season.objects.select_related("competition").get(id=season_id)
            name = f"{str(season).replace('/','_').replace(' ','_')}"
            cache_dir = None
            
            BASE_META_DATA = r"D:/MetaData"
            os.makedirs("D:/alt_cache",exist_ok=True)
            cache_dir = tempfile.mkdtemp(prefix=f"selenium_cache_worker_{i}_", dir="D:/alt_cache")
            os.makedirs(cache_dir,exist_ok=True)
            os.environ["TEMP"] = cache_dir
            os.environ["TMP"] = cache_dir

            season_path = "NONE"
            event_path = "SA_NONE"
            shot_path = "FM_NONE"
            logger = None
            # init season vars
            s_name_sa = season.name
            s_name_fm = season.name_fotmob
            conf = season.competition.confederation
            country = season.competition.country
            name_sa = season.competition.name_scoresaway
            
            event_url = season.season_event_url
            
            ev_driver = None
            st_driver = None
            
            print(f"Starting with {conf} - {country} - {s_name_fm}")
        
            if "26" in s_name_fm or "26" in s_name_sa or "2025" == s_name_sa[:4] or "2025" == s_name_fm[:4]:
                print("Screw Current Season ... ",s_name_fm,s_name_sa)
                print()
                
                return

            ## Make Directories
            logger = get_logger(name)
            logger.info(f"{'-'*7} START OF LOG {'-'*7}")
            
            os.makedirs(BASE_META_DATA, exist_ok=True)
            os.makedirs(f"{BASE_META_DATA}/{conf}", exist_ok=True)
            os.makedirs(f"{BASE_META_DATA}/{conf}/{country}", exist_ok=True)
            season_path = f"{BASE_META_DATA}/{conf}/{country}"

            if event_url :
                if event_url.split("/")[-1] == 'fixtures':
                    event_url = event_url.replace("fixtures","results")
                ev_file = f'{name_sa.replace(" ","_").replace(".","")}_{s_name_sa.replace("/","_").replace(" - ","_").replace(" ","_").replace("-","_")}_events.xlsx'
                event_path = f"{season_path}/{ev_file}"
                ev_df = None
                if os.path.exists(event_path):
                    logger.info(f"'{ev_file}' present already. Skipping ...")
                    ev_df = pd.DataFrame(event_path)
                    return

                # ev_driver = get_driver(cache_dir)
                # ev_driver.get(event_url)
                with safe_driver(cache_dir) as ev_driver :
                    ev_driver.get(event_url)
                    event_data = get_event_url_games_of_season(logger,ev_driver,season,ev_df)
                    if event_data:
                        pd.DataFrame(event_data).to_excel(event_path,index=False)
                        logger.info(f"'{event_path}' is created.")
            print(f"Done with with {conf} - {country} - {s_name_fm} /n")
            print()
            logger.info(f"{'-'*7} END OF LOG {'-'*7}")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()
            if logger :
                logger.info("-"*5)
                logger.info("-"*5)
                logger.info(f"Season Path : {season_path}")
                logger.info(f"Event Path  : {event_path}")
                logger.info(f"Shot Path   : {shot_path}")
                logger.info(f"Exception   : {e}")
                logger.info(f"Type        : {exc_type.__name__}")
                logger.info(f"File        : {fname}")
                logger.info(f"Line No     : {lineno}")
                logger.info(f"Code        : {code_line}")    
                logger.info("-"*5)
                logger.info("-"*5)
            
        finally:
            # Cleanup this process's cache dir
            try:
                shutil.rmtree(cache_dir)
                if logger :
                    logger.info(f"[Proc {i}] Removed cache dir")
            except Exception as cleanup_err:
                if logger :
                    logger.error(f"[Proc {i}] Cleanup failed: {cleanup_err}")

    try : 
        fetch_data_and_save_locally(worker_id,s_id)
    except Exception as e :
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()

            print("-"*5)
            print("-"*5)
            print(f"Exception   : {e}")
            print(f"Type        : {exc_type.__name__}")
            print(f"File        : {fname}")
            print(f"Line No     : {lineno}")
            print(f"Code        : {code_line}")   

def get_shot_links_only(worker_id,s_id):
    import django
    django.setup()
    from base_app.models import ConfigItems
    from leagues.models import Season
    from base_app.decorators import cleanup_selenium_instances,timed_retry
    from django.db import connection, close_old_connections
    
    from seleniumwire import webdriver
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException,WebDriverException, StaleElementReferenceException
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    import os,sys,json,traceback,linecache,random,os,shutil
    from time import sleep
    import pandas as pd
    from datetime import datetime
    import gzip,json,brotli,zlib
    from io import BytesIO
    import logging,tempfile
    from contextlib import contextmanager
    
    def get_logger(name, log_dir="D:/runtime_logs"):
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f"{name}.log")
        lgr = logging.getLogger(name)
        lgr.setLevel(logging.INFO)
        
        # Avoid adding multiple handlers if logger is reused
        if not lgr.handlers:
            fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(processName)s] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            
            fh.setFormatter(formatter)
            lgr.addHandler(fh)

        return lgr

    @timed_retry(3)
    def get_driver(cache_dir) :
        os.makedirs("D:/temp_cache_chrome_driver", exist_ok=True)
        os.makedirs("D:/temp_cache_chrome_driver/profile", exist_ok=True)
        os.makedirs("D:/temp_cache_chrome_driver/cache", exist_ok=True)

        # cache_dir = os.path.join("D:/temp_cache_chrome_driver", f"profile_0_{worker_id}")
        # os.makedirs(cache_dir, exist_ok=True)
        # os.makedirs(os.path.join(cache_dir, 'cache'), exist_ok=True)

        options = webdriver.ChromeOptions()
        # CLEAN CONSOLE CHANGE 1
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_argument(f"--user-data-dir={cache_dir}")
        options.add_argument(f"--disk-cache-dir={os.path.join(cache_dir, 'cache')}")
        options.add_argument("--disk-cache-size=104857600")  # 100 MB
        driver = webdriver.Chrome(options=options)
        # logger.info("Returning Driver")
        return driver  

    @contextmanager
    def safe_driver(cache_dir) :
        driver = get_driver(cache_dir)
        try:
            yield driver  # Yield the driver to be used in the 'with' block
        finally:
            driver.quit()  # This runs automatically when the block is exited
    
    @timed_retry(3)
    def remove_cookie_dialog(driver):
        try:
            sleep(5)
            # accept_button = inner_wait.until(EC.element_to_be_clickable((By.ID, "onetrust-banner-sdk")))
            driver.execute_script("var obj = document.getElementById('onetrust-banner-sdk');if(obj){obj.remove()};")
            cookie = driver.find_element(By.CSS_SELECTOR, '#onetrust-banner-sdk')
            if cookie:
                raise Exception("Cookie banner not deleted. Retry ..")
        except NoSuchElementException as e :
            pass

    def random_wait(logger,min_s=3, max_s=14):
        wait_time = round(random.uniform(min_s, max_s), 2)
        logger.info(f"Waiting for {wait_time} seconds...")
        sleep(wait_time)

    def graceful_click_by_index(logger, driver, locator, index, max_retries=3):
        """
        Finds a list of elements, clicks one by its index, and retries on protocol error.

        :param driver: The WebDriver instance.
        :param locator: A tuple (By, "value") for the list of elements.
        :param index: The index of the element to click in the list.
        :param max_retries: The maximum number of times to retry upon failure.
        :return: True if the click was successful, False otherwise.
        """
        for attempt in range(max_retries):
            try:
                # 1. Wait for the list of elements to be present
                elements = WebDriverWait(driver, 60).until(
                    EC.presence_of_all_elements_located(locator)
                )
                # 2. Check if the index is valid
                if index >= len(elements):
                    logger.info(f"Error: Index {index} is out of bounds for list of size {len(elements)}.")
                    return False

                # 3. Get the specific element and click it

                item = elements[index]
                clicker = item.find_element(By.CSS_SELECTOR, 'td.Opta-Divider.Opta-Dash[title="View match"]')
                driver.execute_script("arguments[0].click();", clicker)
                # element_to_click = elements[index]
                # element_to_click.click()

                logger.info(f"Successfully clicked element at index {index}.")
                return True # Exit function on success

            except WebDriverException as e:
                # 4. If a protocol error occurs, reload and prepare to retry
                if "ERR_HTTP2_PROTOCOL_ERROR" in str(e):
                    logger.info(f"Attempt {attempt + 1}/{max_retries}: Protocol error detected. Reloading page...")
                    driver.refresh()
                else:
                    # For other errors, log it and fail immediately
                    logger.info(f"An unhandled WebDriver error occurred: {e.__class__.__name__}")
                    return False

        logger.info(f"Failed to click element at index {index} after {max_retries} attempts.")
        return False

    # @cleanup_selenium_instances
    def get_shot_url_games_of_season(logger,event_driver,season):
        try:
            sleep(10)
            body = ""
            logger.info("Checking requests ... ")
            for req in event_driver.requests:
                if "https://www.fotmob.com/api/data/leagues" in req.url and "ccode" not in req.url :
                    # logger.info(req.url)
                    encoding = req.response.headers.get('Content-Encoding')
                    body = req.response.body
                    if encoding == 'gzip':
                        decompressed = gzip.GzipFile(fileobj=BytesIO(body)).read()
                        body = decompressed.decode('utf-8')

                    elif encoding == 'br':  # Brotli
                        decompressed = brotli.decompress(body)
                        body = decompressed.decode('utf-8')

                    elif encoding == 'deflate':
                        decompressed = zlib.decompress(body)
                        body = decompressed.decode('utf-8')
                    else:
                        try:
                            body = body.decode('utf-8')
                        except UnicodeDecodeError:
                            logger.info("UTF-8 decode failed, printing raw bytes")
                    if body != "" :
                        break
            try :
                return True,json.loads(body)
            except Exception as e :
                logger.info("Exception while comverting data to json. Returning raw body")
                return False,body
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()
            error_msg = f"""
            SHOT ISSUE
            {'-'*10}
            Competition  => {season.competition.competition_name}
            Season       => {season.name}/{season.name_fotmob}
            URL          => {event_driver.current_url}
            Type         => {exc_type.__name__}
            File         => {fname}
            Line No      => {lineno}
            Code         => {code_line}
            {'-'*10}
            """
            logger.error(error_msg)
            return False,[]

    def parse_fotmob_matches(logger, match_lists, season):
        try :
            base_url = ConfigItems.objects.get(key="FOTMOB_BASE_URL").value
            coll =[]
            for match in match_lists:
                temp = {}
                temp['home_team'] = match.get('home',{}).get('name',"")
                temp['home_score'] = match.get('status',{}).get('scoreStr',"").split("-")[0].strip(" ")

                temp['away_team'] = match.get('away',{}).get('name',"")
                temp['away_score'] = match.get('status',{}).get('scoreStr',"").split("-")[-1].strip(" ")

                temp['datetime'] = datetime.strptime(match.get('status',{}).get('utcTime',""),"%Y-%m-%dT%H:%M:%SZ")
                temp['shot_url'] = base_url+match.get('pageUrl')
                coll.append(temp)
            return coll
        except Exception as e :
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()
            error_msg = f"""
            PARSNG SHOT ISSUE
            {'-'*10}
            Competition  => {season.competition.competition_name}
            Season       => {season.name}/{season.name_fotmob}
            Type         => {exc_type.__name__}
            File         => {fname}
            Line No      => {lineno}
            Code         => {code_line}
            {'-'*10}
            """
            logger.error(error_msg)
            return []

    def fetch_data_and_save_locally(i,season_id):
        import django
        django.setup()
        from base_app.models import ConfigItems
        from leagues.models import Season
        # from base_app.decorators import cleanup_selenium_instances,timed_retry
        # from django.core.management.base import BaseCommand
        from django.db import connection, close_old_connections
        try :
            close_old_connections()
            season = Season.objects.select_related("competition").get(id=season_id)
            name = f"{str(season).replace('/','_').replace(' ','_')}"
            cache_dir = None
            
            BASE_META_DATA = r"D:/MetaData"
            os.makedirs("D:/alt_cache",exist_ok=True)
            cache_dir = tempfile.mkdtemp(prefix=f"selenium_cache_worker_{i}_", dir="D:/alt_cache")
            os.makedirs(cache_dir,exist_ok=True)
            os.environ["TEMP"] = cache_dir
            os.environ["TMP"] = cache_dir

            season_path = "NONE"
            event_path = "SA_NONE"
            shot_path = "FM_NONE"
            logger = None
            # init season vars
            s_name_sa = season.name
            s_name_fm = season.name_fotmob
            conf = season.competition.confederation
            country = season.competition.country
            name_fm = season.competition.name_fotmob
            
            shot_url = season.season_shot_url
            
            st_driver = None
            
            print(f"Starting with {conf} - {country} - {name_fm} - {s_name_fm}")
        
            if "26" in s_name_fm or "26" in s_name_sa or "2025" == s_name_sa[:4] or "2025" == s_name_fm[:4]:
                print("Screw Current Season ... ",s_name_fm,s_name_sa)
                print()
                
                return

            ## Make Directories
            logger = get_logger(name)
            logger.info(f"{'-'*7} START OF LOG {'-'*7}")
            
            os.makedirs(BASE_META_DATA, exist_ok=True)
            os.makedirs(f"{BASE_META_DATA}/{conf}", exist_ok=True)
            os.makedirs(f"{BASE_META_DATA}/{conf}/{country}", exist_ok=True)
            season_path = f"{BASE_META_DATA}/{conf}/{country}"
  
            if shot_url :
                st_file = f'{name_fm.replace(" ","_").replace(".","")}_{s_name_fm.replace("/","_").replace(" - ","_").replace(" ","_").replace("-","_")}_shots.xlsx'
                shot_path = f"{season_path}/{st_file}"
                if os.path.exists(shot_path):
                    logger.info(f"'{st_file}' present already. Skipping ...")
                    return

                url = shot_url
                if 'overview' in url and "fotmob" in url :
                    url = url.replace("/overview/","/matches/")
                st_driver = get_driver(cache_dir)
                with safe_driver(cache_dir) as st_driver : 
                    st_driver.get(url)

                    status,match_data = get_shot_url_games_of_season(logger,st_driver,season)
                    if status is False :

                        logger.info("*"*15)
                        logger.info("Failed Somewhere for : ")
                        logger.info(shot_path)
                        logger.info("*"*15)

                        return

                    parsed_matches = parse_fotmob_matches(logger,match_data.get('matches',{}).get('allMatches',[]),season)
                    if parsed_matches :
                        pd.DataFrame(parsed_matches).to_excel(shot_path,index=False)
                    logger.info(f"'{shot_path}' is created successfully.")

            print(f"Done with with {conf} - {country} - {name_fm} - {s_name_fm} /n")
            print()
            logger.info(f"{'-'*7} END OF LOG {'-'*7}")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()
            if logger :
                logger.info("-"*5)
                logger.info("-"*5)
                logger.info(f"Season Path : {season_path}")
                logger.info(f"Event Path  : {event_path}")
                logger.info(f"Shot Path   : {shot_path}")
                logger.info(f"Exception   : {e}")
                logger.info(f"Type        : {exc_type.__name__}")
                logger.info(f"File        : {fname}")
                logger.info(f"Line No     : {lineno}")
                logger.info(f"Code        : {code_line}")    
                logger.info("-"*5)
                logger.info("-"*5)
            
        finally:
            # Cleanup this process's cache dir
            try:
                shutil.rmtree(cache_dir)
                if logger :
                    logger.info(f"[Proc {i}] Removed cache dir")
            except Exception as cleanup_err:
                if logger :
                    logger.error(f"[Proc {i}] Cleanup failed: {cleanup_err}")

    try : 
        fetch_data_and_save_locally(worker_id,s_id)
    except Exception as e :
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            lineno = exc_tb.tb_lineno
            code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()

            print("-"*5)
            print("-"*5)
            print(f"Exception   : {e}")
            print(f"Type        : {exc_type.__name__}")
            print(f"File        : {fname}")
            print(f"Line No     : {lineno}")
            print(f"Code        : {code_line}")   