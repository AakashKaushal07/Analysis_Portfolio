from datetime import datetime
from base_app.models import ConfigItems,OptaEvents,OptaQualifier
from leagues.models import Season
from games.models import Game
from base_app.decorators import timed_retry
from base_app.helpers import log_exception,get_logger
from time import sleep
from django.db.models import Count, Q

from seleniumwire import webdriver
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException,WebDriverException, StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC

import numpy as np
import pandas as pd
from datetime import datetime
import gzip,json,brotli,zlib,os
from io import BytesIO
import logging,tempfile
from contextlib import contextmanager
from unidecode import unidecode


class MatchEventFetcher:
    def __init__(self,cache_dir):
        configs = {item.key : item.value for item in ConfigItems.objects.all()}
        self.temp_cache_location = configs.get("SELENIUM_CACHE_LOCATION")
        self.base_save_location = configs.get("PREPARED_EVENT_PATH")
        self.cache_dir = cache_dir # From the function it needs to be called with mktemp directory
        self.logger = get_logger(self.__class__.__name__+"__TEST",log_dir="D:/Match_Fetching_Logs")
        self.opta_events = OptaEvents.objects.all().values()
        self.opta_qualifiers = OptaQualifier.objects.all().values()

    @timed_retry(3)
    def __get_driver(self) :
        os.makedirs(f"{self.temp_cache_location}", exist_ok=True)
        os.makedirs(f"{self.temp_cache_location}/profile", exist_ok=True)
        os.makedirs(f"{self.temp_cache_location}/cache", exist_ok=True)

        options = webdriver.ChromeOptions()
        # CLEAN CONSOLE CHANGE 1
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        options.add_argument(f"--user-data-dir={self.cache_dir}")
        options.add_argument(f"--disk-cache-dir={os.path.join(self.cache_dir, 'cache')}")
        options.add_argument("--disk-cache-size=104857600")  # 100 MB
        driver = webdriver.Chrome(options=options)
        # logger.info("Returning Driver")
        return driver  

    @contextmanager
    def __safe_driver(self) :
        driver = self.__get_driver()
        try:
            yield driver  # Yield the driver to be used in the 'with' block
        finally:
            if driver :
                driver.quit()  # This runs automatically when the block is exited
    
    def __fetch_event_api_response(self,url) :
        with self.__safe_driver() as event_driver : 
            self.logger.info("Created a safe driver.")
            event_driver.get(url)
            self.logger.info("Waiting for 30s to make sure all APIs are completed execution.")
            sleep(10)
            for request in event_driver.requests:
                try:
                    if "soccerdata/matchevent" in request.url :
                        self.logger.info(f"Found the Correct API link : '{request.url}'")

                        if b'\x1f\x8b' in request.response.body[:2]:  # type: ignore # GZIP magic number
                            self.logger.info(f"Found data in gzip with gzip magic number.")
                            body = gzip.decompress(request.response.body) # type: ignore
                            
                        else:  # Assume Deflate
                            body= zlib.decompress(request.response.body, wbits=zlib.MAX_WBITS | 32)   # type: ignore
                            self.logger.info(f"Found data in zlib deflate.")
                                              
                        return eval(body.decode().split('(')[-1][:-1])
                except Exception as e:
                    self.logger.error(f"Issue while fetching event data : {log_exception(e)}")
    
    def __return_event_data_and_description(self,row,source_df) :
        current_type_id = row.typeId
        working_area_type = source_df[source_df['typeId'] == current_type_id][['event_name','description']]
        if working_area_type.empty :
            return "NA", current_type_id
        return [item.strip(" ").strip("\r").strip("\n") for item in working_area_type.iloc[0].to_list()]
    
    def __return_qualifier_data(self,row,source_df) :
        updated_qualifier = []
        for item in row.qualifier:
            q_id = item.get('qualifierId')
            working_area_qualifier = source_df[source_df['opta_id'] == q_id][['qualifier_name']]
            if working_area_qualifier.empty :
                item['qualifier_name'],item['description'] = ['NA',q_id]
                updated_qualifier.append(item)
                continue
            item['qualifier_name'] = working_area_qualifier.iloc[0].to_list()[0]
            item['qualifier_name'] = item['qualifier_name'].strip(" ").strip("\r").strip("\n")
            updated_qualifier.append(item)
        return updated_qualifier
        
    def __parse_event_body(self,event_body): 
        try:
            # print(result.keys())
            # element = event_body['matchevent']
            # content_element = element['response_content']
            
            # Get Team names
            teams_df = pd.DataFrame(event_body.get('matchInfo',{}).get('contestant',[]))
            self.logger.info(f"Team Names found : {not teams_df.empty}")
            # Get opta-event IDs
            type_id_df = pd.DataFrame(self.opta_events)
            type_id_df.rename(columns={'opta_id':'typeId'},inplace=True)
            self.logger.info(f"Opta Events dataframe : {not type_id_df.empty} | Columns : {list(type_id_df.columns)}")
            # Get opta-qualifiers IDs
            qualifiers_df = pd.DataFrame(self.opta_qualifiers)
            self.logger.info(f"Opta Qualifiers dataframe : {not qualifiers_df.empty} | Columns : {list(qualifiers_df.columns)}")
            
            # Preparing correct format of data
            events_df = pd.DataFrame(event_body.get('liveData',{}).get('event',[]))
            self.logger.info(f"Events dataframe : {not events_df.empty} | Count : {events_df.shape[0]}")
            
            # L1
            id1 = teams_df['id'].iloc[0]
            team1 = teams_df['officialName'].iloc[0]
            id2 = teams_df['id'].iloc[1]
            team2 = teams_df['officialName'].iloc[1]
            
            events_df['contestantId'] = np.where(events_df['contestantId']==id1,team1,team2)
            events_df['outcome'] = np.where(events_df['outcome']==1,'Successful','Unsuccessful')
            events_df.rename(columns={"contestantId":'team','timeMin':'minute','timeSec':'seconds'},inplace=True)
            event_action_result = events_df.apply(self.__return_event_data_and_description,args=(type_id_df,),axis=1)
            self.logger.info(f"Completed Opta events mapping.")
            
            # Unpack the result into two new columns
            events_df['typeId'],events_df['description'] = zip(*event_action_result)
            events_df['qualifier'] = events_df.apply(self.__return_qualifier_data,args=(qualifiers_df,),axis=1)
            self.logger.info(f"Completed Opta qualifiers mapping.")
            events_df.rename(columns={'periodId':'period','typeId':'event_type','timeStamp':'timestamp','playerId':'player_id',"playerName":'player_name','keyPass':'key_pass'},inplace=True)
            
            # Add most used qualifiers
            events_df['end_x'] = 0.0
            events_df['end_y'] = 0.0
            events_df['goal_mouth_y'] = 0.0
            events_df['goal_mouth_z'] = 0.0
            events_df['big_chance'] = False
            events_df['own_goal'] = False
            events_df['zone'] = ''

            for i,row in events_df.iterrows():
                qualifiers = row.get('qualifier',[])
                for qualifier in qualifiers:
                    name = qualifier.get('qualifier_name','').lower()
                    if name == 'pass end x' :
                        events_df.at[i,'end_x'] = float(qualifier.get('value',-1.0)) # type: ignore
                    if name == 'pass end y' :
                        events_df.at[i,'end_y'] = float(qualifier.get('value',-1.0))
                    if name == 'goal mouth y co-ordinate' :
                        events_df.at[i,'goal_mouth_y'] = float(qualifier.get('value',-1.0))
                    if name == 'goal mouth z co-ordinate' :
                        events_df.at[i,'goal_mouth_z'] = float(qualifier.get('value',-1.0))
                    if name == 'big chance' :
                        events_df.at[i,'big_chance'] = True
                    if name == 'own goal' :
                        events_df.at[i,'own_goal'] = True
                    if name == 'zone' :
                        events_df.at[i,'zone'] = qualifier.get('value') if qualifier.get('value') else "NA"
            
            # Replace Period Names
            events_df['period'] = events_df["period"].replace({
                                    1 : "FirstHalf", 2 : "SecondHalf",
                                    3 : "ExtraTimeFirstHalf", 4 : "ExtraTimeSecondHalf",
                                    16 : 'Start', 14 : "End"
                                })
            events_df['qualifier'] = events_df['qualifier'].astype(str).apply(eval)

            return events_df
        except Exception as e:
            self.logger.error(f"Error While parsing events : {log_exception(e)}")

    def __fetch_shot_api_response(self,url) : 
        with self.__safe_driver() as event_driver : 
            self.logger.info("Created a safe driver.")
            event_driver.get(url)
            self.logger.info("Waiting for 10s to make sure all APIs are completed execution.")
            sleep(10)
            for request in event_driver.requests:
                try:
                    if "matchDetails?" in request.url :# \
                        self.logger.info(f"Found the info containing API call : '{request.url}'")
                        encoding = request.response.headers.get('Content-Encoding')
                        self.logger.info(f"Encoding in the request is : '{encoding}'")
                        body = request.response.body
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
                                self.logger.info("UTF-8 decode failed, printing raw bytes")
                        return json.loads(body)       
                except Exception as e:
                    self.logger.error(f"Issue while fetching shot data : {log_exception(e)}")
        
    def __parse_shot_api_response(self,url):
        shot_info = self.__fetch_shot_api_response(url)
        
        # initialize the values
        shots = []
        momentum = []
        if not shot_info :
            self.logger.error(f"Somehow shots_info is not found check into this with url = '{url}'") 
            return False,"shot_info_not_found"
     
        if shot_info.get('content',{}).get('shotmap',{}).get('shots',[]) == []:
            self.logger.error(f"No shot data found. content had keys : '{','.join(shot_info.get('content',{}).keys())}'")
        else :
            shots = shot_info.get('content',{}).get('shotmap',{}).get('shots',[])
            
        if bool(shot_info.get('content',{}).get('momentum',0)) is False:
            self.logger.error(f"No momentum data found. content had keys : '{','.join(shot_info.get('content',{}).keys())}'")
        else:
            if shot_info.get('content',{}).get('momentum',{}).get('main',{}).get('data',[]) == []:
                self.logger.error(f"No momentum data found. content had keys : '{','.join(shot_info.get('content',{}).keys())}'")
            else:
                momentum=shot_info.get('content',{}).get('momentum',{}).get('main',{}).get('data',[])
        
        return shots,momentum
    
    def __merge_expected_goals(self,shots,events_df) :
        self.logger.info("Merging shots to events ...")

        required_cols = {"id", "expectedGoals", "expectedGoalsOnTarget"}
        if not required_cols.issubset(shots.columns):
            self.logger.warning("Shots data missing expected xG columns, skipping merge")
            return events_df

        try:
            merged_df = events_df.merge(
                shots[list(required_cols)],
                on="id",
                how="left"
            )
            
            return merged_df.rename(columns={"expectedGoals": "xG","expectedGoalsOnTarget": "xGOT"})
        except Exception as e:
            self.logger.error(f"Error while merging shots: {e}")
            return events_df
             
    def __merge_momentum_data(self,momentum,events_df):
        self.logger.info("Merging momentum to events ...")

        required_cols = {"minute","period","momentum"}
        if not required_cols.issubset(momentum.columns):
            self.logger.warning("Momentum missing expected columns, skipping merge")
            return events_df

        try:
            merged_df = events_df.merge(
                momentum[list(required_cols)],
                on=['period','minute'],
                how="left"
            )
            return merged_df
        except Exception as e:
            self.logger.error(f"Error while merging momentum: {e}")
            return events_df
    
    def __parse_momentum_data(self,momentum_data,et_announcements):
        if not momentum_data:
            self.logger.error(f"Momentum info is invalid : {momentum_data}")
            return
        if et_announcements.empty:
            self.logger.error(f"Announcement info is empty : {et_announcements}")
            return
        if 'qualifier' not in et_announcements.columns:
            self.logger.error(f"Qualifier data not found : {et_announcements.columns}")
            return
        self.logger.info("Parsing momentum data for use ..")
        result = {
            "FirstHalf": {},
            "SecondHalf": {},
            "ExtraTimeFirstHalf": {},
            "ExtraTimeSecondHalf": {}
        }
        for item in momentum_data:
            if type(item['minute']) == int :
                minute = item["minute"]
                momentum_value = item["value"]
                if minute <= 45:
                    result["FirstHalf"][minute] = momentum_value
                elif minute <= 90:
                    result["SecondHalf"][minute] = momentum_value
                elif minute <= 105:
                    result["ExtraTimeFirstHalf"][minute] = momentum_value
                else:
                    result["ExtraTimeSecondHalf"][minute] = momentum_value
            else :
                momentum_value=  item["value"]
                if int(item['minute'])==45 and 0.5 == item['minute']%1:
                    period = 'FirstHalf'
                elif int(item['minute'])==90 and 0.5 == item['minute']%1:
                    period = 'SecondHalf'
                elif int(item['minute'])==105 and 0.5 == item['minute']%1:
                    period = 'ExtraTimeFirstHalf'
                elif int(item['minute'])==120 and 0.5 == item['minute']%1:
                    period = 'ExtraTimeSecondHalf'
                else :
                    continue
                
                et_time = int(et_announcements.loc[period,'qualifier'][0].get('value',0)) if len(et_announcements.loc[period,'qualifier']) == 1 else -120
                et_dict = {int(item['minute'])+i : momentum_value for i in range(1,et_time+2)}
                result[period] = {**result.get(period),**et_dict}  
        
        lookup = (
            pd.DataFrame({k:v for k,v in result.items() if v!={}})
            .stack()
            .reset_index()
            .rename(columns={
                "level_0": "minute",
                "level_1": "period",
                0: "momentum"
            })
        )
        lookup['minute'] -= 1
        return lookup
            
    def __merge_shots_and_momentum_with_event_data(self,events:pd.DataFrame,shot_info,momentum_info):
        # create shots_df
        if shot_info :
            shots = pd.DataFrame(shot_info)
            # merge shots
            events = self.__merge_expected_goals(shots,events)
            self.logger.info("Merged shots into events Dataframe !")
            # parse momentum info
        else:
            self.logger.info(f"No Shot Data is seen so avoiding the blowup. {shot_info}")
        
        if momentum_info :
            announcements = events[events['event_type'] == 'Injury Time Announcement'][['period','qualifier']].set_index('period')
            parsed_momentum = self.__parse_momentum_data(momentum_info,announcements)
            self.logger.info("Parsed momentum data !")
            # merge momentum
            events = self.__merge_momentum_data(parsed_momentum,events)
            self.logger.info("Merged momentum into events Dataframe !")
        else:
            self.logger.info(f"No Momentum Data is seen so avoiding the blowup. {shot_info}")
        return events
    
    def __get_path_to_save_df(self,season):
        s_name = season.name.replace("/","-").replace(" ","_")
        conf = season.competition.confederation.replace("/","-").replace(" ","_")
        country = season.competition.country.replace("/","-").replace(" ","_")
        comp_name = season.competition.competition_name.replace("/","-").replace(" ","_")
        
        os.makedirs(f"{self.base_save_location}", exist_ok=True)
        os.makedirs(f"{self.base_save_location}/{conf}", exist_ok=True)
        os.makedirs(f"{self.base_save_location}/{conf}/{country}", exist_ok=True)
        os.makedirs(f"{self.base_save_location}/{conf}/{country}/{comp_name}", exist_ok=True)
        os.makedirs(f"{self.base_save_location}/{conf}/{country}/{comp_name}/{s_name}", exist_ok=True)
        return f"{self.base_save_location}/{conf}/{country}/{comp_name}/{s_name}"
    
    def fetch_game_data_for_the_season(self,season_id):
        try:
            tracker,error_items = None,None
            self.logger.info("Starting to fetch the event dataframe..")
            season = Season.objects.select_related('competition').get(id=season_id)
            self.logger.info(f"Working in {season.competition.competition_name} - {season.name_fotmob}")
            stats = Game.objects.filter(season_id=season_id,event_status='not_done').aggregate(
                total_event_matches=Count(
                    'id',
                    filter=Q(game_event_url__isnull=False) & ~Q(game_event_url='')
                ),
                total_shot_matches=Count(
                    'id',
                    filter=Q(game_shot_url__isnull=False) & ~Q(game_shot_url='')
                ),
            )

            all_games_of_season = Game.objects.filter(season_id=season_id,event_status='not_done',id=18622).select_related('season__competition').values('game_event_url','game_shot_url','id')
            self.logger.info(f"Found {stats.get('total_event_matches',-1)} matches with events and {stats.get('total_shot_matches',-1)} matches with shots to be pulled")
            tracker = {
                "competition": season.competition.competition_name,
                "name": season.name,
                "event_matches_pulled": 0,
                "event_matches_transformed": 0,
                "shot_matches_pulled": 0,
                "merge_successes": 0,
                "merge_failure": 0,
                'save_success':0,
                'save_failure':0
            } 
            tracker = {**tracker,**stats}
            
            error_items = []
            
            save_file_path = self.__get_path_to_save_df(season)
            for game in all_games_of_season :
                # Initialization of possible variables
                fotmob_shots, fotmob_momentum = [],[]
                prepared_dataframe = None
                try:
                    self.logger.info("STEP 1 : GET EVENT BODY")
                    event_body = self.__fetch_event_api_response(game.get('game_event_url'))
                    if not event_body:
                        self.logger.error("We have not found any body here. Need to Look in this")
                        error_items.append({
                            "stage" : "Blank event body",
                            "game_id" : game.get("id"),
                            "error" : "Manual Check One off"
                            }
                        )
                        continue
                    tracker['event_matches_pulled']+=1
                except Exception as e :
                    exc = log_exception(e)
                    error_items.append({
                        "stage" : "Get event body",
                        "game_id" : game.get("id"),
                        "error" : exc
                        }
                    )
                    self.logger.error(f"Issue in STEP 1 for '{game.get('id')}': {exc}")
                    continue
                try:
                    events = self.__parse_event_body(event_body)
                    tracker['event_matches_transformed']+=1
                except Exception as e :
                    exc = log_exception(e)
                    error_items.append({
                        "stage" : "Parse event body",
                        "game_id" : game.get("id"),
                        "error" : exc
                        }
                    )
                    self.logger.error(f"Issue in STEP 2 for '{game.get('id')}: {exc}")
                    continue   
                try:
                    msg = "Proceeding to fetch shot and momentum data ..." if game.get('game_shot_url','') != '' else "No FotMob URL found. Skipping further processes."
                    self.logger.info(msg)
                    if game.get('game_shot_url',None):
                        fotmob_shots, fotmob_momentum = self.__parse_shot_api_response(game.get('game_shot_url',None))
                    if bool(fotmob_shots) is False and bool(fotmob_momentum) is False :
                        self.logger.info(f"Alas, we tried to fetch shot and moemntum data but we got | shots = {bool(fotmob_shots)} ; momentum{bool(fotmob_momentum)}. Since both are flase, no need to go for a merge")
                    else:
                        tracker['shot_matches_pulled']+=1
                        
                except Exception as e :
                    exc = log_exception(e)
                    error_items.append({
                        "stage" : "Fetch shots",
                        "game_id" : game.get("id"),
                        "error" : exc
                        }
                    )
                    self.logger.error(f"Issue in STEP 3 for '{game.get('id')}: {exc}")
                    continue
                
                try:
                    if fotmob_momentum or fotmob_shots :
                        prepared_dataframe = self.__merge_shots_and_momentum_with_event_data(events,fotmob_shots,fotmob_momentum)
                        tracker['merge_successes']+=1
                except Exception as e :
                    exc = log_exception(e)
                    error_items.append({
                        "stage" : "Merge",
                        "game_id" : game.get("id"),
                        "error" : exc
                        }
                    )
                    tracker['merge_failure']+=1
                    self.logger.error(f"Issue in STEP 4 for '{game.get('id')}: {exc}")
                    continue
                
                try:
                    event_file_path = fr"{save_file_path}/{game.get('id')}.xlsx"
                    self.logger.info(f"Saving the events to {event_file_path} ..")
                    prepared_dataframe.to_excel(event_file_path,index=False)
                    self.logger.info(f"Saved events externally to {event_file_path} .")
                    tracker['save_success']+=1
                except Exception as e :
                    exc = log_exception(e)
                    error_items.append({
                        "stage" : "Save Externally",
                        "game_id" : game.get("id"),
                        "error" : exc
                        }
                    )
                    tracker['save_failure']+=1
                    self.logger.error(f"Issue in STEP 5 for '{game.get('id')}: {exc}")
                    continue
            
        except Exception as e:
            exc = log_exception(e)
            self.logger.error(f"Uncaught Issue : {exc}")
        finally :
            return tracker,error_items        
        
######## LOCAL TEST
# season_id = 293
# cache_dir = tempfile.mkdtemp(prefix=f"Tempu__", dir="D:/alt_cache")
# obj = MatchEventFetcher(cache_dir)
# obj.fetch_game_data_for_the_season(season_id) 