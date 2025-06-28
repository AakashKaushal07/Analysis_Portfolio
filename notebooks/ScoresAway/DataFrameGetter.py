import gzip
import time
import zlib
from seleniumwire import webdriver
import pandas as pd
import numpy as np

## SCORES AWAY ELEMENT

# Fetch events data from match page
def fetch_event_response(url):
    driver = webdriver.Chrome()  # Use your specific driver setup
    driver.get(url)

    # Wait for 30 seconds to allow all requests to complete
    time.sleep(30)

    data = {}
    for request in driver.requests:
        try:
            # print("URL : ",request.url)
            if "https://api.performfeeds.com/soccerdata/matchevent" in request.url :# \
            # or "https://api.performfeeds.com/soccerdata/squad" in request.url :

                if b'\x1f\x8b' in request.response.body[:2]:  # GZIP magic number
                    body = gzip.decompress(request.response.body)
                else:  # Assume Deflate
                    body= zlib.decompress(request.response.body, wbits=zlib.MAX_WBITS | 32)
                req_type = request.url.split('https://api.performfeeds.com/soccerdata/')[-1].split('/')[0]
                if req_type not in data.keys() :
                    data[req_type] = {}
                    
                if request.response:  # Only process completed requests
                    data[req_type] = {
                            "url": request.url,
                            "method": request.method,
                            "status_code": request.response.status_code,
                            "response_content": eval(body.decode().split('(')[-1][:-1])
                        }
                    
        except Exception as e:
            print(f"Exception in {request.url}, some problme, fak it : {e}")
    driver.quit()
    return data

def return_event_data_and_description(row,source_df):
    current_type_id = row.typeId
    working_area_type = source_df[source_df['typeId'] == current_type_id][['Type','Description']]
    if working_area_type.empty :
        return "NA", current_type_id
    return [item.strip(" ").strip("\\r").strip("\\n") for item in working_area_type.iloc[0].to_list()]

def return_qualifier_data(row,source_df):
    updated_qualifier = []
    for item in row.qualifier:
        q_id = item.get('qualifierId')
        working_area_qualifier = source_df[source_df['ID'] == q_id][['name']]
        if working_area_qualifier.empty :
            item['qualifierName'],item['description'] = ['NA',q_id]
            updated_qualifier.append(item)
            continue
        item['qualifierName'] = working_area_qualifier.iloc[0].to_list()[0]
        item['qualifierName'] = item['qualifierName'].strip(" ").strip("\\r").strip("\\n")
        updated_qualifier.append(item)
    return updated_qualifier

# prepare the events df
def fetch_events_for_match(url) :
    # Example usage:
    result = fetch_event_response(url)
    # print(result.keys())
    element = result['matchevent']
    content_element = element['response_content']
    
    # Get Team names
    teams_df = pd.DataFrame(content_element['matchInfo']['contestant'])
    # Get opta-event IDs
    type_id_df = pd.read_csv(r'./opta_data/template/opta-events.csv', encoding = 'ISO-8859-1')
    type_id_df.rename(columns={'ID':'typeId'},inplace=True)
    # Get opta-qualifiers IDs
    qualifiers_df = pd.read_csv(r'./opta_data/template/opta-qualifiers.csv')
    # Preparing correct format of data
    events_df = pd.DataFrame(content_element['liveData']['event'])
    # L1
    id1 = teams_df['id'].iloc[0]
    team1 = teams_df['officialName'].iloc[0]
    id2 = teams_df['id'].iloc[1]
    team2 = teams_df['officialName'].iloc[1]
    
    events_df['contestantId'] = np.where(events_df['contestantId']==id1,team1,team2)
    events_df['outcome'] = np.where(events_df['outcome']==1,'Successful','Unsuccessful')
    events_df.rename(columns={"contestantId":'Team','timeMin':'Minute','timeSec':'Seconds'},inplace=True)
    # L2        
    event_action_result = events_df.apply(return_event_data_and_description,args=(type_id_df,),axis=1)
    # Unpack the result into two new columns
    events_df['typeId'],events_df['Description'] = zip(*event_action_result)
    events_df['qualifier'] = events_df.apply(return_qualifier_data,args=(qualifiers_df,),axis=1)
    return events_df

## FOTMOB ITEM
# Fetch events data from 
def fetch_shot_info_fotmob(url):
    driver = webdriver.Chrome()  # Use your specific driver setup
    driver.get(url)

    # Wait for 30 seconds to allow all requests to complete
    time.sleep(30)

    data = {}
    for request in driver.requests:
        try:
            if "https://api.performfeeds.com/soccerdata/matchevent" in request.url :# \
            # or "https://api.performfeeds.com/soccerdata/squad" in request.url :
                
                if b'\x1f\x8b' in request.response.body[:2]:  # GZIP magic number
                    body = gzip.decompress(request.response.body)
                else:  # Assume Deflate
                    body= zlib.decompress(request.response.body, wbits=zlib.MAX_WBITS | 32)
                req_type = request.url.split('https://api.performfeeds.com/soccerdata/')[-1].split('/')[0]
                if req_type not in data.keys() :
                    data[req_type] = {}
                    
                if request.response:  # Only process completed requests
                    data[req_type] = {
                            "url": request.url,
                            "method": request.method,
                            "status_code": request.response.status_code,
                            "response_content": eval(body.decode().split('(')[-1][:-1])
                        }
                    
        except Exception as e:
            print(f"Exception in {request.url}, some problme, fak it : {e}")
    driver.quit()
    return data
