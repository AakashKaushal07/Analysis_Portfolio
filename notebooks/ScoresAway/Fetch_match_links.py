#!/usr/bin/env python
# coding: utf-8

# In[1]:


from selenium import webdriver
from selenium.webdriver.common.by import By
import requests
from bs4 import BeautifulSoup
from time import sleep
import DataFrameGetter as dfg


# In[2]:


# ISL_MAIN_URL = "https://www.scoresway.com/en_GB/soccer/indian-super-league-2023-2024/as2gkuxuvi5zqktyqlc2t4kk4/results"
ISL_MAIN_URL = "https://www.scoresway.com/en_GB/soccer/indian-super-league-2024-2025/2h95ink16qa9eabhpeydbyz9w/results"


# In[3]:


def getSoup(url) :
    res = requests.get(url)
    soup = BeautifulSoup(res.text)
    sleep(3)
    return soup

def getDriver(url) :
    driver = webdriver.Chrome()
    driver.get(url)
    driver.implicitly_wait(10)
    sleep(5)
    return driver  
    
def removeCookieDialog(driver):
    driver.execute_script("var obj = document.getElementById('onetrust-banner-sdk');if(obj){obj.remove()};")

def getMaxElement() :
    driver = getDriver(ISL_MAIN_URL)
    removeCookieDialog(driver)
    clicker = driver.find_element(By.CSS_SELECTOR, '.Opta-Exp')
    clicker.click()

    ul_element = driver.find_element(By.CSS_SELECTOR, 'ul.Opta-Cf')
    max_index = len(ul_element.find_elements(By.TAG_NAME, 'li'))
    driver.close()
    return max_index


# In[4]:


max_index = getMaxElement()


# In[5]:


max_index


# In[ ]:


mw_result_links = {}

for i in range(max_index) :
    inner_driver = getDriver(ISL_MAIN_URL)
    removeCookieDialog(inner_driver)
    clicker = inner_driver.find_element(By.CSS_SELECTOR, '.Opta-Exp')
    clicker.click()
    ul_element = inner_driver.find_element(By.CSS_SELECTOR, 'ul.Opta-Cf')
    
    all_mw = ul_element.find_elements(By.TAG_NAME, 'a')
    if len(all_mw) < i :
        print(i,"Overflow",all_mw)
    mw_item  = all_mw[i]
    name = mw_item.text
    print(i,"WORKING FOR : ",name,mw_item.tag_name)
    # continue
    mw_item.click()
    fixture_tables =[x for x in inner_driver.find_elements(By.CSS_SELECTOR,'tbody.Opta-result.Opta-fixture') if x.text]
    urls = []
    for j,fixture in enumerate(fixture_tables) :
        try :
            print(f"Working on {name}, Game {j+1}")
            match_page = fixture.find_element(By.CSS_SELECTOR, '[title="View match"]')
            match_page.click()
            # sleep(5)
            removeCookieDialog(inner_driver)
            # results_panes = inner_driver.find_elements(By.CSS_SELECTOR,'ul.striplist')
            result_urls = [x.find_element(By.TAG_NAME,'a').get_property('href')
                     for x in inner_driver.find_element(By.CSS_SELECTOR,'ul.striplist').find_elements(By.TAG_NAME,'li') 
                     if x.text == 'PLAYER STATS']
            if result_urls :
                urls.append((j,result_urls[0]))
            inner_driver.execute_script("window.history.go(-1)")
            # break
        except Exception as e :
            print(type(e),print(e))
            break
    
    if not name :
        name = "Playoff"
    if name not in mw_result_links :
        mw_result_links[name] = urls
    inner_driver.close()

mw_result_links.keys()

def get_matchweek_game_results(url):
    driver = getDriver(url)
    removeCookieDialog(driver)
    fixture_tables =[x for x in driver.find_elements(By.CSS_SELECTOR,'tbody.Opta-result.Opta-fixture') if x.text]
    urls = []
    
    for i,fixture in enumerate(fixture_tables) :
        try :
            match_page = fixture.find_element(By.CSS_SELECTOR, '[title="View match"]')
            match_page.click()
            sleep(5)
            removeCookieDialog(driver)
            # results_panes = driver.find_elements(By.CSS_SELECTOR,'ul.striplist')
            result_urls = [x.find_element(By.TAG_NAME,'a').get_property('href')
                     for x in driver.find_element(By.CSS_SELECTOR,'ul.striplist').find_elements(By.TAG_NAME,'li') 
                     if x.text == 'PLAYER STATS']
            if result_urls :
                urls.append((i,result_urls[0]))
            driver.execute_script("window.history.go(-1)")
        except Exception as e :
            driver.close()
            break
    return urls

# as2gkuxuvi5zqktyqlc2t4kk4/5vqm3epot9ch15e2ldqjzypsk/

