#!/usr/bin/env python
# coding: utf-8


# !pip install feedparser
# !pip install pymongo[srv]
# from IPython.display import clear_output
# !pip install -U sentence-transformers
# !pip install --no-cache-dir transformers sentencepiece
# !pip install transformers
# !pip install newspaper3k
# !pip install torch
# !pip install selenium
# !pip install webdriver-manager
# !pip install schedule
#!pip install certifi

try:
   if compareNewsTitle:
      if(len(compareNewsTitle)>130):
        compareNewsTitle.clear()

except NameError:
    compareNewsTitle= ["test"]

try:
   if responseList:
      if(len(responseList)>130):
        responseList.clear()

except NameError:
    responseList= []
    
notification_title=["hi"]


try:
   if newsJsonList:
      if(len(newsJsonList)>130):
        newsJsonList.clear()

except NameError:
    newsJsonList= ["mock"]



import feedparser
import schedule
import re
from IPython.display import clear_output
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.parser import parse
import torch
from sentence_transformers import SentenceTransformer, util
from pymongo import MongoClient
import pytz  
from IPython.display import clear_output
from newspaper import Article
from newspaper import Config
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM,AutoModel
from time import sleep
import requests
import json
import certifi
import schedule
import time
import certifi
import ssl
# Selenium Drivers
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time 
from webdriver_manager.chrome import ChromeDriverManager

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:78.0) Gecko/20100101 Firefox/78.0'
serverToken = 'AAAAWxk_7Xc:APA91bGb3xz7gLJYtqKiT0kWsPnew--7hqSitGa4Rad9am3Bq64_MXvrg-W4c-NY3JQ6zogCzWlO6YISMEmDYwaH-k-t2uHahtVVE5iBOe-xIvfZkB4wPYS6yCloqTrdqpT9Exk2x0TX'

config = Config()
config.browser_user_agent = user_agent
config.request_timeout = 10


IST = pytz.timezone('Asia/Kolkata')
blackListWordsList=['moneycontrol','mc','lottery','coronavirus','morning','scan','petrol','diesel','?','et','economics','times','ettech','etmarkets','covid','target',
                    'explained','explain','euro','monsoon','terror','terrorism','delta','vaccine','vaccinated','vairant','why','pro','etprime','pics']

rssFeeds = [
    ('MoneyControl', 'Global Market','https://www.moneycontrol.com/rss/internationalmarkets.xml'),
    ('MoneyControl', 'Business','https://www.moneycontrol.com/rss/business.xml'),
    ('MoneyControl', 'IPO','https://www.moneycontrol.com/rss/iponews.xml'),
    ('MoneyControl','Economy','https://www.moneycontrol.com/rss/economy.xml'),
    ('MoneyControl', 'Technology','https://www.moneycontrol.com/rss/technology.xml'),
    ('MoneyControl','Buzzing Stocks','https://www.moneycontrol.com/rss/buzzingstocks.xml'),
    ('EconomicsTimes', 'Industry', 'https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms'),
    ('EconomicsTimes', 'Business', 'https://economictimes.indiatimes.com/small-biz/rssfeeds/5575607.cms'),  
    ('EconomicsTimes', 'Technology', 'https://economictimes.indiatimes.com/tech/rssfeeds/13357270.cms'),
    ('EconomicsTimes', 'Markets', 'https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms'),
    ('EconomicsTimes', 'Markets', 'https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms'),
    ('EconomicsTimes', 'Economy', 'https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms'),
    ('EconomicsTimes', 'Bonds', 'https://economictimes.indiatimes.com/markets/bonds/rssfeeds/2146846.cms'),
    ('EconomicsTimes', 'Crypto', 'https://economictimes.indiatimes.com/markets/cryptocurrency/rssfeeds/82519373.cms'),
    ('FinancialExpress', 'Economy', 'https://www.financialexpress.com/economy/feed/'),
    ('FinancialExpress', 'Markets', 'https://www.financialexpress.com/industry/feed/'),
    ('FinancialExpress', 'Finance', 'https://www.financialexpress.com/industry/banking-finance/feed/'),
    ('FinancialExpress', 'Global Market', 'https://www.financialexpress.com/market/world-markets/feed/'),
    ('FinancialExpress', 'Commodity', 'https://www.financialexpress.com/market/commodities/feed/'),
    ('IIFL', 'Markets', 'https://www.indiainfoline.com/rss/news.xml'),
    ('IIFL', 'Business', 'https://www.indiainfoline.com/rss/resultexpress.xml'),
    ('LiveMint', 'Business', 'https://www.livemint.com/rss/companies'),
    ('LiveMint', 'Insurance', 'https://www.livemint.com/rss/insurance'),
#     ('LiveMint', 'Money', 'https://www.livemint.com/rss/money'),
    ('LiveMint', 'Mutual Funds', 'https://www.livemint.com/rss/Mutual%20Funds'),
]

#Mongodb installations
client = MongoClient('mongodb+srv://easyeyesprod:1CJtPx3l2dZGk1Rz@finboxprod.qp2zb.mongodb.net/finbox?retryWrites=true&w=majority',tlsCAFile=certifi.where())
db=client.get_database('finbox')
news=db.article


GOOGLE_CHROME_PATH = '/app/.apt/usr/bin/google_chrome'
CHROMEDRIVER_PATH = '/app/.chromedriver/bin/chromedriver'
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')

chrome_options.binary_location = GOOGLE_CHROME_PATH
driver = webdriver.Chrome(execution_path=CHROMEDRIVER_PATH, chrome_options=chrome_options)
#Install driver
# opts=webdriver.ChromeOptions()
# opts.headless=True

# driver = webdriver.Chrome(ChromeDriverManager().install() ,options=opts)

def isBlackListWordsPresent(sentence):
  if any(word in sentence for word in blackListWordsList):
    return True
  else:
    return False

def isBlackListWordsPresentInDescription(sentence):
  if any(word in sentence.lower() for word in blackListWordsList):
    return True
  else:
    return False

def timeDiff(publishedDate):
  return ((datetime.now(IST).replace(tzinfo=None)-parse(publishedDate).replace(tzinfo=None)).total_seconds())/3600

def parseRSS( rss_url ):
    return feedparser.parse(rss_url)

def cleanHtml(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext

def parseMoneyControlFeed(publisher, feed):
  result = dict()
  soup = BeautifulSoup(feed.summary, "html.parser")
  images = soup.find_all("img")

  #skip if no image exists for feed
  if len(images)==0 or bool(cleanHtml(feed.summary))==False or isBlackListWordsPresent(feed.title.lower())==True or isBlackListWordsPresentInDescription(cleanHtml(feed.summary).lower())==True: return None

  if timeDiff(feed.published)<=5:
    result['title'] = feed.title,
    result['imageUrl'] = images[0].get("src")
    result['description'] = cleanHtml(feed.summary)
    result['meta'] = {}
    result['meta']['publisher'] = publisher 
    result['meta']['publishTime'] = feed.published
    result['meta']['publishUrl'] = feed.link
    return result

def parseEconomicTimesFeed(publisher, feed):
  result = dict()
  soup = BeautifulSoup(feed.summary, "html.parser")
  images = soup.find_all("img")

  # #skip if no image exists for feed
  if len(images)==0 or bool(cleanHtml(feed.summary))==False or len(cleanHtml(feed.summary))<150 or isBlackListWordsPresent(feed.title.lower())==True or isBlackListWordsPresentInDescription(cleanHtml(feed.summary).lower())==True: return None
  
  if timeDiff(feed.published)<=.5:
    result['title'] = feed.title,
    result['imageUrl'] = images[0].get("src")
    result['description'] = cleanHtml(feed.summary)
    result['meta'] = {}
    result['meta']['publisher'] = publisher 
    result['meta']['publishTime'] = feed.published
    result['meta']['publishUrl'] = feed.link
    return result

def parseFinancialExpressFeed(publisher, feed):
  result = dict()
  #skip if no image exists for feed
  if "media_content" not in feed  or bool(cleanHtml(feed.summary))==False or isBlackListWordsPresent(feed.title.lower())==True: return None

  if timeDiff(feed.published)<=5:
    result['title'] = feed.title,
    result['imageUrl'] = feed.media_content[0]['url']
    result['description'] = cleanHtml(feed.summary)
    result['meta'] = {}
    result['meta']['publisher'] = publisher 
    result['meta']['publishTime'] = feed.published
    result['meta']['publishUrl'] = feed.link
    return result

def parseIIFLFeed(publisher, feed):
  result = dict()
  #skip if no image exists for feed
  if "media_thumbnail" not in feed  or bool(cleanHtml(feed.summary))==False or isBlackListWordsPresent(feed.title.lower())==True: return None

  if timeDiff(feed.published)<=5:
    result['title'] = feed.title,
    result['imageUrl'] = feed.media_thumbnail[0]['url']
    result['description'] = cleanHtml(feed.summary)
    result['meta'] = {}
    result['meta']['publisher'] = publisher 
    result['meta']['publishTime'] = feed.published
    result['meta']['publishUrl'] = feed.link
    return result


def parseLiveMintFeed(publisher, feed):
  result = dict()

  #skip if no image exists for feed
  if "media_content" not in feed or bool(cleanHtml(feed.summary))==False or isBlackListWordsPresent(feed.title.lower())==True or isBlackListWordsPresent(feed.title.lower())==True: return None

  if timeDiff(feed.published)<=.5:
    result['title'] = feed.title,
    result['imageUrl'] = feed.media_content[0]['url']
    result['description'] = cleanHtml(feed.summary)
    result['meta'] = {}
    result['meta']['publisher'] = publisher 
    result['meta']['publishTime'] = feed.published
    result['meta']['publishUrl'] = feed.link
    return result


def parseFeedByPublisher(publisher, feed):
  switcher={
      "MoneyControl": parseMoneyControlFeed,
      "EconomicsTimes": parseEconomicTimesFeed,
      "FinancialExpress": parseFinancialExpressFeed,
      "IIFL": parseIIFLFeed,
      "LiveMint":parseLiveMintFeed,
  }
  return switcher.get(publisher)(publisher, feed);

      
def parseNewsFromRSS(publisher,  rss_url):
    items = [] 
    feeds = parseRSS(rss_url)
    for feed in feeds.entries:
      parsedFeed = parseFeedByPublisher(publisher, feed)
      if parsedFeed:
        items.append(parsedFeed) 
    return items

def getNewsItems():
  newsItems = []
  for source, newsType, url in rssFeeds:
      newsItems.append({'newsType': newsType,'news':parseNewsFromRSS(source, url)})
  for item in newsItems:
    print(item)
  return newsItems

def isSemanticSimilarity(sentence1,sentence2):
  cosine_score_list=[]
  model=SentenceTransformer('stsb-mpnet-base-v2')
  clear_output
  embedding1=model.encode(sentence1,convert_to_tensor=True)
  for i in range(len(sentence2)):
    # print(sentence2[i])
    embedding2=model.encode(sentence2[i],convert_to_tensor=True)
    cosine_scores=util.pytorch_cos_sim(embedding1,embedding2)
    if cosine_scores>0.5:
      return True
    else:
      continue
  return False

def getSummary(news_url):
  try:
    article=Article(news_url,config=config)
    article.download()
    article.parse()
    # Uses Facebook Large CNN Model
    summarizer=pipeline("summarization", model="facebook/bart-large-cnn")
    summary_output=summarizer(article.text[:1024],max_length=100,min_length=50)
    replace_strings= {'\u200b': '',
                      '\n': '',
                      '\\': '',
                      '/': '',}
    for x,y in replace_strings.items():
        summary = summary_output[0]['summary_text'].replace(x, y)
    return summary

  except:
    return None



def fetch_image_urls(query:str, max_links_to_fetch:int, wd:webdriver, sleep_between_interactions:int=1):  
    
    # build the google query
    search_url = "https://www.google.com/search?safe=off&site=&tbm=isch&source=hp&q={q}&oq={q}&gs_l=img"

    # load the page
    wd.get(search_url.format(q=query))

    image_urls = set()
    image_count = 0
    results_start = 0
    while image_count < max_links_to_fetch:

        # get all image thumbnail results
        thumbnail_results = wd.find_elements_by_css_selector("img.Q4LuWd")
        number_results = len(thumbnail_results)
        
        for img in thumbnail_results[results_start:number_results]:
            # try to click every thumbnail such that we can get the real image behind it
            try:
                img.click()
                time.sleep(sleep_between_interactions)
            except Exception:
                continue

            # extract image urls    
            actual_images = wd.find_elements_by_css_selector('img.n3VNCb')
            for actual_image in actual_images:
                if actual_image.get_attribute('src') and 'http' in actual_image.get_attribute('src'):
                  if actual_image.get_attribute('src').endswith('.jpg') or actual_image.get_attribute('src').endswith('.png') or actual_image.get_attribute('src').endswith('.jpeg'):
                    image_urls.add(actual_image.get_attribute('src'))

            image_count = len(image_urls)

            if len(image_urls) >= max_links_to_fetch:
                # print(f"Found: {len(image_urls)} image links, done!")
                break
        else:
            # print("Found:", len(image_urls), "image links, looking for more ...")
            time.sleep(30)
            return
            load_more_button = wd.find_element_by_css_selector(".mye4qd")
            if load_more_button:
                wd.execute_script("document.querySelector('.mye4qd').click();")

        # move the result startpoint further down
        results_start = len(thumbnail_results)

    if(len(image_urls)!=0):
      # print("Image present")
      image_urls.pop()
      return image_urls.pop()
    else:
      # print("Image url is not present")
      return None
def getSummaryArticle(article):
  try:
    # Uses Facebook Large CNN Model
    summarizer=pipeline("summarization", model="facebook/bart-large-cnn")
    summary_output=summarizer(article[:1024],max_length=100,min_length=50)
    replace_strings= {'\u200b': '',
                      '\n': '',
                      '\\': '',
                      '/': '',}
    for x,y in replace_strings.items():
        summary = summary_output[0]['summary_text'].replace(x, y)
    return summary

  except:
    return None
def getImageUrl(news_url):
  hashtags_list=[]
  try:
    article=Article(news_url,config=config)
    article.download()
    article.parse()
    for i in article.tags:
      if(i.startswith('#')):
        hashtags_list.append(i)
    if(len(hashtags_list)!=0):
      print("hashtag present "+ hashtags_list[0][1:])
      return hashtags_list[0][1:]
    else:
      print("Hashtags not present")
      return None
  except:
    return None

def getSleepTime(totalArticles):
  sleep_time=None
  count=0
  for article in totalArticles:
    if(len(article['news'])!=0):
      for newsItem in article['news']:
        count=count+1
  print("Article count is"+ str(count))
  if count> 20:
    print("Sleep time is "+ str(120))
    return 120
  elif count <20 and count> 15:
    print("Sleep time is "+ str(180))
    return 180
  elif count <15 and count> 10:
    print("Sleep time is "+ str(210))
    return 180
  elif count <10 and count> 5:    
    print("Sleep time is "+ str(240))
    return 240
  else:    
    print("Sleep time is "+ str(360))
    return 360 

def cleanhtml(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html).replace("#39", "")
  return cleantext

def insertNews():
  response=""  
  content= ""
  imageUrl= ""
  tags= ""
  title= ""
  publishUrl= ""
  publisher= ""
  publishtime= ""
  scrapedNewsArticles= getNewsItems()
  sleep_time=getSleepTime(scrapedNewsArticles)
  headers = {
        'Content-Type': 'application/json',
        'Authorization': 'key=' + serverToken,
      }

  for items in scrapedNewsArticles:
    if(len(items['news'])!=0):
      tags=items['newsType']
      for newsItem in items['news']:
        if len(compareNewsTitle)==0:
          compareNewsTitle.append(newsItem['title'][0])
          if newsItem['meta']['publisher']=="MoneyControl" or newsItem['meta']['publisher']=="LiveMint" or newsItem['meta']['publisher']=="FinancialExpress":
            summary= getSummary(newsItem['meta']['publishUrl']) if getSummary(newsItem['meta']['publishUrl'])!=None else newsItem['description']
            content= "<p>" + summary +"<a href=" + newsItem['meta']['publishUrl'] + "><br> Read more at " + newsItem['meta']['publisher']  + "</a></p>"
          elif newsItem['meta']['publisher']=="IIFL":
            driver.get(newsItem['meta']['publishUrl'])
            content= "<p>" + summary +"<a href=" + newsItem['meta']['publishUrl'] + "><br> Read more at " + newsItem['meta']['publisher']  + "</a></p>"
          else:
            content= "<p>" + newsItem['description']+"<a href=" + newsItem['meta']['publishUrl'] + "><br> Read more at " + newsItem['meta']['publisher']  + "</a></p>"

          if newsItem['meta']['publisher']=="MoneyControl":
            if tags=="Buzzing Stocks":
              imageUrl="https://images.moneycontrol.com/static-mcnews/2021/03/BUZZING-STOCKS-770x433.jpg?impolicy=website&width=770&height=431"
            else:
              imageUrl= fetch_image_urls(getImageUrl(newsItem['meta']['publishUrl']),5,driver) if getImageUrl(newsItem['meta']['publishUrl'])!=None else newsItem['imageUrl']
          else:
            imageUrl= newsItem['imageUrl']
          
          # print("Imageurl used is "+ imageUrl)


          news_json={'content':content,
            'imageUrl': imageUrl,
            'tags': tags,
            'title': newsItem['title'][0],
            "time": str(datetime.now(IST)),
            'meta':{
                'publishUrl': newsItem['meta']['publishUrl'],
                'publisher': newsItem['meta']['publisher'],
                'publishtime':newsItem['meta']['publishTime'] }}
          
          response=news.insert_one(news_json)
          responseList.insert(0,response)
          newsJsonList.insert(0,news_json)  
          # print("Sleeping")
          sleep(sleep_time)
          # print("awake")

        else:
          if isSemanticSimilarity(newsItem['title'][0],compareNewsTitle)==False:
            compareNewsTitle.append(newsItem['title'][0])
            if newsItem['meta']['publisher']=="MoneyControl" or newsItem['meta']['publisher']=="LiveMint" or newsItem['meta']['publisher']=="FinancialExpress":
              summary= getSummary(newsItem['meta']['publishUrl']) if getSummary(newsItem['meta']['publishUrl'])!=None else newsItem['description']
              content= "<p>" + summary +"<a href=" + newsItem['meta']['publishUrl'] + "><br> Read more at " + newsItem['meta']['publisher']  + "</a></p>"
            elif newsItem['meta']['publisher']=="IIFL":
              driver.get(newsItem['meta']['publishUrl'])
              summary= getSummaryArticle(driver.find_elements_by_xpath("//*[@id='story_card']/div[2]/div")[0].text.replace("\n"," "))
              content= "<p>" + summary +"<a href=" + newsItem['meta']['publishUrl'] + "><br> Read more at " + newsItem['meta']['publisher']  + "</a></p>"
            else:
              content= "<p>" + newsItem['description']+"<a href=" + newsItem['meta']['publishUrl'] + "><br> Read more at " + newsItem['meta']['publisher']  + "</a></p>"

            if newsItem['meta']['publisher']=="MoneyControl":
              imageUrl= fetch_image_urls(getImageUrl(newsItem['meta']['publishUrl']),5,driver) if getImageUrl(newsItem['meta']['publishUrl'])!=None else newsItem['imageUrl']

            else:
              imageUrl= newsItem['imageUrl']     

            print("Imageurl used is "+ imageUrl)       
            
            news_json={'content':content,
              'imageUrl': imageUrl,
              'tags': tags,
              'title': newsItem['title'][0],
              "time": str(datetime.now(IST)),
              'meta':{
                  'publishUrl': newsItem['meta']['publishUrl'],
                  'publisher': newsItem['meta']['publisher'],
                  'publishtime':newsItem['meta']['publishTime']}}
            
            response=news.insert_one(news_json)
            responseList.insert(0,response)
            newsJsonList.insert(0,news_json)
            print("Sleeping")
            sleep(sleep_time)
            print("awake")
            
          else:
            continue
  if len(newsJsonList)>0:        
      if notification_title[0]!=newsJsonList[0]['title']:        
          notification_title.insert(0,newsJsonList[0]['title'])
          if len(newsJsonList)>0:
            body = {
                  'notification': {'title': cleanhtml(newsJsonList[0]['title']),
                                    'body': "Read here",
                                    'image': newsJsonList[0]['imageUrl']
                                    },
                  'to':
                      "/topics/all",
                  'priority': 'high',
                  'data': {
                      'page':'/news',
                      'articleId': str(responseList[0].inserted_id)
                  },
                }
            response = requests.post("https://fcm.googleapis.com/fcm/send",headers = headers, data=json.dumps(body))
            print(response.status_code)  


if __name__ == '__main__':
  schedule.every(1).minutes.do(insertNews)

  while 1:
      schedule.run_pending()
      time.sleep(1)



