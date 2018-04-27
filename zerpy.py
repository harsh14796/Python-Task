import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
import wget
import zipfile
import os
import redis
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By

url = 'https://www.bseindia.com/markets/equity/EQReports/Equitydebcopy.aspx'
"""
#downloading file via slelenium webdriver
class ClickAndSendKeys():

    def test(self):
        driver = webdriver.Chrome()
        driver.maximize_window()
        driver.get(url)
        driver.implicitly_wait(5)

    bhavcopy = driver.find_element(By.ID, "btnhylZip")
    bhavcopy.click()
    driver.close()
"""
# OR via beautiful soup
conn = urlopen(url)
html = conn.read()
soup = BeautifulSoup(html,"lxml")

tag = soup.find(id='btnhylZip') #id of zip file
#link of zip file.
link = tag.get('href',None)

zip_name = wget.download(link)
print()
zip_ref = zipfile.ZipFile(zip_name, 'r')
zip_ref.extractall("csv-files/")
csv_file = zip_ref.namelist()[0]
zip_ref.close()
print(csv_file[0:-4])

#Removing zip file
os.remove(zip_name)

data = pd.read_csv("csv-files/"+csv_file)

#screening the columns
data = data[['SC_CODE', 'SC_NAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE']].copy()

#Percantage difference
data['PERCENTAGE'] = round(((data['CLOSE'] - data['OPEN'])/ data['OPEN'])*100, 2)

data_compute = data.copy()

#Top ten gainers and losers
data_gain = data_compute.nlargest(10,['PERCENTAGE']).copy()
data_loose = data_compute.nsmallest(10,['PERCENTAGE']).copy()

#redis database connection
if os.environ.get("REDIS_URL") :
    redis_url = os.environ.get("REDIS_URL")
else:
    redis_url = "localhost"
r = redis.from_url(redis_url)

#inserting elemets to database
for index, row in data.iterrows():
	r.hmset(row['SC_CODE'],row.to_dict())
	r.set("equity:"+row['SC_NAME'],row['SC_CODE'])

#Delete old gainers and loosers.
for key in r.scan_iter("gain:*"):
	r.delete(key)
for key in r.scan_iter("loose:*"):
	r.delete(key)

#Saving sc code of gainers
for index, row in data_gain.iterrows():
	r.set("gain:"+row['SC_NAME'],row['SC_CODE'])

#Saving sc code of losers
for index, row in data_loose.iterrows():
	r.set("loose:"+row['SC_NAME'],row['SC_CODE'])

#last updated string in redis database
r.set("latest",csv_file[2:4]+"-"+csv_file[4:6]+"-20"+csv_file[6:8])
