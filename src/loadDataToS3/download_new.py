import os


baseUrl = "https://aqs.epa.gov/aqsweb/airdata/hourly_"

year = range(1980,2019)
code = ['44201', '42401','42101','42602','88101','88502','81102','SPEC','PM10SPEC','WIND','TEMP','PRESS','RH_DP']

print year
print code

files = []

for y in year:
    for c in code:
        yearStr = str(y)
        url = baseUrl + c + "_" + yearStr + ".zip"
        print url
        files.append(url)

for url in files:
    os.system("wget " + url + " -P /home/ubuntu/data --trust-server-names")
