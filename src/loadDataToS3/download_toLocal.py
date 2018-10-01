import os
import sys 

def download_files(task_year):
    baseUrl = "https://aqs.epa.gov/aqsweb/airdata/hourly_"

    code = ['44201', '42401','42101','42602','88101','88502','81102','WIND','TEMP','PRESS','RH_DP']

    print(task_year)

    files = []

    for c in code:
        url = baseUrl + c + "_" + task_year + ".zip"
        print(url)
        files.append(url)

    for url in files:
        os.system("wget " + url + " -P /home/ubuntu/data --trust-server-names")

if __name__ == "__main__":
    if len(sys.argv) > 2:
        print('too many arguments\n')
        exit()
    year = sys.argv[1]
    download_files(year)
