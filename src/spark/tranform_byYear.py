import sys
import json
import os

from pyspark.sql import SparkSession
import mysql.connector as sqlcon
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark import SparkConf, SparkContext, SQLContext
from boto.s3.connection import S3Connection
from pyspark.sql.types import *

sc = SparkContext()
sqlContext = SQLContext(sc)
weather_codes = ['WIND','TEMP','PRESS','RH_DP']
gases_codes = ['44201', '42401','42101','42602']
particulates_codes = ['88101','88502','81102']

#define the parameter code list
parameter_codes = ['44201', '42401','42101','42602','88101','88502','81102',
    'SPEC','PM10SPEC','WIND','TEMP','PRESS','RH_DP']

#define the dictionary map the file names to schema column names
schema_dict = {
    '44201': "ozone", '42401': "SO2",'42101':"CO",'42602':"NO2",
    '88101': "PM2.5_FRM",'88502':"PM2.5_nonFRM",'81102':"PM10_mass",
    'SPEC':"PM2.5_speciation",'PM10SPEC': "PM10_speciation",
    'WIND':"winds",'TEMP':"temperature",'PRESS':"pressure",'RH_DP':"RH_dewpoint"
}

def convert_to_int(string):
    '''
    Returns an integer if it can or returns None otherwise
    '''
    try:
        number = int(string)
    except ValueError:
        return None
    return number

def file_year_paraCode(fname):
    '''
    Given the filename XXX_Code_year.extension, return integer year and code
    '''
    try:
        basename = fname.split('.')[0]
        parameterCode = basename.split('_')[1]
        if parameterCode == 'RH':
            year_string = basename.split('_')[3]
            parameterCode = "RH_DP"
        else:
            year_string = basename.split('_')[2]
    except (ValueError, IndexError):
        return None, None
    if parameterCode not in parameter_codes:
        print parameterCode
        return None, None
    year = convert_to_int(year_string)
    if not year:
        print parameterCode
        return None, None
    return year, parameterCode



def get_file_list_perYear(bucket_name, target_year):
    '''
    Given the S3 bucket, return a list of files in the same year
    '''

    file_list = []

    conn = S3Connection()
    bucket = conn.get_bucket(bucket_name)

    for bucket_object in bucket.get_all_keys():
        fname = bucket_object.key

        if not fname.startswith('hourly') and not fname.startswith('Hourly'):
            continue
        year,parameterCode = file_year_paraCode(fname)

        if not year:
            continue
        if year == target_year:
            file_list.append((fname, year))

    return [f[0] for f in file_list]


files_year = get_file_list_perYear("sy-insight-epa-data", 1999)
print files_year

weather_files = []
gases_files = []
particulates_files = []
for fname in files_year:
    year,parameterCode = file_year_paraCode(fname)
    if parameterCode in weather_codes:
        weather_files.append(fname)
    if parameterCode in gases_codes:
        gases_files.append(fname)
    if parameterCode in particulates_codes:
        particulates_files.append(fname)

weather_files = ['hourly_TEMP_1999.csv', 'hourly_PRESS_1999.csv']

df_join_weather = None
for fname in weather_files:
    year,parameterCode = file_year_paraCode(fname)
    fdata = sqlContext.read.format('com.databricks.spark.csv').option('header', 'true').load('s3a://sy-insight-epa-data/'+fname)

    if parameterCode == "RH_DP":
        fdata = fdata.filter(fdata["Parameter Name"] == "Relative Humidity ")
    if parameterCode == "WIND":
        fdata = fdata.filter(fdata["Parameter Code"] == "61103")

    df = fdata.select('State Name', 'County Name', 'Latitude','Longitude','Date GMT','Time GMT','Sample Measurement','MDL')
    parameter = schema_dict[parameterCode]
    df = df.withColumnRenamed("Sample Measurement", parameter).withColumnRenamed("State Name", "state_name").withColumnRenamed("County Name", "county_name").withColumnRenamed("Date GMT", "Date_GMT").withColumnRenamed("Time GMT", "Time_GMT")
    df = df.withColumn("latitude", df["Latitude"].cast(DoubleType())).withColumn("longitude", df["Longitude"].cast(DoubleType())).withColumn(parameter, df[parameter].cast(DoubleType()))

    if df_join_weather == None:
        df_join_weather = df
    else:
        df_join_weather = df_join_weather.join(df, ["state_name",'county_name','latitude','longitude','Date_GMT','Time_GMT'],"outer")
        print df_join_weather.count()


print df_join_weather.take(10)
