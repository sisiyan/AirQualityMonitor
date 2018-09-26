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
from pyspark.sql import functions
import datetime
import time

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
    '88101': "PM2point5_FRM",'88502':"PM2point5_nonFRM",'81102':"PM10_mass",
    'SPEC':"PM2point5_speciation",'PM10SPEC': "PM10_speciation",
    'WIND':"winds",'TEMP':"temperature",'PRESS':"pressure",'RH_DP':"relative_humidity"
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

def classify_files(files_per_year):
    #Find file names for each type of parameter on each year
    weather_files = []
    gases_files = []
    particulates_files = []
    for fname in files_per_year:
        year,parameterCode = file_year_paraCode(fname)
        if parameterCode in weather_codes:
            weather_files.append(fname)
        if parameterCode in gases_codes:
            gases_files.append(fname)
        if parameterCode in particulates_codes:
            particulates_files.append(fname)
    return weather_files, gases_files, particulates_files

def average_over_day(fdata):

    df = fdata.select('State Name', 'County Name', 'Latitude','Longitude','Date GMT','Time GMT','Sample Measurement')
    df_dailyBin = df.groupby('State Name', 'County Name','Latitude','Longitude','Date GMT')\
                    .agg({'Sample Measurement': 'mean'})
    return df_dailyBin

def rename_cols(df, parameter_avg):
    df = df.withColumnRenamed("avg(Sample Measurement)", parameter_avg)\
        .withColumnRenamed("State Name", "state_name")\
        .withColumnRenamed("County Name", "county_name")\
        .withColumnRenamed("Date GMT", "date_GMT")

    df = df.withColumn("latitude", df["Latitude"].cast(DoubleType()))\
        .withColumn("longitude", df["Longitude"].cast(DoubleType()))\
        .withColumn(parameter_avg, df[parameter_avg].cast(DoubleType()))

    return df


def main():
    for yr in range(2011, 2019):

        files_per_year = get_file_list_perYear("sy-insight-epa-data", yr)
        # for fname in files_per_year
        # df_dailyBin = average_over_day("hourly_42101_1980.csv")
        # print df_dailyBin.filter((df_dailyBin["Latitude"] == 33.520661) & (df_dailyBin["Longitude"] == -86.801934) & (df_dailyBin["Date GMT"] == "1980-01-01")).show()


        weather_files, gases_files, particulates_files = classify_files(files_per_year)

        # Outer Join weather data
        df_join_weather = None
        for fname in weather_files:
            year,parameterCode = file_year_paraCode(fname)
            fdata = sqlContext.read.format('com.databricks.spark.csv').option('header', 'true')\
                    .load('s3a://sy-insight-epa-data/'+fname).dropDuplicates()

            if parameterCode == "RH_DP":
                fdata = fdata.filter(fdata["Parameter Name"] == "Relative Humidity ")
            if parameterCode == "WIND":
                fdata = fdata.filter(fdata["Parameter Code"] == "61103")

            df = average_over_day(fdata)

            parameter_avg = schema_dict[parameterCode] + "_avg"
            df = rename_cols(df, parameter_avg)

            if df_join_weather == None:
                df_join_weather = df
            else:
                df_join_weather = df_join_weather\
                    .join(df, ["state_name",'county_name','latitude','longitude','date_GMT'],"outer")

        # Outer join all gasese pollutant data
        df_join_gases = None
        for fname in gases_files:
            year,parameterCode = file_year_paraCode(fname)
            fdata = sqlContext.read.format('com.databricks.spark.csv').option('header', 'true')\
                    .load('s3a://sy-insight-epa-data/'+fname).dropDuplicates()

            df = average_over_day(fdata)
            parameter_avg = schema_dict[parameterCode] + "_avg"
            df = rename_cols(df, parameter_avg)

            if df_join_gases == None:
                df_join_gases = df
            else:
                df_join_gases = df_join_gases\
                    .join(df, ["state_name",'county_name','latitude','longitude','date_GMT'],"outer")

        # Outer join all particulate pollutant data
        df_join_particulates = None
        for fname in particulates_files:
            year,parameterCode = file_year_paraCode(fname)

            if parameterCode == 'SPEC' or parameterCode == 'PM10SPEC':
                continue

            fdata = sqlContext.read.format('com.databricks.spark.csv').option('header', 'true')\
                    .load('s3a://sy-insight-epa-data/'+fname).dropDuplicates()
            df = average_over_day(fdata)
            parameter_avg = schema_dict[parameterCode] + "_avg"
            df = rename_cols(df, parameter_avg)

            if df_join_particulates == None:
                df_join_particulates = df
            else:
                df_join_particulates = df_join_particulates.join(df, ["state_name",'county_name','latitude','longitude','date_GMT'],"outer")

        # Inner join the weather data and gas pollutant data
        df_join_gases_weather = df_join_weather\
                                .join(df_join_gases, ["state_name",'county_name','latitude','longitude','date_GMT'], "inner")
        split_date = functions.split(df_join_gases_weather['date_GMT'], '-')
        df_join_gases_weather = df_join_gases_weather.withColumn('GMT_year', split_date.getItem(0))
        df_join_gases_weather = df_join_gases_weather.withColumn('GMT_month', split_date.getItem(1))

        df_join_gases_weather = df_join_gases_weather\
            .withColumn("date_GMT", df_join_gases_weather["date_GMT"].cast(DateType()))\
            .withColumn('GMT_year', df_join_gases_weather['GMT_year'].cast(IntegerType()))\
            .withColumn('GMT_month', df_join_gases_weather['GMT_month'].cast(IntegerType()))\
            .dropDuplicates()

        # Inner join the weather data and particulate pollutant data
        df_join_particulates_weather = df_join_weather\
                                .join(df_join_particulates, ["state_name",'county_name','latitude','longitude','date_GMT'], "inner")
        split_date = functions.split(df_join_particulates_weather['date_GMT'], '-')
        df_join_particulates_weather = df_join_particulates_weather.withColumn('GMT_year', split_date.getItem(0))
        df_join_particulates_weather = df_join_particulates_weather.withColumn('GMT_month', split_date.getItem(1))

        df_join_particulates_weather = df_join_particulates_weather\
            .withColumn("date_GMT", df_join_particulates_weather["date_GMT"].cast(DateType()))\
            .withColumn('GMT_year', df_join_particulates_weather['GMT_year'].cast(IntegerType()))\
            .withColumn('GMT_month', df_join_particulates_weather['GMT_month'].cast(IntegerType()))\
            .dropDuplicates()
        print "Year: "+ str(yr) + " is done."

        # write the joined the weather and gas pollutant data to database
        df_join_gases_weather.write\
            .format("jdbc")\
            .option("url", "jdbc:mysql://airqualityweather.cyncvghu6naw.us-east-1.rds.amazonaws.com:3306/airQualityWeather")\
            .option("driver", "com.mysql.jdbc.Driver")\
            .option("truncate", "true")\
            .option("fetchsize", 1000)\
            .option("batchsize", 100000)\
            .option("dbtable", "Gases_Weather_Join_Daily")\
            .option("user", "root")\
            .option("password", "ys8586dswfye") \
            .mode('append')\
            .save()

        # write the joined the weather and particulate pollutant data to database
        df_join_particulates_weather.write\
            .format("jdbc")\
            .option("url", "jdbc:mysql://airqualityweather.cyncvghu6naw.us-east-1.rds.amazonaws.com:3306/airQualityWeather")\
            .option("driver", "com.mysql.jdbc.Driver")\
            .option("truncate", "true")\
            .option("fetchsize", 1000)\
            .option("batchsize", 100000)\
            .option("dbtable", "Particulates_Weather_Join_Daily")\
            .option("user", "root")\
            .option("password", "ys8586dswfye") \
            .mode('append')\
            .save()

if __name__ == '__main__':
    main()
