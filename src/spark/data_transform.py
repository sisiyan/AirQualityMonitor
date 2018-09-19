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

"""
    Files with different parameters will be joined together to form a big dataframe
    by using the location and GMT_time
"""
#Define the column names for the target schema
field = [StructField("state_name",StringType(),True),StructField("county_name",StringType(),True),
    StructField("latitude",StringType(),True),StructField("longitude",StringType(),True),
    StructField("GMT_year", StringType(), True), StructField("GMT_month", StringType(), True),
    StructField("GMT_day", StringType(), True), StructField("GMT_time", StringType(), True),
    StructField("ozone", StringType(), True), StructField("SO2", StringType(), True),
    StructField("CO", StringType(), True), StructField("NO2", StringType(), True),
    StructField("PM2.5_FRM", StringType(), True), StructField("PM2.5_nonFRM", StringType(), True),
    StructField("PM10_mass", StringType(), True), StructField("PM2.5_speciation", StringType(), True),
    StructField("PM10_speciation", StringType(), True), StructField("winds", StringType(), True),
    StructField("temperature", StringType(), True), StructField("pressure", StringType(), True),
    StructField("RH_dewpoint", StringType(), True)
    ]

schema = StructType(field)

#Create an empty dataframe with the column names
df = sqlContext.createDataFrame(sc.emptyRDD(),schema)
print df

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

#test the correctness
#print get_file_list_perYear("sy-insight-epa-data", 2018)
#file = sc.textFile("s3a://sy-insight-epa/raw_data/hourly_42401_2018.csv")

#for yr in range(1980, 2019):
files_year = get_file_list_perYear("sy-insight-epa-data", 1980)
print files_year

files_year = ['hourly_42101_1980.csv', 'hourly_42401_1980.csv', 'hourly_81102_1980.csv', 'hourly_WIND_1980.csv']

#files_year = ['hourly_42101_1980.csv', 'hourly_42401_1980.csv']

df_join = None

for fname in files_year:
    fdata = sqlContext.read.format('com.databricks.spark.csv').option('header', 'true').load('s3a://sy-insight-epa-data/'+fname)
    df = fdata.select('State Name', 'County Name', 'Latitude','Longitude','Date GMT','Time GMT','Sample Measurement')
    year, parameterCode = file_year_paraCode(fname)
    print schema_dict[parameterCode]
    parameter = schema_dict[parameterCode]
    df = df.withColumnRenamed("Sample Measurement", parameter).withColumnRenamed("State Name", "state_name").withColumnRenamed("County Name", "county_name").withColumnRenamed("Date GMT", "Date_GMT").withColumnRenamed("Time GMT", "Time_GMT")
    df = df.withColumn("latitude", df["Latitude"].cast(DoubleType())).withColumn("longitude", df["Longitude"].cast(DoubleType())).withColumn(parameter, df[parameter].cast(DoubleType()))


    if df_join == None:
        df_join = df
    else:
        df_join = df_join.join(df, ["state_name",'county_name','latitude','longitude','Date_GMT','Time_GMT'],"outer")


# df_join.write\
#     .format("jdbc")\
#     .option("url", "jdbc:mysql://airqualityweather.cyncvghu6naw.us-east-1.rds.amazonaws.com:3306/airQualityWeather")\
#     .option("driver", "com.mysql.jdbc.Driver")\
#     .option("dbtable", "testTable")\
#     .option("user", "root")\
#     .option("password", "ys8586dswfye") \
#     .mode('append')\
#     .save()



print df_join
#print "======================== Total Number of Rows ========================="
# print df_join.count()
# print "========================================================================"


# df_join = df_join.filter(~(df_join.winds.isNull() & df_join.temperature.isNull() & df_join.pressure.isNull()))
#
# print df_join.take(10)
# print "======================== Total Number of Rows ========================="
# print df_join.count()
# print "========================================================================"
