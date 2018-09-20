from pyspark.sql import SparkSession
import mysql.connector as sqlcon
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark import SparkConf, SparkContext, SQLContext
from boto.s3.connection import S3Connection
from pyspark.sql.types import *
import functools


sc = SparkContext()
sqlContext = SQLContext(sc)

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



def get_file_list_perParameter(bucket_name, target_Code):
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
        if parameterCode == target_Code:
            file_list.append((fname, parameterCode))

    return [f[0] for f in file_list]

def unionAll(dataframe_list):
    return functools.reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dataframe_list)

temperature_files = get_file_list_perParameter("sy-insight-epa-data", "TEMP")
wind_files = get_file_list_perParameter("sy-insight-epa-data", "WIND")
pressure_files = get_file_list_perParameter("sy-insight-epa-data", "PRESS")
RHDP_files = get_file_list_perParameter("sy-insight-epa-data", "RH_DP")

print temperature_files
# print wind_files
# print pressure_files
# print RHDP_files

temperature_files = ["hourly_TEMP_1997.csv", "hourly_TEMP_1998.csv","hourly_TEMP_1999.csv"]
temperature_df = None
for fname in temperature_files:
    fdata = sqlContext.read.format('com.databricks.spark.csv').option('header', 'true').load('s3a://sy-insight-epa-data/'+fname)
    df = fdata.select('State Name', 'County Name', 'Latitude','Longitude','Date GMT','Time GMT','Sample Measurement')
    if temperature_df == None:
        temperature_df = df
    else:
        temperature_df = temperature_df.union(df)
temperature_df = temperature_df.withColumnRenamed("Sample Measurement", "temperature").withColumnRenamed("State Name", "state_name").withColumnRenamed("County Name", "county_name").withColumnRenamed("Date GMT", "Date_GMT").withColumnRenamed("Time GMT", "Time_GMT")

#temperature_df = temperature_df.withColumn("latitude", df["Latitude"].cast(DoubleType())).withColumn("longitude", df["Longitude"].cast(DoubleType())).withColumn("temperature", df["temperature"].cast(DoubleType()))

wind_files = ["hourly_WIND_1997.csv", "hourly_WIND_1998.csv","hourly_WIND_1999.csv"]
wind_df = None
for fname in wind_files:
    fdata = sqlContext.read.format('com.databricks.spark.csv').option('header', 'true').load('s3a://sy-insight-epa-data/'+fname)
    df = fdata.select('State Name', 'County Name', 'Latitude','Longitude','Date GMT','Time GMT','Sample Measurement')
    if wind_df == None:
        wind_df = df
    else:
        wind_df = wind_df.union(df)
wind_df = wind_df.withColumnRenamed("Sample Measurement", "winds").withColumnRenamed("State Name", "state_name").withColumnRenamed("County Name", "county_name").withColumnRenamed("Date GMT", "Date_GMT").withColumnRenamed("Time GMT", "Time_GMT")
#wind_df = wind_df.withColumn("latitude", df["Latitude"].cast(DoubleType())).withColumn("longitude", df["Longitude"].cast(DoubleType())).withColumn("winds", df["winds"].cast(DoubleType()))

pressure_files = ["hourly_PRESS_1997.csv", "hourly_PRESS_1998.csv","hourly_PRESS_1999.csv"]
pressure_df = None
for fname in pressure_files:
    fdata = sqlContext.read.format('com.databricks.spark.csv').option('header', 'true').load('s3a://sy-insight-epa-data/'+fname)
    df = fdata.select('State Name', 'County Name', 'Latitude','Longitude','Date GMT','Time GMT','Sample Measurement')
    if pressure_df == None:
        pressure_df = df
    else:
        pressure_df = pressure_df.union(df)
pressure_df = pressure_df.withColumnRenamed("Sample Measurement", "pressure").withColumnRenamed("State Name", "state_name").withColumnRenamed("County Name", "county_name").withColumnRenamed("Date GMT", "Date_GMT").withColumnRenamed("Time GMT", "Time_GMT")
#pressure_df = wind_df.withColumn("latitude", df["Latitude"].cast(DoubleType())).withColumn("longitude", df["Longitude"].cast(DoubleType())).withColumn("pressure", df["pressure"].cast(DoubleType()))

RHDP_files = ["hourly_RH_DP_1997.csv", "hourly_RH_DP_1998.csv","hourly_RH_DP_1999.csv"]
RHDP_df = None
for fname in RHDP_files:
    fdata = sqlContext.read.format('com.databricks.spark.csv').option('header', 'true').load('s3a://sy-insight-epa-data/'+fname)
    df = fdata.select('State Name', 'County Name', 'Latitude','Longitude','Date GMT','Time GMT','Sample Measurement')
    if RHDP_df == None:
        RHDP_df = df
    else:
        RHDP_df = RHDP_df.union(df)
RHDP_df = RHDP_df.withColumnRenamed("Sample Measurement", "RH_dewpoint").withColumnRenamed("State Name", "state_name").withColumnRenamed("County Name", "county_name").withColumnRenamed("Date GMT", "Date_GMT").withColumnRenamed("Time GMT", "Time_GMT")
#RHDP_df = RHDP_df.withColumn("latitude", df["Latitude"].cast(DoubleType())).withColumn("longitude", df["Longitude"].cast(DoubleType())).withColumn("RH_dewpoint", df["RH_dewpoint"].cast(DoubleType()))

#most import weather parameters, large size
temp_join_wind = temperature_df.join(wind_df, ["state_name",'county_name','latitude','longitude','Date_GMT','Time_GMT'],"outer")
#temp_join_wind_pressure = temp_join_wind.join(pressure_df, ["state_name",'county_name','latitude','longitude','Date_GMT','Time_GMT'],"left")
weather_join = temp_join_wind.join(pressure_df, ["state_name",'county_name','latitude','longitude','Date_GMT','Time_GMT'],"left").join(RHDP_df, ["state_name",'county_name','latitude','longitude','Date_GMT','Time_GMT'],"left")


weather_join.write.csv('weather_join_1997to1999.csv')
print weather_join.count()
