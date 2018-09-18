import sys
import json
import os

from pyspark.sql import SparkSession
import mysql.connector as sqlcon
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark import SparkConf, SparkContext, SQLContext

sc = SparkContext()
spark = SparkSession(sc)

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

df = sqlContext.createDataFrame(sc.emptyRDD(),schema)
print df

#file = sc.textFile("s3a://sy-insight-epa/raw_data/hourly_42401_2018.csv")
