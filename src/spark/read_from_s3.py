import sys
import json
import os

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import mysql.connector as sqlcon

file = sc.textFile("s3a://openaq-fetches/realtime/2018-09-03/1536018864.ndjson")
