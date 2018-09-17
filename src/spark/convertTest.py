import sys
import json
import ndjson
import os



from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import mysql.connector as sqlcon
from botocore.exceptions import ClientError

# NDJSON->dictionary
def extractor(json_body):
    json_obj = json.loads(json_body)
    print(json_obj)
    try:
        parameter = json_obj['parameter']

    except:
        return None
    data = {'parameter': parameter}
    print data
    return data

if __name__ == "__main__":

    sc = SparkContext(appName="")
    some_rdd = sc.textFile("s3a://openaq-fetches/realtime/")

    msgs = some_rdd.map(lambda x: extractor(x))
