from __future__ import print_function

import sys
import json
import os

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import mysql.connector as sqlcon
from botocore.exceptions import ClientError


def insert_to_db(partition):
    connection = sqlcon.connect(host = h,user = u, passwd = pwd, db = db)
    cursor = connection.cursor()
    sql = "INSERT IGNORE INTO customers VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    sql_filtered = "INSERT IGNORE INTO customers_filtered VALUES (%s, %s, %s, %s, %s, %s)"
    infos = []
    infos_filtered = []
    for info in partition:
        user_id = info['user_id']
        name = info['name']
        ssn = info['ssn']
        credit_card_number = info['credit_card_number']
        address = info['address']
        zipcode = info['zipcode']
        credit_card_limit = info['credit_card_limit']
        current_balance = info['current_balance']
        is_traveling = info['is_traveling']
        description = info['description']
        background = info['background']
        infos.append((user_id, name, ssn, credit_card_number, address,
            zipcode, credit_card_limit, current_balance, is_traveling, description, background))
        infos_filtered.append((user_id, zipcode, credit_card_limit,
            current_balance, is_traveling, 0.0))
    try:
        cursor.executemany(sql, infos)
        cursor.executemany(sql_filtered, infos_filtered)
        connection.commit()
    except:
        connection.rollback()
    connection.close()
    return infos


if __name__ == "__main__":

    sc = SparkContext(appName="")
    some_rdd = sc.textFile("s3a://")

    msgs = some_rdd.map(lambda x: extractor(x)).filter(lambda x: none_filter(x))
    inserted_customers = msgs.mapPartitions(insert_to_db)
print(inserted_customers.count())
