#!/bin/bash
spark-submit --master spark://ec2-xx-xxx-xxx-xx.compute-1.amazonaws.com:7077 \
             --jars /usr/share/java/mysql-connector-java-8.0.12.jar \
             --driver-memory 2G \
             --executor-memory 2G \
             /home/ubuntu/insightProject/src/spark/join_daily_airQ_weather_onSite.py
