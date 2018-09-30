#!/bin/bash

RDS_MYSQL_ENDPOINT="airqualityweather.cyncvghu6naw.us-east-1.rds.amazonaws.com";
RDS_MYSQL_USER="root";
RDS_MYSQL_PASS="airqualityweathersiyan355";
RDS_MYSQL_BASE="airQualityWeather";

mysql -h $RDS_MYSQL_ENDPOINT -u $RDS_MYSQL_USER -p $RDS_MYSQL_PASS -D $RDS_MYSQL_BASE > -e 'quit';

if [[ $? -eq 0 ]]; then
    echo "MySQL connection: OK";
else
    echo "MySQL connection: Fail";
fi;
