#!/bin/bash

mysql -h endpoint -P 3306 \
    -u db_username -pdb_password < /home/ubuntu/insightProject/src/mysql/db_operations.sql
