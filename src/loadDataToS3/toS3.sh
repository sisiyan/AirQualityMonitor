#!/bin/bash

#unzip all zip files
cd /home/ubuntu/data
unzip \*.zip -d /home/ubuntu/data_unpack

#upload to S3
echo "transferring data files to s3"
aws s3 cp /home/ubuntu/data_unpack s3://sy-insight-aq-climate-data/data --recursive
