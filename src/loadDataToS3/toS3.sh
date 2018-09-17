#!/bin/bash
# This script pulls all submission and comment Json data from files.pushshift.io.
# Then the script expands all files using bzip2
# Finally the files are uploaded to S3 to the specific bucket s3://ac-reddit-data/Raw/

mdkir /home/ubuntu/data
mkdir /home/ubuntu/data_unpack
cd /home/ubuntu/data

wget https://aqs.epa.gov/aqsweb/airdata/download_files.html#Raw/hourly*.zip

# mkdir climate_data
# mkdir climate_data_unpack
# cd climate_data
# wget -r -nH -nd -R index.html* https://www.ncei.noaa.gov/data/global-hourly/archive/
# wait
#
for filename in /home/ubuntu/data/*; do
  tar zxvf "$filename" -C /home/ubuntu/climate_data_unpack
  rm "$filename"
 wait
done
#
for filename in /home/ubuntu/data_unpack/*; do
  aws s3 cp "$filename" s3://sy-insight-aq-climate-data/data/
  rm "$filename"
  wait
done
