# Air Quality Monitor
The goal of my Insight Data Engineering project was to develop a data pipeline and Web platform for monitoring air quality globally combined with climate data such as temperature and wind. My data application will provide a relational database that data scientists and analysts can easily query and retrieve the air quality and climate data joined together for each location and time, to perform analytics and modeling.

## Table of contents
## Motivation
Air quality is very important for human health and the environment. Air pollution can cause both short term and long term effects on health, e.g. respiratory diseases. Therefore, it is worthwhile to actively monitor and update the air quality parameters. Air quality is related with climate data, and it can be very interesting for data scientist and environmental scientist to study this relationship.

The statistical results of each air quality parameter for a week, a month, the same time of everyday, the same month every year will be compute. The changing rates of the air quality parameters will also be tracked. It could be used for monitoring local air quality and providing warnings when air quality is unacceptable.

The broader goal of this project is to build a platform combining air quality data and weather data for data analytics teams to help develop new predictive models of air quality.

## Data pipeline
![pipeline_image](./doc/Pipeline.png)
<br>
<br>
1. Download datasets on EC2 instance and then transfer to S3 bucket
2. Data cleaning, transformation, joining and computation are processed by Spark.
3. Joined tables and analytics results are loaded into a MySQL database launched on RDS.
4. The historical monthly averaged air pollutant level and weather parameters for each county will be displayed on a webpage made by Dash.
5. The EPA website keeps updating the datasets multiple times per year. Therefore, Airflow is used to automate the data acquisition, batch processing and storage on every month.
