import pandas as pd
import sqlalchemy
import pymysql

def get_connection():
    connection = pymysql.connect(host='airqualityweather.cyncvghu6naw.us-east-1.rds.amazonaws.com',
                             user='root',
                             password='airqualityweathersiyan355',
                             db='airQualityWeather',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    return connection

def query_weather(connection, state, county, weather_param):
    weather_dict = {'Temperature': 'temperature_avg',
                    'Wind Speed': 'winds_avg',
                    'Relative Humidity': 'relative_humidity_avg',
                    'Barometric Pressure': 'pressure_avg'
                    }
    unit_dict = {'Temperature': 'Temperature (\xb0 F)',
                 'Wind Speed': 'Knots',
                 'Relative Humidity': '%',
                 'Barometric Pressure': 'mbar'}

    col_name = 'avg(' + weather_dict[weather_param] + ')'
    unit = unit_dict[weather_param]
    sql = "SELECT "+ col_name + ", date_GMT FROM Gases_Weather_Join_Daily WHERE state_name = %s AND county_name=%s GROUP BY GMT_year, GMT_month ORDER BY GMT_year, GMT_month"
    df = pd.read_sql(sql, connection, params=(state, county))

    return col_name, unit, df

def query_gases(connection, state, county, gases_param):
    params_dict = {'CO': 'CO_avg',
                    'SO2': 'SO2_avg',
                    'NO2': 'NO2_avg',
                    'Ozone': 'ozone_avg'
                    }
    unit_dict = {'CO': 'ppm',
                 'SO2': 'ppb',
                 'NO2': 'ppb',
                 'Ozone': 'ppm'}
    
    col_name = 'avg(' + params_dict[gases_param] + ')'
    unit = unit_dict[gases_param]
    sql = "SELECT "+ col_name + ", date_GMT FROM Gases_Weather_Join_Daily WHERE state_name = %s AND county_name=%s GROUP BY GMT_year, GMT_month ORDER BY GMT_year, GMT_month"
    df = pd.read_sql(sql, connection, params=(state, county))

    return col_name, unit, df


def query_particulates(connection, state, county, particulates_param):
    params_dict = {'PM2.5 FRM': 'PM2point5_FRM_avg',
                   'PM2.5 non FRM': 'PM2point5_nonFRM_avg',
                   'PM10 Mass': 'PM10_mass_avg'
                  }
    unit_dict = {'PM2.5 FRM': 'mg/m\u00b3',
                 'PM2.5 non FRM': 'mg/m\u00b3',
                 'PM10 Mass': 'mg/m\u00b3'}

    col_name = 'avg(' + params_dict[particulates_param] + ')'
    unit = unit_dict[particulates_param]
    sql = "SELECT "+ col_name + ", date_GMT FROM Particulates_Weather_Join_Daily WHERE state_name = %s AND county_name=%s GROUP BY GMT_year, GMT_month ORDER BY GMT_year, GMT_month"
    df = pd.read_sql(sql, connection, params=(state, county))

    return col_name, unit, df
