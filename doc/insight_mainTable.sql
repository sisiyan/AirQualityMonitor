use airQualityWeather;

create table testTable (
	id int NOT NULL AUTO_INCREMENT,
	state_name varchar(20) NOT NULL,
    county_name varchar(20) NOT NULL,
	latitude decimal NOT NULL,
    longitude decimal NOT NULL,
    Date_GMT varchar(20) NOT NULL,
    Time_GMT varchar(20) NOT NULL,
    SO2 float NULL,
    CO float NULL,
    PM10_mass float NULL,
    winds float NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE Units (
unit_id int NOT NULL AUTO_INCREMENT,
parameter_name varchar(32) NOT NULL,
unit varchar(32) NOT NULL,
PRIMARY KEY (unit_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE Gases_Weather_Join (
gases_record_id int NOT NULL AUTO_INCREMENT,
state_name varchar(20) NOT NULL,
county_name varchar(20) NOT NULL,
latitude float(8,6) NOT NULL,
longitude float(8,5) NOT NULL,
date_GMT DATE NOT NULL,
time_GMT timestamp NOT NULL,
pressure double NULL,
relative_humidity double NULL,
temperature double NULL,
winds double NULL,
CO double NULL,
CO_MDL double NULL,
SO2 double NULL,
SO2_MDL double NULL,
NO2 double NULL,
NO2_MDL double NULL,
ozone double NULL,
ozone_MDL double NULL,
GMT_year int NOT NULL,
GMT_month int NOT NULL,
GMT_day int NOT NULL,
PRIMARY KEY (gases_record_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE Gases_Weather_Join (
gases_record_id int NOT NULL AUTO_INCREMENT,
state_name varchar(20) NOT NULL,
county_name varchar(20) NOT NULL,
latitude float(8,6) NOT NULL,
longitude float(8,5) NOT NULL,
date_GMT DATE NOT NULL,
time_GMT int NOT NULL,
pressure double NULL,
relative_humidity double NULL,
temperature double NULL,
winds double NULL,
CO double NULL,
CO_MDL double NULL,
SO2 double NULL,
SO2_MDL double NULL,
NO2 double NULL,
NO2_MDL double NULL,
ozone double NULL,
ozone_MDL double NULL,
GMT_year int NOT NULL,
GMT_month int NOT NULL,
GMT_day int NOT NULL,
PRIMARY KEY (gases_record_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE Particulates_Weather_Join (
particulates_record_id int NOT NULL AUTO_INCREMENT,
state_name varchar(20) NOT NULL,
county_name varchar(20) NOT NULL,
latitude float(8,6) NOT NULL,
longitude float(8,5) NOT NULL,
date_GMT DATE NOT NULL,
time_GMT int NOT NULL,
pressure double NULL,
relative_humidity double NULL,
temperature double NULL,
winds double NULL,
PM10_mass double NULL,
PM10_mass_MDL double NULL,
PM2point5_FRM double NULL,
PM2point5_FRM_MDL double NULL,
PM2point5_nonFRM double NULL,
PM2point5_nonFRM_MDL double NULL,
GMT_year int NOT NULL,
GMT_month int NOT NULL,
GMT_day int NOT NULL,
PRIMARY KEY (particulates_record_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
