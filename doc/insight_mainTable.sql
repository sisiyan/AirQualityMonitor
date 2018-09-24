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
unit_id int NOT NULL AUTO_INCREMENT,
state_name varchar(20) NOT NULL,
county_name varchar(20) NOT NULL,
latitude float(8,6) NOT NULL,
longitude float(8,5) NOT NULL,
Date_GMT DATE NOT NULL,
Time_GMT TIME NOT NULL,

PRIMARY KEY (unit_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
