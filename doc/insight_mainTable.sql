use dbAirQualityWeather;

create table mainData (
	id int NOT NULL AUTO_INCREMENT,
	state_name varchar(20) NOT NULL,
    county_name varchar(20) NOT NULL,
	latitude decimal NOT NULL,
    longitude decimal NOT NULL,
    Date_GMT varchar(20) NOT NULL,
    Time_GMT varchar(20) NOT NULL,
    ozone float NULL,
    SO2 float NULL,
    NO2 float NULL,
    PM2_5_FRM float NULL,
    PM2_5_nonFRM float NULL,
    PM10_mass float NULL,
    PM2_5_speciation float NULL,
    PM10_speciation float NULL,
    winds float NULL,
    temperature float NULL,
    pressure float NULL,
    RH_dewpoint float NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;