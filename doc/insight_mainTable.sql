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