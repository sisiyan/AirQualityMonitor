USE airQualityWeather;

TRUNCATE TABLE final_Gases_Weather_Join_Daily;

INSERT INTO final_Gases_Weather_Join_Daily
SELECT * FROM Update_Gases_Weather_Join_Daily
ORDER BY gases_rec_id;

TRUNCATE TABLE final_Particulates_Weather_Join_Daily;

INSERT INTO final_Particulates_Weather_Join_Daily
SELECT * FROM Update_Particulates_Weather_Join_Daily
ORDER BY particulates_rec_id;

TRUNCATE TABLE Update_Gases_Weather_Join_Daily;
TRUNCATE TABLE Update_Particulates_Weather_Join_Daily;
