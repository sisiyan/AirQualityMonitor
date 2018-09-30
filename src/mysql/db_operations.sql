USE airQualityWeather;

TRUNCATE TABLE Gases_Weather_Join_Daily_front;

INSERT INTO Gases_Weather_Join_Daily_front
SELECT * FROM Update_Gases_Weather_Join_Daily
ORDER BY gases_rec_id;

TRUNCATE TABLE Particulates_Weather_Join_Daily_front;

INSERT INTO Particulates_Weather_Join_Daily_front
SELECT * FROM Update_Particulates_Weather_Join_Daily
ORDER BY particulates_rec_id;
