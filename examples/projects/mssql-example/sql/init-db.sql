CREATE DATABASE sensordb;
GO
USE sensordb;
GO
CREATE TABLE sensor_readings (
    id INT IDENTITY(1,1) PRIMARY KEY,
    sensor_name NVARCHAR(100) NOT NULL,
    temperature FLOAT NOT NULL,
    humidity FLOAT NOT NULL,
    recorded_at DATETIME2(6) DEFAULT SYSDATETIME()
);
GO
INSERT INTO sensor_readings (sensor_name, temperature, humidity) VALUES
    (N'sensor_A', 22.5, 45.0),
    (N'sensor_B', 23.1, 42.3),
    (N'sensor_A', 22.8, 44.7),
    (N'sensor_B', 23.5, 41.9),
    (N'sensor_C', 21.0, 50.2);
GO
