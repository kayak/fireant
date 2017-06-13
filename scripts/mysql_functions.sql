/*
Implements the Vertica TRUNC function in MySQL

Valid formats:
  - hour - To the start of the hour
  - day - To the start of the day (00:00:00)
  - week - To the monday in the week
  - month - To the start of the month
  - quarter - To the start of the quarter
  - year - To the start of the year

Called with TRUNC(<date>, <format required>) -> returns a string


Note: A new database/schema will be created called 'dashmore' which will store the new function. All database users that
Dashmore will use to query data with need to have permissions to execute this function.

To give permissions, use the following query for each MySQL user you want to use to query data through Dashmore:

  GRANT EXECUTE ON FUNCTION dashmore.TRUNC to '<user>'@'<host>';

*/

DELIMITER $$

CREATE DATABASE IF NOT EXISTS dashmore DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci'$$

DROP FUNCTION IF EXISTS dashmore.TRUNC$$

CREATE FUNCTION dashmore.TRUNC(DT  DATETIME,
                      FMT ENUM ('hour', 'day', 'week', 'month', 'quarter', 'year')) RETURNS NVARCHAR(19)

  BEGIN
    DECLARE NEW_DT NVARCHAR(19);

    CASE FMT
      WHEN 'hour'
      THEN SET NEW_DT = DATE_FORMAT(DT, '%Y-%m-%d %H:00:00');
      WHEN 'day'
      THEN SET NEW_DT = DATE_FORMAT(DT, '%Y-%m-%d 00:00:00');
      WHEN 'week'
      THEN SET NEW_DT = DATE_FORMAT(DATE_SUB(DT, INTERVAL WEEKDAY(DT) DAY), '%Y-%m-%d 00:00:00');
      WHEN 'month'
      THEN SET NEW_DT = DATE_FORMAT(DT, '%Y-%m-01');
      WHEN 'quarter'
      THEN SET NEW_DT = MAKEDATE(YEAR(TIMESTAMP(DT)), 1) + INTERVAL QUARTER(TIMESTAMP(DT)) QUARTER - INTERVAL 1 QUARTER;
      WHEN 'year'
      THEN SET NEW_DT = DATE_FORMAT(DT, '%Y-01-01');
    ELSE SET NEW_DT = DT;
    END CASE;

    RETURN NEW_DT;
  END$$

DELIMITER ;


