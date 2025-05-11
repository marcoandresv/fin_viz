GRANT ALL PRIVILEGES ON economic_dashboard.* TO 'root'@'%';
FLUSH PRIVILEGES;

SHOW GRANTS FOR 'root'@'%';


USE economic_dashboard;

SELECT *
FROM sp500;

SELECT *
FROM sp500
WHERE `S&P 500 Index` IS NULL;

describe sp500;

-------------------------------------------
-- FILLING-IN NULL VALUES
-------------------------------------------

-- STEP 1: preview NULL values
SELECT 
  t.`DATE`,
  t.`S&P 500 Index`,
  (
    SELECT t2.`S&P 500 Index`
    FROM sp500 t2
    WHERE t2.`DATE` < t.`DATE` AND t2.`S&P 500 Index` IS NOT NULL
    ORDER BY t2.`DATE` DESC
    LIMIT 1
  ) AS prev_value,
  (
    SELECT t3.`S&P 500 Index`
    FROM sp500 t3
    WHERE t3.`DATE` > t.`DATE` AND t3.`S&P 500 Index` IS NOT NULL
    ORDER BY t3.`DATE` ASC
    LIMIT 1
  ) AS next_value,
  (
    (
      (SELECT t2.`S&P 500 Index`
       FROM sp500 t2
       WHERE t2.`DATE` < t.`DATE` AND t2.`S&P 500 Index` IS NOT NULL
       ORDER BY t2.`DATE` DESC
       LIMIT 1
      )
      +
      (SELECT t3.`S&P 500 Index`
       FROM sp500 t3
       WHERE t3.`DATE` > t.`DATE` AND t3.`S&P 500 Index` IS NOT NULL
       ORDER BY t3.`DATE` ASC
       LIMIT 1
      )
    ) / 2
  ) AS filled_value
FROM sp500 t
WHERE t.`S&P 500 Index` IS NULL;

-- STEP 2: update
UPDATE sp500 t
SET t.`S&P 500 Index` = (
  (
    (SELECT t2.`S&P 500 Index`
     FROM sp500 t2
     WHERE t2.`DATE` < t.`DATE` AND t2.`S&P 500 Index` IS NOT NULL
     ORDER BY t2.`DATE` DESC
     LIMIT 1
    )
    +
    (SELECT t3.`S&P 500 Index`
     FROM sp500 t3
     WHERE t3.`DATE` > t.`DATE` AND t3.`S&P 500 Index` IS NOT NULL
     ORDER BY t3.`DATE` ASC
     LIMIT 1
    )
  ) / 2
)
WHERE t.`S&P 500 Index` IS NULL;

-------------------------------------------
-------------------------------------------

Select * from sp500;

SELECT *
FROM cpiaucsl;

SELECT *
FROM fedfunds;

SELECT *
FROM gdp;

SELECT *
FROM indpro;

SELECT *
FROM pce;



