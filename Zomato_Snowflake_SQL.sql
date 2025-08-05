create or replace stage manage_stage
url="s3://zomatxxx" 
CREDENTIALS= ( AWS_key_ID ='Axxxxxxxxxxxxxx'

AWS_Secret_Key= 'Uxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
desc stage manage_stage;
list @manage_stage;

CREATE OR REPLACE TABLE Zomato_Raw (
    url STRING,
    address STRING,
    name STRING,
    online_order STRING,
    book_table STRING,
    rate STRING,
    votes STRING,
    phone STRING,
    location STRING,
    rest_type STRING,
    dish_liked STRING,
    cuisines STRING,
    approx_cost STRING,
    reviews_list STRING,
    menu_item STRING,
    listed_in_type STRING,
    listed_in_city STRING
);

COPY INTO Zomato_Raw
FROM @manage_stage/zomato.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

SELECT * FROM Zomato_Raw;



CREATE OR REPLACE TABLE Dim_Restaurant (
  Restaurant_ID INT AUTOINCREMENT PRIMARY KEY,
  Name STRING,
  Address STRING,
  Online_Order STRING,
  Book_Table STRING,
  URL STRING
);
INSERT INTO Dim_Restaurant (Name, Address, Online_Order, Book_Table, URL)
SELECT DISTINCT Name, Address, Online_Order, Book_Table, URL
FROM Zomato_Raw;
SELECT * FROM Dim_Restaurant;

CREATE OR REPLACE TABLE Dim_Location (
  Location_ID INT AUTOINCREMENT PRIMARY KEY,
  Phone STRING
);

INSERT INTO Dim_Location (Phone)
SELECT DISTINCT Phone
FROM Zomato_Raw;
SELECT * FROM Dim_Location;

CREATE OR REPLACE TABLE Dim_Cuisine (
  Cuisine_ID INT AUTOINCREMENT PRIMARY KEY,
  Dishes_Liked STRING,
  Cuisines STRING
);

INSERT INTO Dim_Cuisine (Dishes_Liked, Cuisines)
SELECT DISTINCT Dish_Liked, Cuisines
FROM Zomato_Raw;
SELECT * FROM Dim_Cuisine;

CREATE OR REPLACE TABLE Dim_Type (
  Type_ID INT AUTOINCREMENT PRIMARY KEY,
  Listed_In_Type STRING
);
INSERT INTO Dim_Type (Listed_In_Type)
SELECT DISTINCT "LISTED_IN_TYPE"
FROM Zomato_Raw;
SELECT * FROM Dim_Type;

CREATE OR REPLACE TABLE Fact_Restaurant (
  Fact_ID INT AUTOINCREMENT PRIMARY KEY,
  Restaurant_ID INT,
  Location_ID INT,
  Cuisine_ID INT,
  Type_ID INT,
  Cost_For_Two INT,
  Rating FLOAT,
  Votes INT
);
INSERT INTO Fact_Restaurant (
  Restaurant_ID,
  Location_ID,
  Cuisine_ID,
  Type_ID,
  Cost_For_Two,
  Rating,
  Votes
)
SELECT
  Dim_Restaurant.Restaurant_ID,
  Dim_Location.Location_ID,
  Dim_Cuisine.Cuisine_ID,
  Dim_Type.Type_ID,
  TRY_TO_NUMBER(Zomato_Raw.APPROX_COST),
  TRY_TO_NUMBER(SPLIT_PART(Zomato_Raw.RATE, '/', 1)),
  TRY_TO_NUMBER(Zomato_Raw.VOTES)
FROM Zomato_Raw
JOIN Dim_Restaurant
  ON TRIM(LOWER(Zomato_Raw.NAME)) = TRIM(LOWER(Dim_Restaurant.Name))
  AND TRIM(LOWER(Zomato_Raw.URL)) = TRIM(LOWER(Dim_Restaurant.URL))
JOIN Dim_Location
  ON TRIM(Zomato_Raw.PHONE) = TRIM(Dim_Location.Phone)
JOIN Dim_Cuisine
  ON TRIM(Zomato_Raw.DISH_LIKED) = TRIM(Dim_Cuisine.Dishes_Liked)
  AND TRIM(Zomato_Raw.CUISINES) = TRIM(Dim_Cuisine.Cuisines)
JOIN Dim_Type
  ON TRIM(Zomato_Raw.LISTED_IN_TYPE) = TRIM(Dim_Type.Listed_In_Type);

-- Check phone mismatch
SELECT DISTINCT Phone 
FROM Zomato_Raw
WHERE TRIM(Phone) NOT IN (
  SELECT TRIM(Phone) FROM Dim_Location
);

-- Check dish + cuisine mismatch
SELECT DISTINCT Dish_Liked, Cuisines 
FROM Zomato_Raw
WHERE (Dish_Liked, Cuisines) NOT IN (
  SELECT Dishes_Liked, Cuisines FROM Dim_Cuisine
);

-- Check Listed_in_Type mismatch
SELECT DISTINCT Listed_In_Type 
FROM Zomato_Raw
WHERE TRIM(LOWER(Listed_In_Type)) NOT IN (
  SELECT TRIM(LOWER(Listed_In_Type)) FROM Dim_Type
);


SELECT DISTINCT Zomato_Raw.NAME, Zomato_Raw.URL
FROM Zomato_Raw
WHERE NOT EXISTS (
  SELECT 1 FROM Dim_Restaurant
  WHERE TRIM(LOWER(Zomato_Raw.NAME)) = TRIM(LOWER(Dim_Restaurant.Name))
    AND TRIM(LOWER(Zomato_Raw.URL)) = TRIM(LOWER(Dim_Restaurant.URL))
);
-- 🔍 Check if Phone is missing in Dim_Location

SELECT DISTINCT TRIM(PHONE) AS raw_phone FROM Zomato_Raw;
SELECT DISTINCT TRIM(Phone) AS dim_phone FROM Dim_Location;


SELECT DISTINCT DISH_LIKED, CUISINES FROM Zomato_Raw
WHERE NOT EXISTS (
  SELECT 1 FROM Dim_Cuisine
  WHERE TRIM(Zomato_Raw.DISH_LIKED) = TRIM(Dim_Cuisine.Dishes_Liked)
    AND TRIM(Zomato_Raw.CUISINES) = TRIM(Dim_Cuisine.Cuisines)
);

INSERT INTO Dim_Type (Listed_In_Type)
SELECT DISTINCT TRIM(LISTED_IN_TYPE)
FROM Zomato_Raw
WHERE TRIM(LOWER(LISTED_IN_TYPE)) NOT IN (
  SELECT TRIM(LOWER(Listed_In_Type)) FROM Dim_Type
);
SELECT DISTINCT LISTED_IN_TYPE FROM Zomato_Raw;
SELECT DISTINCT Listed_In_Type FROM Dim_Type;

-- Use cases--
-- 1.Top Rated Restaurants
-- 2.Top Cuisines in Each City based on orders
-- 3.Top 5 Dishes That Changed Popularity by Type(Buffet vs Delivery)
-- 4.Average Cost per Restaurant Type
-- 5.Top Budget-Friendly Cuisines
-- Find the number of restaturnat who are delivery online or offline and their respective check if number is missing 
-- Restaurants Offering Both Services
-- Dishes with average cost who offer only NORTH INDIAN AND SOUTH INDIAN FOODS
-- Recently Added Restaurant_IDs IN LAST 3 MMONTH RESPECTIVELY

INSERT INTO Fact_Restaurant (
  Restaurant_ID,
  Location_ID,
  Cuisine_ID,
  Type_ID,
  Cost_For_Two,
  Rating,
  Votes
)
SELECT
  R.Restaurant_ID,
  L.Location_ID,
  C.Cuisine_ID,
  T.Type_ID,
  TRY_TO_NUMBER(Z.APPROX_COST),
  TRY_TO_NUMBER(SPLIT_PART(Z.RATE, '/', 1)),
  TRY_TO_NUMBER(Z.VOTES)
FROM Zomato_Raw Z
LEFT JOIN Dim_Restaurant R
  ON TRIM(LOWER(Z.Name)) = TRIM(LOWER(R.Name))
  AND TRIM(LOWER(Z.URL)) = TRIM(LOWER(R.URL))
LEFT JOIN Dim_Location L
  ON TRIM(Z.Phone) = TRIM(L.Phone)
LEFT JOIN Dim_Cuisine C
  ON TRIM(Z.Dish_Liked) = TRIM(C.Dishes_Liked)
  AND TRIM(Z.Cuisines) = TRIM(C.Cuisines)
LEFT JOIN Dim_Type T
  ON TRIM(Z.Listed_In_Type) = TRIM(T.Listed_In_Type)
WHERE
  Z.Name IS NOT NULL;
SELECT 
  R.Name,
  F.Rating,
  F.Votes
FROM Fact_Restaurant F
JOIN Dim_Restaurant R ON F.Restaurant_ID = R.Restaurant_ID
WHERE F.Rating IS NOT NULL
ORDER BY F.Rating DESC, F.Votes DESC
LIMIT 10;


INSERT INTO Dim_Restaurant (Name, Address, Online_Order, Book_Table, URL)
SELECT DISTINCT Name, Address, Online_Order, Book_Table, URL
FROM Zomato_Raw;

INSERT INTO Dim_Location (Phone)
SELECT DISTINCT Phone FROM Zomato_Raw;

INSERT INTO Dim_Cuisine (Dishes_Liked, Cuisines)
SELECT DISTINCT Dish_Liked, Cuisines FROM Zomato_Raw;

INSERT INTO Dim_Type (Listed_In_Type)
SELECT DISTINCT Listed_In_Type FROM Zomato_Raw;
-- 
INSERT INTO Fact_Restaurant (
  Restaurant_ID,
  Location_ID,
  Cuisine_ID,
  Type_ID,
  Cost_For_Two,
  Rating,
  Votes
)
SELECT
  R.Restaurant_ID,
  L.Location_ID,
  C.Cuisine_ID,
  T.Type_ID,
  TRY_TO_NUMBER(Z.APPROX_COST),
  TRY_TO_NUMBER(SPLIT_PART(Z.RATE, '/', 1)),
  TRY_TO_NUMBER(Z.VOTES)
FROM Zomato_Raw Z
JOIN Dim_Restaurant R 
  ON TRIM(LOWER(Z.Name)) = TRIM(LOWER(R.Name)) 
  AND TRIM(LOWER(Z.URL)) = TRIM(LOWER(R.URL))
JOIN Dim_Location L ON TRIM(Z.Phone) = TRIM(L.Phone)
JOIN Dim_Cuisine C ON TRIM(Z.Dish_Liked) = TRIM(C.Dishes_Liked) AND TRIM(Z.Cuisines) = TRIM(C.Cuisines)
JOIN Dim_Type T ON TRIM(Z.Listed_In_Type) = TRIM(T.Listed_In_Type);
-- 1. Top rated rest
SELECT 
  R.Name, R.Address, F.Rating, F.Votes
FROM Fact_Restaurant F
JOIN Dim_Restaurant R ON F.Restaurant_ID = R.Restaurant_ID
WHERE F.Rating IS NOT NULL
ORDER BY F.Rating DESC, F.Votes DESC
LIMIT 10;


-- 2.Create temp table to hold split cuisines per city
SELECT 
  TRIM(LOWER(f.value::STRING)) AS Cuisine,
  LISTED_IN_CITY AS City
FROM Zomato_Raw,
LATERAL FLATTEN(input => SPLIT(CUISINES, ',')) f
WHERE CUISINES IS NOT NULL 
  AND LISTED_IN_CITY IS NOT NULL;

-- 3.Top Cuisines in Each City based on orders
SELECT 
  TRIM(LOWER(f.value::STRING)) AS Cuisine,
  LISTED_IN_CITY AS City,
  COUNT(*) AS Total_Orders
FROM Zomato_Raw,
LATERAL FLATTEN(input => SPLIT(CUISINES, ',')) f
WHERE CUISINES IS NOT NULL
  AND LISTED_IN_CITY IS NOT NULL
GROUP BY LISTED_IN_CITY, TRIM(LOWER(f.value::STRING))
ORDER BY LISTED_IN_CITY, Total_Orders DESC;

-- 4. Average Cost per Restaurant Type
SELECT 
  LISTED_IN_TYPE AS Restaurant_Type,
  ROUND(AVG(TRY_TO_NUMBER(APPROX_COST)), 2) AS Avg_Cost_For_Two
FROM Zomato_Raw
WHERE APPROX_COST IS NOT NULL 
  AND TRY_TO_NUMBER(APPROX_COST) IS NOT NULL
GROUP BY LISTED_IN_TYPE
ORDER BY Avg_Cost_For_Two ASC;

-- 5.Top Budget-Friendly Cuisines
SELECT
  TRIM(cuisine.value) AS cuisine,
  ROUND(AVG(TRY_TO_NUMBER(REPLACE(approx_cost, ',', ''))), 2) AS avg_cost
FROM Zomato_Raw,
LATERAL FLATTEN(INPUT => SPLIT(cuisines, ',')) AS cuisine
WHERE approx_cost IS NOT NULL
GROUP BY cuisine
ORDER BY avg_cost ASC;

-- 6.

SELECT 
  COUNT(*) AS total_delivery_restaurants
FROM Zomato_Raw
WHERE LOWER(online_order) = 'yes' OR LOWER(book_table) = 'yes';
SELECT 
  COUNT(*) AS delivery_with_missing_phone
FROM Zomato_Raw
WHERE (LOWER(online_order) = 'yes' OR LOWER(book_table) = 'yes')
  AND (phone IS NULL OR TRIM(phone) = '' OR phone ILIKE '%not%available%');
SELECT
  COUNT(*) AS total_delivery_restaurants,
  COUNT_IF(phone IS NULL OR TRIM(phone) = '' OR phone ILIKE '%not%available%') AS missing_phone_count
FROM Zomato_Raw
WHERE LOWER(online_order) = 'yes' OR LOWER(book_table) = 'yes';


-- 7. Restaurants Offering Both Services
SELECT 
  name,
  address,
  online_order,
  book_table,
  cuisines,
  approx_cost
FROM Zomato_Raw
WHERE LOWER(TRIM(online_order)) = 'yes'
  AND LOWER(TRIM(book_table)) = 'yes';

-- 8.  Dishes with average cost who offer only NORTH INDIAN AND SOUTH INDIAN FOODS
SELECT 
  cuisines,
  ROUND(AVG(TRY_TO_NUMBER(approx_cost)), 2) AS avg_cost_for_two
FROM Zomato_Raw
WHERE 
  (LOWER(TRIM(cuisines)) = 'north indian'
   OR LOWER(TRIM(cuisines)) = 'south indian')
GROUP BY cuisines;

-- 9. Recently Added Restaurant_IDs IN LAST 3 MMONTH RESPECTIVELY
ALTER TABLE Dim_Restaurant
ADD COLUMN Created_At TIMESTAMP;
UPDATE Dim_Restaurant
SET Created_At = CURRENT_TIMESTAMP()
WHERE Created_At IS NULL;
INSERT INTO Dim_Restaurant (Name, Address, Online_Order, Book_Table, URL, Created_At)
SELECT DISTINCT Name, Address, Online_Order, Book_Table, URL, CURRENT_TIMESTAMP()
FROM Zomato_Raw;
SELECT 
  Restaurant_ID,
  Name,
  Address,
  Created_At
FROM Dim_Restaurant
WHERE Created_At >= DATEADD(MONTH, -3, CURRENT_TIMESTAMP())
ORDER BY Created_At DESC;



--10. Top Cities with Highest Number of Restaurants
SELECT 
  LISTED_IN_CITY AS City,
  COUNT(*) AS Total_Restaurants
FROM Zomato_Raw
WHERE LISTED_IN_CITY IS NOT NULL
GROUP BY LISTED_IN_CITY
ORDER BY Total_Restaurants DESC;















COPY INTO @manage_stage/top_rated_restaurants.csv
FROM (
  SELECT 
    R.Name,
    R.Address,
    F.Rating,
    F.Votes
  FROM Fact_Restaurant F
  JOIN Dim_Restaurant R ON F.Restaurant_ID = R.Restaurant_ID
  WHERE F.Rating IS NOT NULL
  ORDER BY F.Rating DESC, F.Votes DESC
  LIMIT 10
)
FILE_FORMAT = (
  TYPE = CSV 
  FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
  COMPRESSION = NONE
)
OVERWRITE = TRUE;
LIST @manage_stage;
COPY INTO @manage_stage/top_budget_cuisines.csv
FROM (
  SELECT
    TRIM(cuisine.value) AS cuisine,
    ROUND(AVG(TRY_TO_NUMBER(REPLACE(approx_cost, ',', ''))), 2) AS avg_cost
  FROM Zomato_Raw,
  LATERAL FLATTEN(INPUT => SPLIT(cuisines, ',')) AS cuisine
  WHERE approx_cost IS NOT NULL
  GROUP BY cuisine
  ORDER BY avg_cost ASC
)
FILE_FORMAT = (
  TYPE = CSV 
  FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
  COMPRESSION = NONE
)
OVERWRITE = TRUE;
LIST @manage_stage;





-- aur aise hi baaki jo part files hain

-- s3://zomatoooo2/top_budget_cuisines.csv_0_3_0.csv

CREATE OR REPLACE TABLE zomato_use_cases (
  Use_Case STRING,
  Column1 STRING,
  Column2 STRING,
  Column3 STRING,
  Column4 STRING,
  Column5 STRING
);
-- 1. Top Rated Restaurants
INSERT INTO zomato_use_cases
SELECT 'Top Rated Restaurants', R.Name, R.Address, F.Rating::STRING, F.Votes::STRING, NULL
FROM Fact_Restaurant F
JOIN Dim_Restaurant R ON F.Restaurant_ID = R.Restaurant_ID
WHERE F.Rating IS NOT NULL
ORDER BY F.Rating DESC, F.Votes DESC
LIMIT 10;

-- 2. Top Cuisines in Each City
INSERT INTO zomato_use_cases
SELECT 'Top Cuisines per City', LISTED_IN_CITY, TRIM(LOWER(f.value::STRING)), COUNT(*)::STRING, NULL, NULL
FROM Zomato_Raw,
LATERAL FLATTEN(input => SPLIT(CUISINES, ',')) f
WHERE CUISINES IS NOT NULL AND LISTED_IN_CITY IS NOT NULL
GROUP BY LISTED_IN_CITY, TRIM(LOWER(f.value::STRING));

-- 3. Average Cost per Restaurant Type
INSERT INTO zomato_use_cases
SELECT 'Avg Cost per Type', LISTED_IN_TYPE, ROUND(AVG(TRY_TO_NUMBER(APPROX_COST)), 2)::STRING, NULL, NULL, NULL
FROM Zomato_Raw
WHERE APPROX_COST IS NOT NULL AND TRY_TO_NUMBER(APPROX_COST) IS NOT NULL
GROUP BY LISTED_IN_TYPE;

-- 4. Budget-Friendly Cuisines
INSERT INTO zomato_use_cases
SELECT 'Budget Cuisines', TRIM(cuisine.value), ROUND(AVG(TRY_TO_NUMBER(REPLACE(approx_cost, ',', ''))), 2)::STRING, NULL, NULL, NULL
FROM Zomato_Raw,
LATERAL FLATTEN(INPUT => SPLIT(cuisines, ',')) AS cuisine
WHERE approx_cost IS NOT NULL
GROUP BY cuisine.value;

-- 5. Delivery & Missing Phones
INSERT INTO zomato_use_cases
SELECT 'Delivery & Missing Phones', COUNT(*)::STRING,
COUNT_IF(phone IS NULL OR TRIM(phone) = '' OR phone ILIKE '%not%available%')::STRING,
NULL, NULL, NULL
FROM Zomato_Raw
WHERE LOWER(online_order) = 'yes' OR LOWER(book_table) = 'yes';

-- 6. Restaurants with Both Services
INSERT INTO zomato_use_cases
SELECT 'Both Services', name, address, online_order, book_table, approx_cost
FROM Zomato_Raw
WHERE LOWER(TRIM(online_order)) = 'yes' AND LOWER(TRIM(book_table)) = 'yes';

-- 7. North/South Indian Only
INSERT INTO zomato_use_cases
SELECT 'North/South Indian', cuisines, ROUND(AVG(TRY_TO_NUMBER(approx_cost)), 2)::STRING, NULL, NULL, NULL
FROM Zomato_Raw
WHERE LOWER(TRIM(cuisines)) IN ('north indian', 'south indian')
GROUP BY cuisines;

-- 8. Recently Added Restaurants
INSERT INTO zomato_use_cases
SELECT 'Recently Added Restaurants', Name, Address, Created_At::STRING, NULL, NULL
FROM Dim_Restaurant
WHERE Created_At >= DATEADD(MONTH, -3, CURRENT_TIMESTAMP());

-- 9. Top Cities by Restaurant Count
INSERT INTO zomato_use_cases
SELECT 'Top Cities by Restaurant Count', LISTED_IN_CITY, COUNT(*)::STRING, NULL, NULL, NULL
FROM Zomato_Raw
WHERE LISTED_IN_CITY IS NOT NULL
GROUP BY LISTED_IN_CITY;
-- 10. Top Cities with Highest Number of Restaurants
INSERT INTO zomato_use_cases
SELECT 
  'Top Cities with Most Restaurants' AS Use_Case,
  LISTED_IN_CITY AS Column1,
  COUNT(*)::STRING AS Column2,
  NULL AS Column3,
  NULL AS Column4,
  NULL AS Column5
FROM Zomato_Raw
WHERE LISTED_IN_CITY IS NOT NULL
GROUP BY LISTED_IN_CITY
ORDER BY COUNT(*) DESC;

COPY INTO @manage_stage/zomato.csv
FROM zomato_use_cases
FILE_FORMAT = (FORMAT_NAME = my_csv_format)
SINGLE = TRUE
OVERWRITE = TRUE;







COPY INTO @manage_stage/all_exports/zomato_raw
FROM Zomato_Raw
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = 'NONE'
)
OVERWRITE = TRUE;

-- 📤 Export Dimensions
COPY INTO @manage_stage/all_exports/dim_restaurant FROM Dim_Restaurant FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = 'NONE') OVERWRITE = TRUE;
COPY INTO @manage_stage/all_exports/dim_location   FROM Dim_Location   FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = 'NONE') OVERWRITE = TRUE;
COPY INTO @manage_stage/all_exports/dim_cuisine     FROM Dim_Cuisine     FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = 'NONE') OVERWRITE = TRUE;
COPY INTO @manage_stage/all_exports/dim_type        FROM Dim_Type        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = 'NONE') OVERWRITE = TRUE;

-- 📤 Export Fact Table
COPY INTO @manage_stage/all_exports/fact_restaurant
FROM Fact_Restaurant
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = 'NONE'
)
OVERWRITE = TRUE;

-- 📤 Export Use Case: Top Rated Restaurants
COPY INTO @manage_stage/all_exports/top_rated_restaurants
FROM (
  SELECT R.Name, R.Address, F.Rating, F.Votes
  FROM Fact_Restaurant F
  JOIN Dim_Restaurant R ON F.Restaurant_ID = R.Restaurant_ID
  WHERE F.Rating IS NOT NULL
  ORDER BY F.Rating DESC, F.Votes DESC
  LIMIT 10
)
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = 'NONE'
)
OVERWRITE = TRUE;

-- 📤 Export Use Case: Top Cuisines by City
COPY INTO @manage_stage/all_exports/top_cuisines_by_city
FROM (
  SELECT LISTED_IN_CITY AS City, TRIM(LOWER(f.value::STRING)) AS Cuisine, COUNT(*) AS Total_Orders
  FROM Zomato_Raw,
  LATERAL FLATTEN(input => SPLIT(CUISINES, ',')) f
  WHERE CUISINES IS NOT NULL AND LISTED_IN_CITY IS NOT NULL
  GROUP BY LISTED_IN_CITY, TRIM(LOWER(f.value::STRING))
)
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = 'NONE'
)
OVERWRITE = TRUE;

-- 📤 Export Use Case: Average Cost by Type
COPY INTO @manage_stage/all_exports/avg_cost_by_type
FROM (
  SELECT LISTED_IN_TYPE, ROUND(AVG(TRY_TO_NUMBER(APPROX_COST)), 2) AS Avg_Cost_For_Two
  FROM Zomato_Raw
  WHERE APPROX_COST IS NOT NULL AND TRY_TO_NUMBER(APPROX_COST) IS NOT NULL
  GROUP BY LISTED_IN_TYPE
)
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = 'NONE'
)
OVERWRITE = TRUE;

-- 📤 Export Use Case: Budget-Friendly Cuisines
COPY INTO @manage_stage/all_exports/budget_friendly_cuisines
FROM (
  SELECT TRIM(cuisine.value) AS Cuisine, ROUND(AVG(TRY_TO_NUMBER(REPLACE(approx_cost, ',', ''))), 2) AS Avg_Cost
  FROM Zomato_Raw,
  LATERAL FLATTEN(INPUT => SPLIT(cuisines, ',')) AS cuisine
  WHERE approx_cost IS NOT NULL
  GROUP BY cuisine.value
)
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = 'NONE'
)
OVERWRITE = TRUE;

-- 📤 Export Use Case: Top Cities by Restaurant Count
COPY INTO @manage_stage/all_exports/top_cities_by_restaurant_count
FROM (
  SELECT LISTED_IN_CITY AS City, COUNT(*) AS Total_Restaurants
  FROM Zomato_Raw
  WHERE LISTED_IN_CITY IS NOT NULL
  GROUP BY LISTED_IN_CITY
  ORDER BY Total_Restaurants DESC
)
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = 'NONE'
)
OVERWRITE = TRUE;



