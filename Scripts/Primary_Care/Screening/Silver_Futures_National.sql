-- Creates the national screening table using the raw NHS Futures data.
-- Processes London and England (via aggregation) seperately and combines both into a single table.
-- Contact: jake.kealey@nhs.net
CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__SCREENING.SCREENING_NATIONAL (
    "Region_Name" VARCHAR,
    "Programme" VARCHAR,
    "Date_Full" DATE,
    "Cohort_Age_Range" VARCHAR,
    "Cohort_Description" VARCHAR,
    "Denominator_Name" VARCHAR,
    "Denominator" NUMBER,
    "Numerator" NUMBER,
    "Performance" FLOAT,
    "Acceptable" FLOAT,
    "Achievable" FLOAT
)
COMMENT="Dynamic table to clean source cancer screening data from NHS Futures."
TARGET_LAG = "2 hours"
REFRESH_MODE = INCREMENTAL
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

SELECT
    "Organisation Name" AS "Region_Name",
    "Programme",
    --Convert Date string into Date object
    TO_DATE(
        '01 ' || LEFT("Month of Date", 3) || ' ' || RIGHT("Month of Date", 4), 
        'DD MON YYYY'
    ) AS "Date_Full",
    "Cohort Age Range" AS "Cohort_Age_Range",
    "Cohort Description" AS "Cohort_Description",
    "Denominator Name" AS "Denominator_Name",
    "Denominator",
    "Numerator",
    "Performance",
    TO_NUMBER(REPLACE("Acceptable", '%', '')) / 100 AS "Acceptable",
    TO_NUMBER(REPLACE("Achievable", '%', '')) / 100 AS "Achievable"
    
--Process the England and London data seperately
FROM (
    --England data
    SELECT
        'England' AS "Organisation Name",
        "Programme",
        "Month of Date",
        "Cohort Age Range",
        "Cohort Description",
        "Denominator Name",
        --Aggregate figures across all regions
        SUM(TO_NUMBER(REPLACE("Denominator", ',', ''))) AS "Denominator",
        SUM(TO_NUMBER(REPLACE("Numerator", ',', ''))) AS "Numerator",
        --Use DIV0NULL to account for rows where the Denominator is 0
        DIV0NULL(
            SUM(TO_NUMBER(REPLACE("Numerator", ',', ''))),
            SUM(TO_NUMBER(REPLACE("Denominator", ',', '')))
        ) AS "Performance",
        "Acceptable",
        "Achievable"
    FROM DATA_LAKE.CANCER__SCREENING.SCREENING_NATIONAL
    GROUP BY ALL

    UNION ALL

    --London data
    SELECT
        "Organisation Name",
        "Programme",
        "Month of Date",
        "Cohort Age Range",
        "Cohort Description",
        "Denominator Name",
        TO_NUMBER(REPLACE("Denominator", ',', '')) AS "Denominator",
        TO_NUMBER(REPLACE("Numerator", ',', '')) AS "Numerator",
        TO_NUMBER(REPLACE("Performance", '%', '')) / 100 AS "Performance",
        "Acceptable",
        "Achievable"
    FROM DATA_LAKE.CANCER__SCREENING.SCREENING_NATIONAL
    WHERE "Organisation Name" = 'London'
)