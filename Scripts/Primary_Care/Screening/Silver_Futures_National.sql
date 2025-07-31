-- Creates the national screening table using the raw NHS Futures data.
-- Processes London and England (via aggregation) seperately and combines both into a single table.
-- Contact: jake.kealey@nhs.net
CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__SCREENING.SCREENING_NATIONAL (
    "REGION_NAME" VARCHAR,
    "PROGRAMME" VARCHAR,
    "DATE_FULL" DATE,
    "COHORT_AGE_RANGE" VARCHAR,
    "COHORT_DESCRIPTION" VARCHAR,
    "DENOMINATOR_NAME" VARCHAR,
    "DENOMINATOR" NUMBER,
    "NUMERATOR" NUMBER,
    "PERFORMANCE" FLOAT,
    "ACCEPTABLE" FLOAT,
    "ACHIEVABLE" FLOAT
)
COMMENT="Dynamic table to clean source cancer screening data from NHS Futures."
TARGET_LAG = "2 hours"
REFRESH_MODE = INCREMENTAL
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

SELECT
    "Organisation Name" AS "REGION_NAME",
    "Programme" AS "PROGRAMME",
    --Convert Date string into Date object
    TO_DATE(
        '01 ' || LEFT("Month of Date", 3) || ' ' || RIGHT("Month of Date", 4), 
        'DD MON YYYY'
    ) AS "DATE_FULL",
    "Cohort Age Range" AS "COHORT_AGE_RANGE",
    "Cohort Description" AS "COHORT_DESCRIPTION",
    "Denominator Name" AS "DENOMINATOR_NAME",
    "Denominator" AS "DENOMINATOR",
    "Numerator" AS "NUMERATOR",
    "Performance" AS "PERFORMANCE",
    TO_NUMBER(REPLACE("Acceptable", '%', '')) / 100 AS "ACCEPTABLE",
    TO_NUMBER(REPLACE("Achievable", '%', '')) / 100 AS "ACHIEVABLE"
    
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