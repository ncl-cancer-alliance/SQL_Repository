-- Pulls and cleans NCL Practice level screening data from the raw NHS Futures data.
-- Contact: jake.kealey@nhs.net
CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__SCREENING.SCREENING_LOCAL (
    "PRACTICE_CODE" VARCHAR,
    "PRACTICE_NAME" VARCHAR,
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
    "Organisation Code" AS "PRACTICE_CODE",
    "Organisation Name" AS "PRACTICE_NAME",
    "Programme" AS "PROGRAMME",
    --Convert Date string into Date object
    TO_DATE(
        '01 ' || LEFT("Month of Date", 3) || ' ' || RIGHT("Month of Date", 4), 
        'DD MON YYYY'
    ) AS "DATE_FULL",
    "Cohort Age Range" AS "COHORT_AGE_RANGE",
    "Cohort Description" AS "COHORT_DESCRIPTION",
    "Denominator Name" AS "DENOMINATOR_NAME",
    --Convert numeric fields to numeric type values
    TO_NUMBER(REPLACE("Denominator", ',', '')) AS "DENOMINATOR",
    TO_NUMBER(REPLACE("Numerator", ',', '')) AS "NUMERATOR",
    TO_NUMBER(REPLACE("Performance", '%', '')) / 100 AS "PERFORMANCE",
    TO_NUMBER(REPLACE("Acceptable", '%', '')) / 100 AS "ACCEPTABLE",
    TO_NUMBER(REPLACE("Achievable", '%', '')) / 100 AS "ACHIEVABLE"
FROM DATA_LAKE__NCL.CANCER__SCREENING.SCREENING_LOCAL;