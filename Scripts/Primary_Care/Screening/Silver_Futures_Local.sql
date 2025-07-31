-- Pulls and cleans NCL Practice level screening data from the raw NHS Futures data.
-- Contact: jake.kealey@nhs.net
CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__SCREENING.SCREENING_LOCAL (
    "Practice_Code" VARCHAR,
    "Practice_Name" VARCHAR,
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
    "Organisation Code" AS "Practice_Code",
    "Organisation Name" AS "Practice_Name",
    "Programme",
    --Convert Date string into Date object
    TO_DATE(
        '01 ' || LEFT("Month of Date", 3) || ' ' || RIGHT("Month of Date", 4), 
        'DD MON YYYY'
    ) AS "Date_Full",
    "Cohort Age Range" AS "Cohort_Age_Range",
    "Cohort Description" AS "Cohort_Description",
    "Denominator Name" AS "Denominator_Name",
    --Convert numeric fields to numeric type values
    TO_NUMBER(REPLACE("Denominator", ',', '')) AS "Denominator",
    TO_NUMBER(REPLACE("Numerator", ',', '')) AS "Numerator",
    TO_NUMBER(REPLACE("Performance", '%', '')) / 100 AS "Performance",
    TO_NUMBER(REPLACE("Acceptable", '%', '')) / 100 AS "Acceptable",
    TO_NUMBER(REPLACE("Achievable", '%', '')) / 100 AS "Achievable"
FROM DATA_LAKE.CANCER__SCREENING.SCREENING_LOCAL;