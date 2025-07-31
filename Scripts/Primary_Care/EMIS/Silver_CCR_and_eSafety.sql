-- This pulls raw data on eSafety Netting and Cancer Care Review metrics and combines them into a single table
-- Contact: jake.kealey@nhs.net

CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__PRIMARY_CARE.SAFETY_NETTING_AND_CCR(
    "Indicator_Name",
    "Practice_Name",
    "Practice_Code",
    "Numerator",
    "Denominator",
    "Date_Full"
)
TARGET_LAG = '2 hours'
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Dynamic table to combine CCR and eSafety source data'
AS

SELECT 
    "Indicator_Name",
    "Organisation" AS "Practice_Name",
    "CDB" AS "Practice_Code",
    "Population Count" AS "Numerator",
    "Parent" AS "Denominator",
    --Convert Date string into Date object
     DATE_FROM_PARTS("_Year", "_Month", 1) AS "Date_Full"

--For each data source, add a "Indicator_Name" and combine the datasets
FROM (
    SELECT 'CAN004 - Cancer Care Review within 12 months' AS "Indicator_Name", * 
    FROM DATA_LAKE.CANCER__EMIS.CCR_CAN004
    
    UNION ALL
    
    SELECT 'CAN005 - Cancer support offered within 3 months' AS "Indicator_Name", * 
    FROM DATA_LAKE.CANCER__EMIS.CCR_CAN005
    
    UNION ALL
    
    SELECT 'USC referrals safety netted via e-safety netting tool' AS "Indicator_Name", * 
    FROM DATA_LAKE.CANCER__EMIS.ESAFETY
)
