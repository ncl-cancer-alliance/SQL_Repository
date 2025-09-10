-- Dynamic table to prepare Social Prescribing data for use in the Primary Care Dashboard.
-- Contact: eric.pinto@nhs.net

CREATE OR REPLACE DYNAMIC TABLE DEV__REPORTING.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__EMIS__SOCIAL_PRESCRIBING(
    
    INDICATOR_ID,
	INDICATOR_NAME,
	GP_PRACTICE_CODE,
	GP_PRACTICE_DESC,
	PCN_CODE,
	PCN_NAME,
	BOROUGH_NAME,
	VALUE,
	VALUE_UNIT,
	VALUE_TYPE,
	NUMERATOR,
	DENOMINATOR,
	DATE_INDICATOR,
	DATE_INDICATOR_TYPE,
	DATE_INDICATOR_RANGE,
	DATE_INDICATOR_SORTABLE,
    IS_MAX_DATE,
    MAX_DATE
    
    ) target_lag = '2 hours' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to prepare Social Prescribing data for use in the Primary Care Dashboard.'
 as

WITH ORIGINAL AS (

    SELECT
        TO_NUMBER(NULL) AS INDICATOR_ID,
        'No. of Social Prescribing referrals made within the last 12 months' AS INDICATOR_NAME,
        GP_PRACTICE_CODE,
        GP_PRACTICE_DESC,
        PCN_CODE,
        PCN_NAME,
        BOROUGH AS BOROUGH_NAME,
        POPULATION_COUNT AS VALUE,
        NULL AS VALUE_UNIT,
        'Number' AS VALUE_TYPE,
        POPULATION_COUNT AS NUMERATOR,
        PARENT_COUNT AS DENOMINATOR,
        TO_VARCHAR(DATE_FULL) AS DATE_INDICATOR,
        'Calendar' AS DATE_INDICATOR_TYPE,
        '1m' AS DATE_INDICATOR_RANGE,
        TO_NUMBER(TO_CHAR(DATE_FULL, 'YYYYMM') || '00') AS DATE_INDICATOR_SORTABLE,

        -- Boolean to get max date
        CASE 
            WHEN TO_NUMBER(TO_CHAR(DATE_FULL, 'YYYYMM') || '00') = MAX(TO_NUMBER(TO_CHAR(DATE_FULL, 'YYYYMM') || '00')) OVER()
            THEN TRUE
            ELSE FALSE
        END AS IS_MAX_DATE,

        TO_VARCHAR(MAX(DATE_FULL) OVER()) AS MAX_DATE

    FROM DEV__MODELLING.CANCER__EMIS.SOCIAL_PRESCRIBING a

    LEFT JOIN (
        SELECT DISTINCT 
            PCN_NAME,
            PCN_CODE,
            BOROUGH, 
            GP_PRACTICE_CODE, 
            GP_PRACTICE_DESC, 
            DEPRIVATION_QUINTILE
        FROM MODELLING.LOOKUP_NCL.PRACTICE_REFERENCE_FINAL
    ) b ON a.CDB = b.GP_PRACTICE_CODE

)

SELECT *
FROM ORIGINAL

-- Filter to include only rows within 3 years of the latest available date
WHERE DATE_INDICATOR_SORTABLE >= TO_NUMBER(
    TO_CHAR(DATEADD(YEAR, -3, TO_DATE(MAX_DATE, 'YYYY-MM-DD')), 'YYYYMM') || '00'
)