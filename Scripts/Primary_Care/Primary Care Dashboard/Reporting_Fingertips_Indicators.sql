-- Dynamic table to prepare Population Health data for use in the Primary Care Dashboard.
-- Contact: eric.pinto@nhs.net

create or replace dynamic table DEV__REPORTING.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__FINGERTIPS__INDICATOR_DATA(
	INDICATOR_ID,
	INDICATOR_NAME,
	AREA_TYPE,
	AREA_NAME,
	AREA_CODE,
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
	IS_NCL,
	IS_LONDON,
	IS_ENGLAND,
	IS_MAX_DATE,
	MAX_DATE
) target_lag = '1 day' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to prepare Population Health data for use in the Primary Care Dashboard.'
 as

-- CTE to enable retrieving past 3 years of data from individual Indicators
WITH Original AS (
SELECT 
    iaa.INDICATOR_ID,
    iaa.INDICATOR_NAME,
    iaa.AREA_TYPE,
	iaa.AREA_NAME,
    iaa.AREA_CODE,
    gp_ref.PRACTICE_NAME AS GP_PRACTICE_DESC,
    gp_ref.PCN_CODE,
    gp_ref.PCN_NAME,
    gp_ref.BOROUGH AS BOROUGH_NAME,
    iaa.VALUE,
    iaa.VALUE_UNIT,
    iaa.VALUE_TYPE,
    iaa.NUMERATOR,
    iaa.DENOMINATOR,
    iaa.DATE_INDICATOR,
    iaa.DATE_INDICATOR_TYPE,
    iaa.DATE_INDICATOR_RANGE,
    iaa.DATE_INDICATOR_SORTABLE,
    CASE WHEN AREA_NAME = 'NHS North Central London Integrated Care Board - QMJ' THEN 1 ELSE 0 END AS IS_NCL ,
    CASE WHEN AREA_NAME = ('London region (statistical)') THEN 1 ELSE 0 END AS IS_LONDON,
    CASE WHEN AREA_TYPE = 'England' THEN 1 ELSE 0 END AS IS_ENGLAND,
    -- Boolean to get max date for each Indicator
    CASE 
        WHEN DATE_INDICATOR_SORTABLE = MAX(DATE_INDICATOR_SORTABLE) OVER (PARTITION BY INDICATOR_NAME)
        THEN TRUE
        ELSE FALSE
    END AS IS_MAX_DATE,
    -- Actual max date for each Indicator (year only)
    TO_VARCHAR(LEFT(MAX(DATE_INDICATOR_SORTABLE) OVER (PARTITION BY INDICATOR_NAME), 4)) AS MAX_DATE

    
FROM DEV__MODELLING.FINGERTIPS.INDICATOR_DATA_ALL_AREAS iaa

LEFT JOIN MODELLING.LOOKUP_NCL.GP_PRACTICE gp_ref
ON iaa.AREA_CODE = gp_ref.PRACTICE_CODE

WHERE INDICATOR_ID IN (276, 91280, 91337, 91355, 91357, 91845, 92588, 93553, 94136, 93764, 93088)
AND AREA_TYPE IN ('GPs','ICBs','Regions (statistical)','England')
-- These Where clauses are needed to deduplicate 'England' Data
AND CATEGORY_TYPE IS NULL
AND SEX = 'Persons'
---------------
--INDICATOR 93088 has multiple Age Brackets. Select 18+ Only.
AND (INDICATOR_ID <> 93088 OR AGE = '18+ yrs')
)

SELECT *
FROM ORIGINAL
-- WHERE clause to get the previous 3 years of data for each indicator separately
WHERE CAST(LEFT(DATE_INDICATOR_SORTABLE, 4) AS NUMBER) >= MAX_DATE - 3;