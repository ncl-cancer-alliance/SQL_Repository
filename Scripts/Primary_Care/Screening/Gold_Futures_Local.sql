-- Dynamic table which combines cleaned local screening data with lung screening data.
-- Contact: eric.pinto@nhs.net

create or replace dynamic table DEV__REPORTING.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__SCREENING__LOCAL(
	REGION_NAME,
	PCN_NAME,
	BOROUGH,
	PRACTICE_CODE,
	PRACTICE_NAME,
	DEPRIVATION_QUINTILE,
	POPULATION_COUNT,
	PARENT_COUNT,
	DATE_FULL,
	COHORT_AGE_RANGE,
	COHORT_DESCRIPTION,
	DENOMINATOR_NAME,
	ACCEPTABLE,
	ACHIEVABLE,
	PROGRAMME,
	IS_MAX_DATE
) target_lag = '7 days' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table which combines cleaned local screening data with lung screening data.'
 as
WITH gp_gran AS (
    SELECT  
        'NCL' AS REGION_NAME,
        b.PCN_NAME,
        b.BOROUGH,
        a.PRACTICE_CODE,
        b.PRACTICE_NAME AS Practice_Name,
        CEIL(gp_imd.IMD_DECILE/2) AS DEPRIVATION_QUINTILE,
        SUM(a.NUMERATOR::INT) AS Population_Count,
        SUM(a.DENOMINATOR::INT) AS Parent_Count,
        a.DATE_FULL,
        a.COHORT_AGE_RANGE,
        a.COHORT_DESCRIPTION,
        a.DENOMINATOR_NAME,
        a.ACCEPTABLE,
        a.ACHIEVABLE,
        REPLACE(PROGRAMME,'Cancer','') AS "PROGRAMME",
        -- Boolean for Max Date for Each Screening Programme
        CASE 
            WHEN a.DATE_FULL = (
                SELECT MAX(DATE_FULL)
                FROM DEV__MODELLING.CANCER__SCREENING.SCREENING_LOCAL
                WHERE PROGRAMME = a.PROGRAMME
            ) THEN TRUE
            ELSE FALSE
        END AS IS_MAX_DATE
    FROM DEV__MODELLING.CANCER__SCREENING.SCREENING_LOCAL a
    LEFT JOIN (
        SELECT DISTINCT 
            PCN_NAME, 
            BOROUGH, 
            PRACTICE_CODE, 
            PRACTICE_NAME
        FROM MODELLING.LOOKUP_NCL.GP_PRACTICE
    ) b ON a.PRACTICE_CODE = b.PRACTICE_CODE
    
    LEFT JOIN MODELLING.LOOKUP_NCL.IMD_PRACTICE gp_imd
    ON a.PRACTICE_CODE = gp_imd.PRACTICE_CODE
    AND gp_imd.DATE_INDICATOR = 2019
    
    GROUP BY 
        b.PRACTICE_NAME,
        b.PCN_NAME,
        b.BOROUGH,
        a.PRACTICE_CODE,
        CEIL(gp_imd.IMD_DECILE/2),
        a.DATE_FULL,
        a.COHORT_AGE_RANGE,
        a.COHORT_DESCRIPTION,
        a.DENOMINATOR,
        a.DENOMINATOR_NAME,
        a.ACCEPTABLE,
        a.ACHIEVABLE,
        a.PROGRAMME,
        IS_MAX_DATE
),

lung_pcn AS (
    SELECT
        'NCL' AS REGION_NAME,
        PCN AS PCN_NAME,
        BOROUGH,
        NULL AS PRACTICE_CODE,
        NULL AS Practice_Name,
        NULL AS DEPRIVATION_QUINTILE,
        SUM(NUMERATOR) AS Population_Count,
        SUM(DENOMINATOR) AS Parent_Count,
        NULL AS DATE_FULL,
        NULL AS COHORT_AGE_RANGE,
        COHORT_DESCRIPTION,
        DENOMINATOR_NAME,
        NULL AS ACCEPTABLE,
        NULL AS ACHIEVABLE,
        'Lung' AS PROGRAMME,
        FALSE AS IS_MAX_DATE
    FROM DEV__MODELLING.CANCER__SCREENING.LUNG_OVERVIEW lung
    LEFT JOIN (
        SELECT DISTINCT 
            PCN_NAME,
            PCN_CODE,
            BOROUGH, 
        FROM MODELLING.LOOKUP_NCL.GP_PRACTICE
    ) b ON lung.PCN_CODE = b.PCN_CODE
    GROUP BY
        PCN,
        BOROUGH,
        COHORT_DESCRIPTION,
        DENOMINATOR_NAME,
        IS_MAX_DATE
)

SELECT * FROM (
    SELECT * FROM gp_gran
    UNION ALL
    SELECT * FROM lung_pcn
) t;