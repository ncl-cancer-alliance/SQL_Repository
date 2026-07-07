create or replace dynamic table REPORTING.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__SCREENING__LOCAL(
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
	IS_MAX_DATE,
	GENDER
) target_lag = '7 days' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table which combines cleaned local screening data with lung screening data.\n\nContact: eric.pinto@nhs.net'
 as

-- gp_gran: practice-level screening data for all non-lung programmes from SCREENING_LOCAL
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
        -- Flag the most recent data extract for its programme
        CASE 
            WHEN a.DATE_FULL = (
                SELECT MAX(DATE_FULL)
                FROM MODELLING.CANCER__SCREENING.SCREENING_LOCAL
                WHERE PROGRAMME = a.PROGRAMME
            ) THEN TRUE
            ELSE FALSE
        END AS IS_MAX_DATE,
        NULL AS GENDER -- not available for non-lung programmes
    FROM MODELLING.CANCER__SCREENING.SCREENING_LOCAL a
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
        AND gp_imd.DATE_INDICATOR = 2025
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

-- lung_gp: practice-level lung screening uptake derived from row-level invite and screening tables
-- Denominator = all eligible patients (CANCER__LUNG_SCREENING__INVITES)
-- Numerator = patients who attended a screening (CANCER__LUNG_SCREENING__SCREENINGS)
lung_gp AS (
    SELECT
        'NCL' AS REGION_NAME,
        gp.PCN_NAME,
        gp.BOROUGH,
        inv.GP_PRACTICE_CODE AS PRACTICE_CODE,
        gp.PRACTICE_NAME,
        CEIL(gp_imd.IMD_DECILE/2) AS DEPRIVATION_QUINTILE,
        COUNT(DISTINCT scr.PSEUDO_ID) AS Population_Count,  -- numerator: distinct screened patients
        COUNT(DISTINCT inv.PSEUDO_ID) AS Parent_Count,      -- denominator: distinct eligible patients
        NULL AS DATE_FULL,        -- no date dimension in source data
        inv.AGE_AT_INVITATION AS COHORT_AGE_RANGE,
        'Uptake' AS COHORT_DESCRIPTION,
        'Eligible' AS DENOMINATOR_NAME,
        NULL AS ACCEPTABLE,
        NULL AS ACHIEVABLE,
        'Lung' AS PROGRAMME,
        TRUE AS IS_MAX_DATE,     
        inv.GENDER
    FROM DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__LUNG_SCREENING__INVITES inv
    LEFT JOIN (
        SELECT DISTINCT PSEUDO_ID -- DISTINCT to handle duplicate screening records
        FROM DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__LUNG_SCREENING__SCREENINGS
        WHERE PSEUDO_ID IS NOT NULL
    ) scr ON inv.PSEUDO_ID = scr.PSEUDO_ID
    LEFT JOIN MODELLING.LOOKUP_NCL.GP_PRACTICE gp 
        ON inv.GP_PRACTICE_CODE = gp.PRACTICE_CODE
    LEFT JOIN MODELLING.LOOKUP_NCL.IMD_PRACTICE gp_imd
        ON inv.GP_PRACTICE_CODE = gp_imd.PRACTICE_CODE
        AND gp_imd.DATE_INDICATOR = 2025
    GROUP BY
        inv.GP_PRACTICE_CODE,
        gp.PCN_NAME,
        gp.BOROUGH,
        gp.PRACTICE_NAME,
        CEIL(gp_imd.IMD_DECILE/2),
        inv.AGE_AT_INVITATION,
        inv.GENDER
)

-- Union all programmes together
SELECT * FROM gp_gran
UNION ALL
SELECT * FROM lung_gp
;