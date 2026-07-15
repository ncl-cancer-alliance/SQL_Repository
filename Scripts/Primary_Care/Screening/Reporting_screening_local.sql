CREATE OR REPLACE DYNAMIC TABLE DEV__REPORTING.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__SCREENING__LOCAL(
	REGION_NAME,
	PCN_NAME,
	REGISTERED_BOROUGH,
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
	GENDER,
	NEIGHBOURHOOD_NAME,
	RESIDENT_BOROUGH
) TARGET_LAG = '7 days' REFRESH_MODE = FULL INITIALIZE = ON_CREATE WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Dynamic table which combines cleaned local screening data with lung screening data.\n\nContact: eric.pinto@nhs.net'
AS

-- gp_gran: practice-level screening data for all non-lung programmes from SCREENING_LOCAL
WITH gp_gran AS (
    SELECT  
        'NCL' AS REGION_NAME,
        b.PCN_NAME,
        b.REGISTERED_BOROUGH_NAME AS REGISTERED_BOROUGH,
        a.PRACTICE_CODE,
        b.PRACTICE_NAME,
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
                FROM DEV__MODELLING.CANCER__SCREENING.SCREENING_LOCAL
                WHERE PROGRAMME = a.PROGRAMME
            ) THEN TRUE
            ELSE FALSE
        END AS IS_MAX_DATE,
        NULL AS GENDER,             -- not available for non-lung programmes
        NULL AS NEIGHBOURHOOD_NAME, -- derived from LSOA, only available for lung
        NULL AS RESIDENT_BOROUGH    -- derived from LSOA, only available for lung
    FROM DEV__MODELLING.CANCER__SCREENING.SCREENING_LOCAL a
    -- Enrich with PCN, borough, and practice name from the practice reference view
    LEFT JOIN (
        SELECT DISTINCT
            PRACTICE_CODE,
            PRACTICE_NAME,
            PCN_NAME,
            REGISTERED_BOROUGH_NAME
        FROM REFERENCE.PRIMARY_CARE.PRACTICE_ALL
    ) b ON a.PRACTICE_CODE = b.PRACTICE_CODE
    -- Enrich with deprivation decile for the most recent available year
    LEFT JOIN MODELLING.LOOKUP_NCL.IMD_PRACTICE gp_imd
        ON a.PRACTICE_CODE = gp_imd.PRACTICE_CODE
        AND gp_imd.DATE_INDICATOR = 2025
    GROUP BY 
        b.PRACTICE_NAME,
        b.PCN_NAME,
        b.REGISTERED_BOROUGH_NAME,
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

-- Derive cutoff date: max screening month minus 1, to exclude the most recent incomplete month
lung_cutoff AS (
    SELECT
        DATEADD('MONTH', -1, DATE_TRUNC('MONTH', MAX(TO_DATE(MONTHYEARPHSCR, 'MON-YY')))) AS CUTOFF_DATE
    FROM DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__LUNG_SCREENING__SCREENINGS
    WHERE MONTHYEARPHSCR IS NOT NULL
),

-- lung_gp: practice-level lung screening uptake derived from row-level invite and screening tables
-- Denominator = all eligible patients (CANCER__LUNG_SCREENING__INVITES)
-- Numerator = patients screened up to and including the cutoff date (CANCER__LUNG_SCREENING__SCREENINGS)
lung_gp AS (
    SELECT
        'NCL' AS REGION_NAME,
        gp.PCN_NAME,
        gp.REGISTERED_BOROUGH_NAME AS REGISTERED_BOROUGH,
        inv.GP_PRACTICE_CODE AS PRACTICE_CODE,
        gp.PRACTICE_NAME,
        CEIL(gp_imd.IMD_DECILE/2) AS DEPRIVATION_QUINTILE,
        COUNT(DISTINCT scr.PSEUDO_ID) AS Population_Count,  -- numerator: distinct screened patients up to cutoff
        COUNT(DISTINCT inv.PSEUDO_ID) AS Parent_Count,      -- denominator: distinct eligible patients
        (SELECT CUTOFF_DATE FROM lung_cutoff) AS DATE_FULL,
        inv.AGE_AT_INVITATION AS COHORT_AGE_RANGE,
        'Uptake' AS COHORT_DESCRIPTION,
        'Eligible' AS DENOMINATOR_NAME,
        NULL AS ACCEPTABLE,
        NULL AS ACHIEVABLE,
        'Lung' AS PROGRAMME,
        TRUE AS IS_MAX_DATE,
        inv.GENDER,
        n.NEIGHBOURHOOD_NAME,
        CASE
            WHEN n.REGISTERED_BOROUGH_NAME IN ('Barnet', 'Camden', 'Enfield', 'Haringey', 'Islington') 
            THEN n.REGISTERED_BOROUGH_NAME
            ELSE 'Outside NCL'
        END AS RESIDENT_BOROUGH
    FROM DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__LUNG_SCREENING__INVITES inv
    LEFT JOIN (
        SELECT DISTINCT PSEUDO_ID -- DISTINCT to handle duplicate screening records
        FROM DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__LUNG_SCREENING__SCREENINGS
        WHERE PSEUDO_ID IS NOT NULL
        AND TO_DATE(MONTHYEARPHSCR, 'MON-YY') <= (SELECT CUTOFF_DATE FROM lung_cutoff)
    ) scr ON inv.PSEUDO_ID = scr.PSEUDO_ID
    -- Enrich with PCN, borough, and practice name from the practice reference view
    LEFT JOIN (
        SELECT DISTINCT
            PRACTICE_CODE,
            PRACTICE_NAME,
            PCN_NAME,
            REGISTERED_BOROUGH_NAME
        FROM REFERENCE.PRIMARY_CARE.PRACTICE_ALL
    ) gp ON inv.GP_PRACTICE_CODE = gp.PRACTICE_CODE
    -- Enrich with deprivation decile for the most recent available year
    LEFT JOIN MODELLING.LOOKUP_NCL.IMD_PRACTICE gp_imd
        ON inv.GP_PRACTICE_CODE = gp_imd.PRACTICE_CODE
        AND gp_imd.DATE_INDICATOR = 2025
    -- Join neighbourhood lookup on LSOA_CODE to get neighbourhood and resident borough
    LEFT JOIN (
        SELECT DISTINCT
            LSOA_CODE,
            NEIGHBOURHOOD_NAME,
            REGISTERED_BOROUGH_NAME
        FROM REFERENCE.GEO.NEIGHBOURHOOD_LSOA
    ) n ON inv.LSOA_CODE = n.LSOA_CODE
    GROUP BY
        inv.GP_PRACTICE_CODE,
        gp.PCN_NAME,
        gp.REGISTERED_BOROUGH_NAME,
        gp.PRACTICE_NAME,
        CEIL(gp_imd.IMD_DECILE/2),
        inv.AGE_AT_INVITATION,
        inv.GENDER,
        n.NEIGHBOURHOOD_NAME,
        CASE
            WHEN n.REGISTERED_BOROUGH_NAME IN ('Barnet', 'Camden', 'Enfield', 'Haringey', 'Islington') 
            THEN n.REGISTERED_BOROUGH_NAME
            ELSE 'Outside NCL'
        END,
        (SELECT CUTOFF_DATE FROM lung_cutoff)
)

-- Union all programmes together
SELECT * FROM gp_gran
UNION ALL
SELECT * FROM lung_gp
;