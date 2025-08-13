-- Dynamic table to provide Screening Benchmarking. This table combines aggregated Borough and NCL level data with National level data.
-- Contact: eric.pinto@nhs.net

CREATE OR REPLACE DYNAMIC TABLE DEV__REPORTING.PUBLIC.CANCER__SCREENING__BENCHMARKING(
    "REGION_NAME",
    "PROGRAMME",
    "DATE_FULL",
    "COHORT_AGE_RANGE",
    "COHORT_DESCRIPTION",
    "DENOMINATOR_NAME",
    "DENOMINATOR",
    "NUMERATOR",
    "ACCEPTABLE",
    "ACHIEVABLE",
    "IS_MAX_DATE",
    "REGION_ORDER"
    

) target_lag = '2 hours' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to provide Screening Benchmarking. This table combines aggregated Borough and NCL level data with National level data.'
 as
WITH borough_gran AS (
    SELECT  
        b.BOROUGH AS REGION_NAME,
        REPLACE(PROGRAMME,'Cancer','') AS "PROGRAMME",
        a.DATE_FULL,
        a.COHORT_AGE_RANGE,
        a.COHORT_DESCRIPTION,
        a.DENOMINATOR_NAME,
        SUM(a.DENOMINATOR::INT) AS DENOMINATOR,
        SUM(a.NUMERATOR::INT) AS NUMERATOR,
        a.ACCEPTABLE,
        a.ACHIEVABLE,
        CASE 
            WHEN a.DATE_FULL = (
                SELECT MAX(DATE_FULL)
                FROM DEV__MODELLING.CANCER__SCREENING.SCREENING_LOCAL
                WHERE PROGRAMME = a.PROGRAMME
            ) THEN TRUE
            ELSE FALSE
        END AS IS_MAX_DATE,
        CASE 
            WHEN b.BOROUGH = 'Barnet' THEN 1
            WHEN b.BOROUGH = 'Camden' THEN 2
            WHEN b.BOROUGH = 'Enfield' THEN 3
            WHEN b.BOROUGH = 'Haringey' THEN 4
            WHEN b.BOROUGH = 'Islington' THEN 5
            ELSE 99
        END AS REGION_ORDER
    FROM DEV__MODELLING.CANCER__SCREENING.SCREENING_LOCAL a
    LEFT JOIN (
        SELECT DISTINCT BOROUGH, GP_PRACTICE_CODE 
        FROM MODELLING.LOOKUP_NCL.PRACTICE_REFERENCE_FINAL
    ) b ON a.PRACTICE_CODE = b.GP_PRACTICE_CODE
    GROUP BY 
        b.BOROUGH,
        a.PROGRAMME,     
        a.DATE_FULL,
        a.COHORT_AGE_RANGE,
        a.COHORT_DESCRIPTION,
        a.DENOMINATOR_NAME,
        a.ACCEPTABLE,
        a.ACHIEVABLE,
        IS_MAX_DATE,
        REGION_ORDER
),

ncl_gran AS (
    SELECT  
        'NCL' AS REGION_NAME,
        REPLACE(PROGRAMME,'Cancer','') AS "PROGRAMME",
        sl.DATE_FULL,
        sl.COHORT_AGE_RANGE,
        sl.COHORT_DESCRIPTION,
        sl.DENOMINATOR_NAME,
        SUM(sl.DENOMINATOR::INT) AS DENOMINATOR,
        SUM(sl.NUMERATOR::INT) AS NUMERATOR,
        sl.ACCEPTABLE,
        sl.ACHIEVABLE,
        CASE 
            WHEN sl.DATE_FULL = (
                SELECT MAX(DATE_FULL)
                FROM DEV__MODELLING.CANCER__SCREENING.SCREENING_LOCAL
                WHERE PROGRAMME = sl.PROGRAMME
            ) THEN TRUE
            ELSE FALSE
        END AS IS_MAX_DATE,
        6 AS REGION_ORDER
    FROM DEV__MODELLING.CANCER__SCREENING.SCREENING_LOCAL sl
    GROUP BY 
        sl.PROGRAMME,
        sl.DATE_FULL,
        sl.COHORT_AGE_RANGE,
        sl.COHORT_DESCRIPTION,
        sl.DENOMINATOR_NAME,
        sl.ACCEPTABLE,
        sl.ACHIEVABLE,
        IS_MAX_DATE,
        REGION_ORDER
),

national_gran AS (
    SELECT  
        sn.REGION_NAME,
        sn.PROGRAMME,
        sn.DATE_FULL,
        sn.COHORT_AGE_RANGE,
        sn.COHORT_DESCRIPTION,
        sn.DENOMINATOR_NAME,
        sn.DENOMINATOR,
        sn.NUMERATOR,
        sn.ACCEPTABLE,
        sn.ACHIEVABLE,
        CASE 
            WHEN sn.DATE_FULL = (
                SELECT MAX(DATE_FULL)
                FROM DEV__MODELLING.CANCER__SCREENING.SCREENING_NATIONAL
                WHERE PROGRAMME = sn.PROGRAMME
            ) THEN TRUE
            ELSE FALSE
        END AS IS_MAX_DATE,
        CASE 
            WHEN sn.REGION_NAME = 'London' THEN 7
            WHEN sn.REGION_NAME = 'England' THEN 8
            ELSE 99
        END AS REGION_ORDER
    FROM DEV__MODELLING.CANCER__SCREENING.SCREENING_NATIONAL sn
)

SELECT * FROM (
    SELECT * FROM national_gran
    UNION ALL 
    SELECT * FROM ncl_gran
    UNION ALL 
    SELECT * FROM borough_gran
);