-- Dynamic table to prepare FIT data for use in the Primary Care Dashboard.
-- Contact: eric.pinto@nhs.net

create or replace dynamic table DEV__REPORTING.PUBLIC.CANCER__EMIS__FIT(
    PRACTICE_CODE,
    PRACTICE_NAME,
    DEPRIVATION_QUINTILE,
    PCN_NAME,
    BOROUGH,
	POPULATION_COUNT,
	PARENT_COUNT,
    DATE_FULL,
    TIME_PERIOD,
    YEAR_QUARTER_SORT,
    DATE_TYPE, 
    IS_MAX_DATE,
	"TIMESTAMP"

) target_lag = '2 hours' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to prepare data for use in the Primary Care Dashboard.'
 as

 SELECT
    b.GP_PRACTICE_CODE AS PRACTICE_CODE,
    b.GP_PRACTICE_DESC AS PRACTICE_NAME,
    b.DEPRIVATION_QUINTILE,
    b.PCN_NAME,
    b.BOROUGH,
    POPULATION_COUNT,
    PARENT_COUNT,
--    MALES,
--    FEMALES,
--    EXCLUDED,
--    STATUS,
    DATE_FULL,
    
    -- Time period label
    CASE 
        WHEN DATE_TYPE = 'Quarterly' THEN 
            CASE 
                WHEN MONTH(DATE_FULL) BETWEEN 4 AND 6  THEN 'Q1 ' || TO_CHAR(DATE_FULL, 'YYYY')
                WHEN MONTH(DATE_FULL) BETWEEN 7 AND 9  THEN 'Q2 ' || TO_CHAR(DATE_FULL, 'YYYY')
                WHEN MONTH(DATE_FULL) BETWEEN 10 AND 12 THEN 'Q3 ' || TO_CHAR(DATE_FULL, 'YYYY')
                WHEN MONTH(DATE_FULL) BETWEEN 1 AND 3  THEN 'Q4 ' || TO_CHAR(DATE_FULL - INTERVAL '1 YEAR', 'YYYY')
                ELSE 'Unknown'
            END
        WHEN DATE_TYPE = 'Monthly' THEN TO_CHAR(DATE_FULL, 'YYYY-MM')
        ELSE 'Unknown'
    END AS TIME_PERIOD,
    
    -- Quarter sort value
    CASE 
        WHEN MONTH(DATE_FULL) BETWEEN 4 AND 6  THEN YEAR(DATE_FULL) * 10 + 1
        WHEN MONTH(DATE_FULL) BETWEEN 7 AND 9  THEN YEAR(DATE_FULL) * 10 + 2
        WHEN MONTH(DATE_FULL) BETWEEN 10 AND 12 THEN YEAR(DATE_FULL) * 10 + 3
        WHEN MONTH(DATE_FULL) BETWEEN 1 AND 3  THEN (YEAR(DATE_FULL) - 1) * 10 + 4
        ELSE NULL
    END AS YEAR_QUARTER_SORT,

    DATE_TYPE,

    -- Flag for latest date
CASE 
    WHEN DATE_FULL = (
        SELECT MAX(DATE_FULL)
        FROM DEV__MODELLING.CANCER__EMIS.FIT AS sub
        WHERE sub.DATE_TYPE = a.DATE_TYPE
    ) THEN TRUE
    ELSE FALSE
END AS IS_MAX_DATE_BY_TYPE,

    "TIMESTAMP"

FROM DEV__MODELLING.CANCER__EMIS.FIT a
LEFT JOIN (
    SELECT DISTINCT 
        PCN_NAME, 
        BOROUGH, 
        GP_PRACTICE_CODE, 
        GP_PRACTICE_DESC, 
        DEPRIVATION_QUINTILE
    FROM MODELLING.LOOKUP_NCL.PRACTICE_REFERENCE_FINAL
) b ON a.CDB = b.GP_PRACTICE_CODE
