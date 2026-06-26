-- This pulls the most recent 4 financial years of data from the CWT National Monthly Modelling table
-- Contact: eric.pinto@nhs.net

create or replace dynamic table REPORTING.CANCER__CWT_SUMMARY_REPORTS.CWT_NATIONAL_DASHBOARD(
	ROW_POPULATION_TYPE,
	DATE_PERIOD,
	FIN_YEAR,
	FIN_MONTH_NUMBER,
	FIN_MONTH_NAME,
	ORGANISATION_TYPE,
	ORGANISATION_CODE,
	ORGANISATION_KEY,
	ORGANISATION_NAME,
	ORGANISATION_NAME_SHORT,
	ORGANISATION_REGION_CODE,
	ORGANISATION_REGION_NAME,
	ORGANISATION_ICB_CODE,
	ORGANISATION_ICB_NAME,
	CANCER_ALLIANCE,
	RADIOTHERAPY_NETWORK,
	STANDARD,
	CANCER_TYPE,
	CANCER_TYPE_SUBCATEGORY,
	CANCER_PATHWAY,
	CANCER_TYPE_GROUP,
	FDS_PATHWAY,
	NO_PATIENTS,
	NO_COMPLIANT,
	NO_BREACHES,
	STANDARD_PERFORMANCE,
	TARGET
) target_lag = '1 day' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table which selects most recent 4 years of data. Subsequent 31 day pathway derived as Combined minus First for Sub-ICB only. Combined excluded from final output.'
 as

-- Original data (Combined excluded)
SELECT 
    ROW_POPULATION_TYPE,
    DATE_PERIOD,
    FIN_YEAR,
    FIN_MONTH_NUMBER,
    FIN_MONTH_NAME,
    ORGANISATION_TYPE,
    ORGANISATION_CODE,
    ORGANISATION_CODE || '_' || ORGANISATION_TYPE AS ORGANISATION_KEY,
    ORGANISATION_NAME,
    ORGANISATION_NAME_SHORT,
    ORGANISATION_REGION_CODE,
    ORGANISATION_REGION_NAME,
    ORGANISATION_ICB_CODE,
    ORGANISATION_ICB_NAME,
    CANCER_ALLIANCE,
    RADIOTHERAPY_NETWORK,
    STANDARD,
    CANCER_TYPE,
    CANCER_TYPE_SUBCATEGORY,
    CANCER_PATHWAY,
    CANCER_TYPE_GROUP,
    FDS_PATHWAY,
    NO_PATIENTS,
    NO_COMPLIANT,
    NO_BREACHES,
    STANDARD_PERFORMANCE,
    TARGET
FROM MODELLING.CANCER__CWT_NATIONAL.CWT_NATIONAL_MONTHLY
-- Limit to most recent 4 financial years
WHERE FIN_YEAR >= (
    SELECT MIN(FIN_YEAR)
    FROM (
        SELECT DISTINCT FIN_YEAR
        FROM MODELLING.CANCER__CWT_NATIONAL.CWT_NATIONAL_MONTHLY
        ORDER BY FIN_YEAR DESC
        LIMIT 4
    ) t)
-- Sub-ICB only has All Cancers and Treatment groups available
-- Non-Sub-ICB: exclude All Cancers to avoid double counting
AND ((ORGANISATION_TYPE = 'Sub-ICB' AND CANCER_TYPE_GROUP IN ('All Cancers','Treatment'))
   OR (ORGANISATION_TYPE <> 'Sub-ICB' AND CANCER_TYPE_GROUP <> 'All Cancers'))
-- Exclude Combined (used for Subsequent derivation in second part). Rare Cancer not used in reporting
AND CANCER_PATHWAY NOT IN ('Rare Cancer', 'Combined')  -- Combined excluded here
AND ORGANISATION_NAME_SHORT NOT LIKE ('%Commissioning%') -- Exclude Commissioning Hub organisations
AND ORGANISATION_NAME_SHORT <> 'England' -- Exclude England summary row
AND ORGANISATION_TYPE NOT IN ('CCG') -- Exclude old CCG codes superseded by ICB

UNION ALL

-- Derived Subsequent rows (Combined minus First) for Sub-ICB 31 Day only
SELECT
    combined.ROW_POPULATION_TYPE,
    combined.DATE_PERIOD,
    combined.FIN_YEAR,
    combined.FIN_MONTH_NUMBER,
    combined.FIN_MONTH_NAME,
    combined.ORGANISATION_TYPE,
    combined.ORGANISATION_CODE,
    combined.ORGANISATION_CODE || '_' || combined.ORGANISATION_TYPE AS ORGANISATION_KEY,
    combined.ORGANISATION_NAME,
    combined.ORGANISATION_NAME_SHORT,
    combined.ORGANISATION_REGION_CODE,
    combined.ORGANISATION_REGION_NAME,
    combined.ORGANISATION_ICB_CODE,
    combined.ORGANISATION_ICB_NAME,
    combined.CANCER_ALLIANCE,
    combined.RADIOTHERAPY_NETWORK,
    combined.STANDARD,
    combined.CANCER_TYPE,
    combined.CANCER_TYPE_SUBCATEGORY,
    'Subsequent' AS CANCER_PATHWAY, -- Hardcode pathway as Subsequent since this is derived data
    combined.CANCER_TYPE_GROUP,
    combined.FDS_PATHWAY,
    -- Derive Subsequent metrics as Combined minus First
    combined.NO_PATIENTS - first.NO_PATIENTS AS NO_PATIENTS,
    combined.NO_COMPLIANT - first.NO_COMPLIANT AS NO_COMPLIANT,
    combined.NO_BREACHES - first.NO_BREACHES AS NO_BREACHES,
    ROUND(
        (combined.NO_COMPLIANT - first.NO_COMPLIANT) /
        NULLIF((combined.NO_PATIENTS - first.NO_PATIENTS), 0) * 100, 1
    ) AS STANDARD_PERFORMANCE,
    combined.TARGET
FROM MODELLING.CANCER__CWT_NATIONAL.CWT_NATIONAL_MONTHLY combined
-- Join Combined rows to their matching First rows
JOIN MODELLING.CANCER__CWT_NATIONAL.CWT_NATIONAL_MONTHLY first
    ON combined.ORGANISATION_CODE        = first.ORGANISATION_CODE
    AND combined.DATE_PERIOD             = first.DATE_PERIOD
    AND combined.FIN_YEAR                = first.FIN_YEAR
    AND combined.ORGANISATION_TYPE       = first.ORGANISATION_TYPE
    AND combined.STANDARD                = first.STANDARD
    AND combined.CANCER_TYPE             = first.CANCER_TYPE
    AND combined.CANCER_TYPE_GROUP       = first.CANCER_TYPE_GROUP
    AND combined.ROW_POPULATION_TYPE     = first.ROW_POPULATION_TYPE
    -- Handle NULL subcategory matching
    AND (combined.CANCER_TYPE_SUBCATEGORY = first.CANCER_TYPE_SUBCATEGORY
        OR (combined.CANCER_TYPE_SUBCATEGORY IS NULL AND first.CANCER_TYPE_SUBCATEGORY IS NULL))
    -- Handle NULL FDS pathway matching
    AND (combined.FDS_PATHWAY = first.FDS_PATHWAY
        OR (combined.FDS_PATHWAY IS NULL AND first.FDS_PATHWAY IS NULL))
WHERE combined.CANCER_PATHWAY = 'Combined' -- Combined side of the join
AND first.CANCER_PATHWAY = 'First' -- First side of the join
AND combined.ORGANISATION_TYPE = 'Sub-ICB'
AND combined.STANDARD = '31 Day'
-- Same 4 year filter as part 1
AND combined.FIN_YEAR >= (
    SELECT MIN(FIN_YEAR)
    FROM (
        SELECT DISTINCT FIN_YEAR
        FROM MODELLING.CANCER__CWT_NATIONAL.CWT_NATIONAL_MONTHLY
        ORDER BY FIN_YEAR DESC
        LIMIT 4
    ) t)
-- Same organisation exclusions as part 1
AND combined.ORGANISATION_NAME_SHORT NOT LIKE ('%Commissioning%')
AND combined.CANCER_TYPE_GROUP IN ('All Cancers', 'Treatment')
AND combined.ORGANISATION_NAME_SHORT <> 'England';