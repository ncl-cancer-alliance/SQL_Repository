CREATE OR REPLACE VIEW DEV__MODELLING.CANCER__CWT_NATIONAL.RANKING
COMMENT = 'Dataset to rank Providers against the FDS, 31 Day, and 62 Day cancer performance metrics.\nContact: jake.kealey@nhs.net'
AS

--CTE to get main columns of interest and limit data to 
WITH BASE_PER AS (
    SELECT 
        DATE_PERIOD,
        FIN_YEAR,
        FIN_MONTH_NUMBER
        FIN_MONTH_NAME,
        ORGANISATION_CODE,
        ORGANISATION_NAME,
        ORGANISATION_NAME_SHORT,
        ORGANISATION_ICB_CODE,
        ORGANISATION_ICB_NAME,
        ORGANISATION_REGION_CODE,
        ORGANISATION_REGION_NAME,
        CANCER_ALLIANCE,
        STANDARD,
        CANCER_TYPE,
        SUM(NO_PATIENTS) AS NO_PATIENTS,
        SUM(NO_COMPLIANT) AS NO_COMPLIANT,
        TO_NUMBER(SUM(NO_COMPLIANT) / SUM(NO_PATIENTS), 10, 2) AS STANDARD_PERFORMANCE,
        TO_NUMBER(AVG(TARGET), 10, 2) AS TARGET
    FROM MODELLING.CANCER__CWT_NATIONAL.CWT_NATIONAL_MONTHLY
    WHERE ROW_POPULATION_TYPE = 'Provider'
    AND ORGANISATION_CODE != 'ALL ENGLISH PROVIDERS'
    AND CANCER_TYPE_GROUP = 'Cancer Type'
    AND STANDARD IN ('FDS', '31 Day', '62 Day')
    AND NOT(STANDARD = '31 Day' AND CANCER_PATHWAY = 'Combined')
    
    GROUP BY ALL
),

--CTE to get ENGLAND level data including the total activity for each
--  DATE_PERIOD, STANDARD, CANCER_TYPE combination
OVERALL AS (
    SELECT 
        DATE_PERIOD,
        STANDARD,
        CANCER_TYPE,
        SUM(NO_PATIENTS) AS NO_PATIENTS_ENGLAND,
        SUM(NO_COMPLIANT) / SUM(NO_PATIENTS) AS STANDARD_PERFORMANCE_ENGLAND
    FROM BASE_PER
    GROUP BY ALL
)

SELECT 
    BASE_PER.*,

    -- Criteria for SIGNIFICANT_ACTIVITY:
    --     Ignore CANCER TYPES with less than 50 monthly patients nationally
    --     Only include trusts with either
    --       - 5 monthly patients under the '62 Day' standard
    --       - 10 patients
    --       - account for at least 1% of national monthly patients
    
    (
        NO_PATIENTS_ENGLAND >= 50
        AND (
            BASE_PER.NO_PATIENTS >= NO_PATIENTS_ENGLAND * 0.01
            OR (
                BASE_PER.NO_PATIENTS >= 10
                OR (BASE_PER.NO_PATIENTS >= 5 AND BASE_PER.STANDARD = '62 Day')
            )
        )
    ) AS SIGNIFICANT_ACTIVITY,
    
    --Get Provider rank for each DATE_PERIOD, STANDARD, CANCER_TYPE combination
    CASE
        WHEN SIGNIFICANT_ACTIVITY
        THEN
            RANK() OVER (
                PARTITION BY SIGNIFICANT_ACTIVITY, BASE_PER.DATE_PERIOD, BASE_PER.CANCER_TYPE, BASE_PER.STANDARD 
                ORDER BY STANDARD_PERFORMANCE DESC
            )
        ELSE NULL
        END AS RANK_PERFORMANCE,
    --Get a count of the number of organisations included in each ranking
    CASE
        WHEN SIGNIFICANT_ACTIVITY
        THEN 
            COUNT(DISTINCT ORGANISATION_CODE) OVER (
                PARTITION BY SIGNIFICANT_ACTIVITY, BASE_PER.DATE_PERIOD, BASE_PER.CANCER_TYPE, BASE_PER.STANDARD
            )
        ELSE NULL
    END AS RANK_DENOMINATOR,
    
    --Unadjusted version of the rank columns that ignores "SIGNIFICANT_ACTIVITY"
    RANK() OVER (
        PARTITION BY BASE_PER.DATE_PERIOD, BASE_PER.CANCER_TYPE, BASE_PER.STANDARD 
        ORDER BY STANDARD_PERFORMANCE DESC
    ) AS UNADJUSTED_RANK_PERFORMANCE,
    --Get a count of the number of organisations included in each ranking
    COUNT(DISTINCT ORGANISATION_CODE) OVER (
        PARTITION BY BASE_PER.DATE_PERIOD, BASE_PER.CANCER_TYPE, BASE_PER.STANDARD
    ) AS UNADJUSTED_RANK_DENOMINATOR,
    
    NO_PATIENTS_ENGLAND
    
FROM BASE_PER

--Join to get ENGLAND level data, needed for the SIGNIFICANT_ACTIVITY Criteria
LEFT JOIN OVERALL
ON BASE_PER.DATE_PERIOD = OVERALL.DATE_PERIOD
AND BASE_PER.STANDARD = OVERALL.STANDARD
AND BASE_PER.CANCER_TYPE = OVERALL.CANCER_TYPE

ORDER BY DATE_PERIOD DESC, CANCER_TYPE, STANDARD