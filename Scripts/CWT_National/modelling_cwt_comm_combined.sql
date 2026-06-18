create or replace view DEV__MODELLING.CANCER__CWT_NATIONAL.CWT_COMM_COMBINED(
	REPORT_DATE,
	ORGANISATION_CODE,
	ORGANISATION_NAME,
	CARE_SETTING,
	STANDARD,
	TYPE_OF_CANCER,
	CANCER_TYPE,
	TOTAL,
	WITHIN_14_DAYS,
	AFTER_14_DAYS,
	PERCENTAGE_SEEN_WITHIN_14_DAYS,
	IN_15_TO_16_DAYS,
	IN_17_TO_21_DAYS,
	IN_22_TO_28_DAYS,
	AFTER_28_DAYS,
	WITHIN_28_DAYS,
	WITHIN_31_DAYS,
	AFTER_31_DAYS,
	PERCENTAGE_TREATED_WITHIN_31_DAYS,
	WITHIN_32_TO_38_DAYS,
	WITHIN_39_TO_48_DAYS,
	WITHIN_49_TO_62_DAYS,
	AFTER_62_DAYS,
	WITHIN_62_DAYS,
	PERCENTAGE_TREATED_WITHIN_62_DAYS,
	WITHIN_63_TO_76_DAYS,
	WITHIN_77_TO_90_DAYS,
	WITHIN_91_TO_104_DAYS,
	AFTER_104_DAYS,
	IN_15_TO_28_DAYS,
	IN_29_TO_42_DAYS,
	IN_43_TO_62_DAYS,
	PERCENTAGE_TOLD_WITHIN_28_DAYS,
	TARGET,
	ORGANISATION_TYPE,
	CREATE_TS
) as

-- PMCT Commissioner (all months)
SELECT
    "ReportDate"                                                            AS REPORT_DATE,
    CCG_CODE                                                                AS ORGANISATION_CODE,
    CLINICAL_COMMISSIONING_GROUP                                            AS ORGANISATION_NAME,
    CARE_SETTING,
    STANDARD,
    TYPE_OF_CANCER,
    CANCER_TYPE,
    TOTAL,
    WITHIN_14_DAYS,
    AFTER_14_DAYS,
    PERCENTAGE_SEEN_WITHIN_14_DAYS,
    IN_15_TO_16_DAYS,
    IN_17_TO_21_DAYS,
    IN_22_TO_28_DAYS,
    AFTER_28_DAYS,
    WITHIN_28_DAYS,
    WITHIN_31_DAYS,
    AFTER_31_DAYS,
    PERCENTAGE_TREATED_WITHIN_31_DAYS,
    WITHIN_32_TO_38_DAYS,
    WITHIN_39_TO_48_DAYS,
    WITHIN_49_TO_62_DAYS,
    AFTER_62_DAYS,
    WITHIN_62_DAYS,
    PERCENTAGE_TREATED_WITHIN_62_DAYS,
    WITHIN_63_TO_76_DAYS,
    WITHIN_77_TO_90_DAYS,
    WITHIN_91_TO_104_DAYS,
    AFTER_104_DAYS,
    IN_15_TO_28_DAYS,
    IN_29_TO_42_DAYS,
    IN_43_TO_62_DAYS,
    PERCENTAGE_TOLD_WITHIN_28_DAYS,
    TARGET,
    ORGANISATION_TYPE,
    "CreateTS"                                                              AS CREATE_TS
FROM DATA_LAKE.PMCT."CwtMonthlySourceAppendReviseComm"

UNION ALL

-- New combined source Commissioner (months not in PMCT)
SELECT
    CAST(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', _INGESTED_AT)) AS DATE)    AS REPORT_DATE,
    ORG_CODE                                                                AS ORGANISATION_CODE,
    ORG_NAME                                                                AS ORGANISATION_NAME,
    REFERRAL_ROUTE_OR_STAGE                                                 AS CARE_SETTING,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS'                                THEN '28 DAY'
        WHEN '31D'                                THEN '31-DAY WAIT'
        WHEN '62D'                                THEN '62 DAY'
        WHEN 'Urgent Suspected Cancer referral'   THEN '2-WEEK WAIT'
        WHEN 'Urgent Breast Symptomatic referral' THEN '2-WEEK WAIT'
    END                                                                     AS STANDARD,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS' THEN
            CASE REFERRAL_ROUTE_OR_STAGE
                WHEN 'ALL ROUTES' THEN '28 DAY FAST DIAGNOSIS (ALL ROUTES)'
                ELSE '28 DAY FAST DIAGNOSIS (BY ROUTE)'
            END
        ELSE
            CASE TREATMENT_MODALITY
                WHEN 'Surgery'                  THEN 'Surgery'
                WHEN 'Radiotherapy'             THEN 'Radiotherapy'
                WHEN 'Anti-cancer drug regimen' THEN 'Drugs'
                WHEN 'Other'                    THEN 'Other'
                ELSE CANCER_TYPE
            END
    END                                                                     AS TYPE_OF_CANCER,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS' THEN
            CASE REFERRAL_ROUTE_OR_STAGE
                WHEN 'ALL ROUTES' THEN '28 DAY FAST DIAGNOSIS (ALL ROUTES)'
                ELSE '28 DAY FAST DIAGNOSIS (BY ROUTE)'
            END
        ELSE
            CASE TREATMENT_MODALITY
                WHEN 'Surgery'                  THEN 'Surgery'
                WHEN 'Radiotherapy'             THEN 'Radiotherapy'
                WHEN 'Anti-cancer drug regimen' THEN 'Drugs'
                WHEN 'Other'                    THEN 'Other'
                ELSE TREATMENT_MODALITY
            END
    END                                                                     AS CANCER_TYPE,
    TOTAL,
    CASE STANDARD_OR_ITEM
        WHEN 'Urgent Suspected Cancer referral'   THEN WITHIN
        WHEN 'Urgent Breast Symptomatic referral' THEN WITHIN
        ELSE WITHIN_14_DAYS
    END                                                                     AS WITHIN_14_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN 'Urgent Suspected Cancer referral'   THEN AFTER
        WHEN 'Urgent Breast Symptomatic referral' THEN AFTER
        ELSE NULL
    END                                                                     AS AFTER_14_DAYS,
    NULL                                                                    AS PERCENTAGE_SEEN_WITHIN_14_DAYS,
    IN_15_TO_16_DAYS,
    IN_17_TO_21_DAYS,
    IN_22_TO_28_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS' THEN AFTER
        ELSE AFTER_28_DAYS
    END                                                                     AS AFTER_28_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS' THEN WITHIN
        ELSE NULL
    END                                                                     AS WITHIN_28_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN '31D' THEN WITHIN
        WHEN '62D' THEN WITHIN_31_DAYS
        ELSE NULL
    END                                                                     AS WITHIN_31_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN '31D' THEN AFTER
        ELSE NULL
    END                                                                     AS AFTER_31_DAYS,
    NULL                                                                    AS PERCENTAGE_TREATED_WITHIN_31_DAYS,
    IN_32_TO_38_DAYS                                                        AS WITHIN_32_TO_38_DAYS,
    IN_39_TO_48_DAYS                                                        AS WITHIN_39_TO_48_DAYS,
    IN_49_TO_62_DAYS                                                        AS WITHIN_49_TO_62_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS' THEN NULL
        ELSE AFTER_62_DAYS
    END                                                                     AS AFTER_62_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN '62D' THEN WITHIN
        ELSE NULL
    END                                                                     AS WITHIN_62_DAYS,
    NULL                                                                    AS PERCENTAGE_TREATED_WITHIN_62_DAYS,
    IN_63_TO_76_DAYS                                                        AS WITHIN_63_TO_76_DAYS,
    IN_77_TO_90_DAYS                                                        AS WITHIN_77_TO_90_DAYS,
    IN_91_TO_104_DAYS                                                       AS WITHIN_91_TO_104_DAYS,
    AFTER_104_DAYS,
    IN_15_TO_28_DAYS,
    IN_29_TO_42_DAYS,
    IN_43_TO_62_DAYS,
    NULL                                                                    AS PERCENTAGE_TOLD_WITHIN_28_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN '28 DAY'      THEN 0.80
        WHEN '31-DAY WAIT' THEN 0.96
        WHEN '62 DAY'      THEN 0.85
        END                                                                 AS TARGET,
    'ICB'                                                                   AS ORGANISATION_TYPE,
    _INGESTED_AT                                                            AS CREATE_TS
FROM DATA_LAKE.PERFORMANCE."CwtMonthlySourceAppendCombined"
WHERE BASIS = 'Commissioner'
AND CAST(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', _INGESTED_AT)) AS DATE) NOT IN (
    SELECT DISTINCT CAST("ReportDate" AS DATE)
    FROM DATA_LAKE.PMCT."CwtMonthlySourceAppendReviseComm"
)
AND NOT (
    REFERRAL_ROUTE_OR_STAGE = 'ALL ROUTES'
    AND CANCER_TYPE <> 'ALL CANCERS'
)


UNION ALL

-- New Sub-ICB commissioner source (months not in PMCT)
SELECT
    CAST(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', _INGESTED_AT)) AS DATE)     AS REPORT_DATE,
    ICB_SUB_LOCATION                                                        AS ORGANISATION_CODE,
    NULL                                                                    AS ORGANISATION_NAME,
    'ALL CARE'                                                              AS CARE_SETTING,
    CASE STANDARD
        WHEN '28-day FDS'      THEN '28 DAY'
        WHEN '31-day Combined' THEN '31-DAY WAIT'
        WHEN '62-day Combined' THEN '62 DAY'
    END                                                                     AS STANDARD,
    CASE SUB_CATEGORY
        WHEN '62-day (Urgent Suspected Cancer)'         THEN 'ALL CANCERS'
        WHEN '62-day (Screening)'                       THEN 'ALL CANCERS'
        WHEN '62-day (Breast Symptomatic)'              THEN 'ALL CANCERS'
        WHEN '62-day (Consultant Upgrade)'              THEN 'ALL CANCERS'
        WHEN '31-day (First)'                           THEN 'ALL CANCERS'
        WHEN '31-day Sub (Surgery)'                     THEN 'SURGERY'
        WHEN '31-day Sub (Radiotherapy)'                THEN 'RADIOTHERAPY'
        WHEN '31-day Sub (Anti-cancer drug regimen)'    THEN 'DRUGS'
        WHEN '31-day Sub (Other)'                       THEN 'OTHER'
        ELSE 'ALL CANCERS'
    END                                                                     AS TYPE_OF_CANCER,
    CASE SUB_CATEGORY
        WHEN '62-day (Urgent Suspected Cancer)'         THEN '62-DAY URGENT SUSPECTED CANCER ALL CANCER'
        WHEN '62-day (Screening)'                       THEN '62-DAY - SCREENING ALL CANCERS'
        WHEN '62-day (Breast Symptomatic)'              THEN '62-DAY BREAST SYMPTOMATIC'
        WHEN '62-day (Consultant Upgrade)'              THEN '62-DAY - CONSULTANT UPGRADE ALL CANCERS'
        WHEN '31-day (First)'                           THEN '31-DAY - FIRST TREATMENT ALL CANCER'
        WHEN '31-day Sub (Surgery)'                     THEN '31-DAY - 2nd/SUBSEQUENT TREATMENT (SURGERY)'
        WHEN '31-day Sub (Radiotherapy)'                THEN '31-DAY - 2nd/SUBSEQUENT TREATMENT (RADIOTHERAPY)'
        WHEN '31-day Sub (Anti-cancer drug regimen)'    THEN '31-DAY - 2nd/SUBSEQUENT TREATMENT (DRUG)'
        WHEN '31-day Sub (Other)'                       THEN '31-DAY - 2nd/SUBSEQUENT TREATMENT (OTHER)'
        ELSE NULL
    END                                                                     AS CANCER_TYPE,
    TOTAL,
    NULL                                                                    AS WITHIN_14_DAYS,
    NULL                                                                    AS AFTER_14_DAYS,
    NULL                                                                    AS PERCENTAGE_SEEN_WITHIN_14_DAYS,
    NULL                                                                    AS IN_15_TO_16_DAYS,
    NULL                                                                    AS IN_17_TO_21_DAYS,
    NULL                                                                    AS IN_22_TO_28_DAYS,
    CASE STANDARD
        WHEN '28-day FDS'      THEN BREACHES
        ELSE NULL
    END                                                                     AS AFTER_28_DAYS,
    CASE STANDARD
        WHEN '28-day FDS'      THEN WITHIN_STANDARD
        ELSE NULL
    END                                                                     AS WITHIN_28_DAYS,
    CASE STANDARD
        WHEN '31-day Combined' THEN WITHIN_STANDARD
        ELSE NULL
    END                                                                     AS WITHIN_31_DAYS,
    CASE STANDARD
        WHEN '31-day Combined' THEN BREACHES
        ELSE NULL
    END                                                                     AS AFTER_31_DAYS,
    NULL                                                                    AS PERCENTAGE_TREATED_WITHIN_31_DAYS,
    NULL                                                                    AS WITHIN_32_TO_38_DAYS,
    NULL                                                                    AS WITHIN_39_TO_48_DAYS,
    NULL                                                                    AS WITHIN_49_TO_62_DAYS,
    CASE STANDARD
        WHEN '62-day Combined' THEN BREACHES
        ELSE NULL
    END                                                                     AS AFTER_62_DAYS,
    CASE STANDARD
        WHEN '62-day Combined' THEN WITHIN_STANDARD
        ELSE NULL
    END                                                                     AS WITHIN_62_DAYS,
    NULL                                                                    AS PERCENTAGE_TREATED_WITHIN_62_DAYS,
    NULL                                                                    AS WITHIN_63_TO_76_DAYS,
    NULL                                                                    AS WITHIN_77_TO_90_DAYS,
    NULL                                                                    AS WITHIN_91_TO_104_DAYS,
    NULL                                                                    AS AFTER_104_DAYS,
    NULL                                                                    AS IN_15_TO_28_DAYS,
    NULL                                                                    AS IN_29_TO_42_DAYS,
    NULL                                                                    AS IN_43_TO_62_DAYS,
    NULL                                                                    AS PERCENTAGE_TOLD_WITHIN_28_DAYS,
    CASE STANDARD
        WHEN '28-day FDS'      THEN 0.80
        WHEN '31-day Combined' THEN 0.96
        WHEN '62-day Combined' THEN 0.85
    END                                                                     AS TARGET,
    'Sub-ICB'                                                               AS ORGANISATION_TYPE,
    _INGESTED_AT                                                            AS CREATE_TS
FROM DATA_LAKE.PERFORMANCE."CwtMonthlySourceAppendReviseComm"
WHERE CAST(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', _INGESTED_AT)) AS DATE) NOT IN (
    SELECT DISTINCT CAST("ReportDate" AS DATE)
    FROM DATA_LAKE.PMCT."CwtMonthlySourceAppendReviseComm"
)

UNION ALL

-- Sub-ICB 31-day Combined (derived by summing First + all Subsequent)
SELECT
    CAST(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', _INGESTED_AT)) AS DATE)    AS REPORT_DATE,
    ICB_SUB_LOCATION                                                        AS ORGANISATION_CODE,
    NULL                                                                    AS ORGANISATION_NAME,
    NULL                                                                    AS CARE_SETTING,
    '31-DAY WAIT'                                                           AS STANDARD,
    'ALL CANCERS'                                                           AS TYPE_OF_CANCER,
    '31-DAY COMBINED TREATMENT ALL CANCER'                                  AS CANCER_TYPE,
    SUM(TOTAL)                                                              AS TOTAL,
    NULL                                                                    AS WITHIN_14_DAYS,
    NULL                                                                    AS AFTER_14_DAYS,
    NULL                                                                    AS PERCENTAGE_SEEN_WITHIN_14_DAYS,
    NULL                                                                    AS IN_15_TO_16_DAYS,
    NULL                                                                    AS IN_17_TO_21_DAYS,
    NULL                                                                    AS IN_22_TO_28_DAYS,
    NULL                                                                    AS AFTER_28_DAYS,
    NULL                                                                    AS WITHIN_28_DAYS,
    SUM(WITHIN_STANDARD)                                                    AS WITHIN_31_DAYS,
    SUM(BREACHES)                                                           AS AFTER_31_DAYS,
    NULL                                                                    AS PERCENTAGE_TREATED_WITHIN_31_DAYS,
    NULL                                                                    AS WITHIN_32_TO_38_DAYS,
    NULL                                                                    AS WITHIN_39_TO_48_DAYS,
    NULL                                                                    AS WITHIN_49_TO_62_DAYS,
    NULL                                                                    AS AFTER_62_DAYS,
    NULL                                                                    AS WITHIN_62_DAYS,
    NULL                                                                    AS PERCENTAGE_TREATED_WITHIN_62_DAYS,
    NULL                                                                    AS WITHIN_63_TO_76_DAYS,
    NULL                                                                    AS WITHIN_77_TO_90_DAYS,
    NULL                                                                    AS WITHIN_91_TO_104_DAYS,
    NULL                                                                    AS AFTER_104_DAYS,
    NULL                                                                    AS IN_15_TO_28_DAYS,
    NULL                                                                    AS IN_29_TO_42_DAYS,
    NULL                                                                    AS IN_43_TO_62_DAYS,
    NULL                                                                    AS PERCENTAGE_TOLD_WITHIN_28_DAYS,
    0.96                                                                    AS TARGET,
    'Sub-ICB'                                                               AS ORGANISATION_TYPE,
    MAX(_INGESTED_AT)                                                       AS CREATE_TS
FROM DATA_LAKE.PERFORMANCE."CwtMonthlySourceAppendReviseComm"
WHERE STANDARD = '31-day Combined'
AND CAST(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', _INGESTED_AT)) AS DATE) NOT IN (
    SELECT DISTINCT CAST("ReportDate" AS DATE)
    FROM DATA_LAKE.PMCT."CwtMonthlySourceAppendReviseComm"
)
GROUP BY
    CAST(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', _INGESTED_AT)) AS DATE),
    ICB_SUB_LOCATION;

;