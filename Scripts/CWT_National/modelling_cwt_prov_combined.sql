create or replace view DEV__MODELLING.CANCER__CWT_NATIONAL.CWT_PROV_COMBINED(
	REPORT_DATE,
	PROVIDER_CODE,
	PROVIDER_NAME,
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

-- PMCT source (all months)
SELECT
    "ReportDate"                        AS REPORT_DATE,
    PROVIDER_CODE,
    PROVIDER_NAME,
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
    "CreateTS"                          AS CREATE_TS
FROM DATA_LAKE.PMCT."CwtMonthlySourceAppendReviseProv"

UNION ALL

-- New combined source (only months not already in PMCT)
SELECT
    CAST(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', _INGESTED_AT)) AS DATE) AS REPORT_DATE,
    ORG_CODE                            AS PROVIDER_CODE,
    ORG_NAME                            AS PROVIDER_NAME,
    REFERRAL_ROUTE_OR_STAGE             AS CARE_SETTING,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS'                                THEN '28 DAY'
        WHEN '31D'                                THEN '31-DAY WAIT'
        WHEN '62D'                                THEN '62 DAY'
        WHEN 'Urgent Suspected Cancer referral'   THEN '2-WEEK WAIT'
        WHEN 'Urgent Breast Symptomatic referral' THEN '2-WEEK WAIT'
    END                                 AS STANDARD,
    CASE
        WHEN STANDARD_OR_ITEM = '31D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Surgery'                   THEN 'Surgery'
        WHEN STANDARD_OR_ITEM = '31D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Radiotherapy'              THEN 'Radiotherapy'
        WHEN STANDARD_OR_ITEM = '31D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Anti-cancer drug regimen'  THEN 'Drugs'
        WHEN STANDARD_OR_ITEM = '31D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Other'                     THEN 'Other'
        WHEN STANDARD_OR_ITEM = '62D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Surgery'                   THEN 'Surgery'
        WHEN STANDARD_OR_ITEM = '62D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Radiotherapy'              THEN 'Radiotherapy'
        WHEN STANDARD_OR_ITEM = '62D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Anti-cancer drug regimen'  THEN 'Drugs'
        WHEN STANDARD_OR_ITEM = '62D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Other'                     THEN 'Other'
        ELSE CANCER_TYPE
    END                                                                     AS TYPE_OF_CANCER,
    CASE
        WHEN STANDARD_OR_ITEM = '31D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Surgery'                   THEN 'Surgery'
        WHEN STANDARD_OR_ITEM = '31D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Radiotherapy'              THEN 'Radiotherapy'
        WHEN STANDARD_OR_ITEM = '31D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Anti-cancer drug regimen'  THEN 'Drugs'
        WHEN STANDARD_OR_ITEM = '31D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Other'                     THEN 'Other'
        WHEN STANDARD_OR_ITEM = '62D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Surgery'                   THEN 'Surgery'
        WHEN STANDARD_OR_ITEM = '62D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Radiotherapy'              THEN 'Radiotherapy'
        WHEN STANDARD_OR_ITEM = '62D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Anti-cancer drug regimen'  THEN 'Drugs'
        WHEN STANDARD_OR_ITEM = '62D' AND CANCER_TYPE = 'ALL CANCERS' AND TREATMENT_MODALITY = 'Other'                     THEN 'Other'
        ELSE CANCER_TYPE
    END                                                                     AS CANCER_TYPE,
    TOTAL,
    CASE STANDARD_OR_ITEM
        WHEN 'Urgent Suspected Cancer referral'   THEN WITHIN
        WHEN 'Urgent Breast Symptomatic referral' THEN WITHIN
        ELSE WITHIN_14_DAYS
    END                                 AS WITHIN_14_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN 'Urgent Suspected Cancer referral'   THEN AFTER
        WHEN 'Urgent Breast Symptomatic referral' THEN AFTER
        ELSE NULL
    END                                 AS AFTER_14_DAYS,
    NULL                                AS PERCENTAGE_SEEN_WITHIN_14_DAYS,
    IN_15_TO_16_DAYS,
    IN_17_TO_21_DAYS,
    IN_22_TO_28_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS' THEN AFTER
        ELSE AFTER_28_DAYS
    END                                 AS AFTER_28_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS' THEN WITHIN
        ELSE NULL
    END                                 AS WITHIN_28_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN '31D' THEN WITHIN
        WHEN '62D' THEN WITHIN_31_DAYS
        ELSE NULL
    END                                 AS WITHIN_31_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN '31D' THEN AFTER
        ELSE NULL
    END                                 AS AFTER_31_DAYS,
    NULL                                AS PERCENTAGE_TREATED_WITHIN_31_DAYS,
    IN_32_TO_38_DAYS                    AS WITHIN_32_TO_38_DAYS,
    IN_39_TO_48_DAYS                    AS WITHIN_39_TO_48_DAYS,
    IN_49_TO_62_DAYS                    AS WITHIN_49_TO_62_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS' THEN NULL
        ELSE AFTER_62_DAYS
    END                                 AS AFTER_62_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN '62D' THEN WITHIN
        ELSE NULL
    END                                 AS WITHIN_62_DAYS,
    NULL                                AS PERCENTAGE_TREATED_WITHIN_62_DAYS,
    IN_63_TO_76_DAYS                    AS WITHIN_63_TO_76_DAYS,
    IN_77_TO_90_DAYS                    AS WITHIN_77_TO_90_DAYS,
    IN_91_TO_104_DAYS                   AS WITHIN_91_TO_104_DAYS,
    AFTER_104_DAYS,
    IN_15_TO_28_DAYS,
    IN_29_TO_42_DAYS,
    IN_43_TO_62_DAYS,
    NULL                                AS PERCENTAGE_TOLD_WITHIN_28_DAYS,
    CASE STANDARD_OR_ITEM
        WHEN 'FDS' THEN 0.80
        WHEN '31D' THEN 0.96
        WHEN '62D' THEN 0.85
    END                                 AS TARGET,
    BASIS                               AS ORGANISATION_TYPE,
    _INGESTED_AT                        AS CREATE_TS
FROM DATA_LAKE.PERFORMANCE."CwtMonthlySourceAppendCombined"
WHERE BASIS = 'Provider'
AND CAST(DATEADD(MONTH, -2, DATE_TRUNC('MONTH', _INGESTED_AT)) AS DATE) NOT IN (
    SELECT DISTINCT CAST("ReportDate" AS DATE)
    FROM DATA_LAKE.PMCT."CwtMonthlySourceAppendReviseProv"
)
AND NOT (
    REFERRAL_ROUTE_OR_STAGE = 'ALL ROUTES'
    AND CANCER_TYPE <> 'ALL CANCERS'
)
AND NOT (
    STANDARD_OR_ITEM = '31D'
    AND REFERRAL_ROUTE_OR_STAGE = 'ALL STAGES'
)
AND NOT (
    STANDARD_OR_ITEM = '62D'
    AND REFERRAL_ROUTE_OR_STAGE = 'ALL ROUTES'
    AND TREATMENT_MODALITY <> 'ALL MODALITIES'
)
AND NOT (
    STANDARD_OR_ITEM = '31D'
    AND CANCER_TYPE = 'ALL CANCERS'
    AND TREATMENT_MODALITY = 'ALL MODALITIES'
    AND REFERRAL_ROUTE_OR_STAGE IN ('First Treatment', 'Subsequent')
);