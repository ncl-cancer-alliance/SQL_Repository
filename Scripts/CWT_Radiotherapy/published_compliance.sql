CREATE OR REPLACE VIEW DEV__PUBLISHED_REPORTING__SECONDARY_USE.CANCER__RADIOTHERAPY.COMPLIANCE 
AS
SELECT
    DATE_PERIOD AS "Date Full",
    FIN_YEAR AS "Financial Year",
    FIN_MONTH_NUMBER AS "Financial Month Number",
    FIN_MONTH_NAME AS "Financial Month Name",
    FIN_QUARTER AS "Financial Quarter",
    ORGANISATION_CODE AS "Organisation Code",
    ORGANISATION_NAME AS "Organisation Name",
    IS_BENCHMARK_SUMMARY_GROUP AS "Benchmark Group",
    ORGANISATION_ICB_NAME AS "Organisation Parent ICB",
    CANCER_ALLIANCE AS "Organisation Cancer Alliance",
    RADIOTHERAPY_NETWORK AS "Organisation Radiotherapy Network",
    CANCER_PATHWAY AS "Cancer Pathway",
    NO_PATIENTS AS "Number of Patient Referrals",
    NO_COMPLIANT AS "Number of Compliant Referrals",
    NO_BREACHES AS "Number of Breached Referrals",
    R12_NO_PATIENTS AS "R12 - Patient Referrals",
    R12_NO_COMPLIANT AS "R12 - Compliant Referrals",
    R12_NO_BREACHES AS "R12 - Breached Referrals",
    TARGET AS "Compliance Target",
    D31_DAYS_WITHIN_31 AS "31 Day - Within 31 Days",
    D31_DAYS_32_TO_38 AS "31 Day - 32 to 38 Days",
    D31_DAYS_39_TO_48 AS "31 Day - 39 to 48 Days",
    D31_DAYS_49_TO_62 AS "31 Day - 49 to 62 Days",
    D31_DAYS_MORE_THAN_62 AS "31 Day - More than 62 Days",
    CASE IS_BENCHMARK_SUMMARY_GROUP
        WHEN TRUE THEN CONCAT('0', ORGANISATION_NAME)
        ELSE CONCAT('1', ORGANISATION_NAME)
    END AS "Order Organisation Name"
    
FROM DEV__REPORTING.CANCER__RADIOTHERAPY.COMPLIANCE
WHERE FIN_YEAR >= '2022-23'