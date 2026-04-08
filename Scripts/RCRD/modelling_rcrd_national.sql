/*
Script: DEV__MODELLING.CANCER__RCRD.RCRD_NATIONAL

Description:
Dynamic table to standardise RCRD National column names

Author: Eric Pinto

Target Output Tables:
 - DEV__MODELLING.CANCER__RCRD.RCRD_NATIONAL
*/

create or replace dynamic table DEV__MODELLING.CANCER__RCRD.RCRD_NATIONAL(
	GEOGRAPHY_TYPE,
	GEOGRAPHY_NAME,
	DATE_FULL,
	CANCER_GROUP,
	CANCER_GROUP_BROAD,
	CANCER_GROUP_DETAILED,
	METRIC_NAME,
	BREAKDOWN_NAME,
	DEMOGRAPHIC_NAME,
	COMPLETENESS_TREATMENT_FOLLOWUP,
	NUMERATOR,
	DENOMINATOR,
	STATISTIC,
	NUMERATOR_12M,
	DENOMINATOR_12M,
	STATISTIC_12M
) target_lag = '2 hours' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to standardise RCRD National column names\n\nContact: eric.pinto@nhs.net'
 as

SELECT 

	"Geography type" AS GEOGRAPHY_TYPE,
	"Geography" AS GEOGRAPHY_NAME,
    TO_DATE("Date" || '-01') AS "DATE_FULL",
    "Cancer group",
    "Cancer group (broad)" AS CANCER_GROUP_BROAD,
    "Cancer group (detailed)" AS CANCER_GROUP_DETAILED,
    "Metric" AS METRIC_NAME,
    "Breakdown" AS BREAKDOWN_NAME,
    "Demographic" AS DEMOGRAPHIC_NAME,
    "Completeness treatment follow-up" AS COMPLETENESS_TREATMENT_FOLLOWUP,
    "Numerator",
    "Denominator",
    "Statistic",
    "Numerator (12m)" AS NUMERATOR_12M,
    "Denominator (12m)" AS DENOMINATOR_12M,
    "Statistic (12m)" AS STATISTIC_12M

FROM DATA_LAKE__NCL.CANCER__RCRD_EVENTS.NATIONAL__RCRD_INC_TRT;