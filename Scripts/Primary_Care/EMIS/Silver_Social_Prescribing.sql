-- Dynamic table to clean EMIS Social Prescriber Referral data..
-- Contact: eric.pinto@nhs.net

create or replace dynamic table DEV__MODELLING.CANCER__EMIS.SOCIAL_PRESCRIBING(
	"ORGANISATION",
	"CDB",
	"POPULATION_COUNT",
	"PARENT_COUNT",
	"MALES",
	"FEMALES",
	"EXCLUDED",
	"STATUS",
	"DATE_FULL",
    "DATE_TYPE",
	"TIMESTAMP"

) target_lag = '2 hours' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to clean EMIS Social Prescriber Referral data.'
 as

 SELECT
	"Organisation" AS ORGANISATION,
	CDB,
	"Population Count" AS POPULATION_COUNT,
	"Parent" AS PARENT_COUNT,
	"Males" AS MALES,
	"Females" AS FEMALES,
	"Excluded" AS EXCLUDED,
	"Status" AS STATUS,
     TO_DATE("_Year" || '-' || LPAD("_Month", 2, '0') || '-01') AS "DATE_FULL",
     'Monthly' AS DATE_TYPE,
	"_TIMESTAMP"
FROM DATA_LAKE.CANCER__EMIS.SOCIAL_PRESCRIBER_REFERRALS