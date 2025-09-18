-- Dynamic table to clean and join FIT Monthly and Quarterly data.
-- Contact: eric.pinto@nhs.net

create or replace dynamic table DEV__MODELLING.CANCER__EMIS.FIT(
	"ORGANISATION",
	"CDB",
	"POPULATION_COUNT",
	"PARENT_COUNT",
	"PERFORMANCE",
	"MALES",
	"FEMALES",
	"EXCLUDED",
	"STATUS",
	"DATE_FULL",
    "DATE_TYPE",
	"TIMESTAMP"

) target_lag = '2 hours' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to clean and join FIT Monthly and Quarterly data.'
 as
SELECT
	"Organisation" AS ORGANISATION,
	CDB,
	"Population Count" AS POPULATION_COUNT,
	"Parent" AS PARENT_COUNT,
	"%" AS PERFORMANCE,
	"Males" AS MALES,
	"Females" AS FEMALES,
	"Excluded" AS EXCLUDED,
	"Status" AS STATUS,
     TO_DATE("_Year" || '-' || LPAD("_Month", 2, '0') || '-01') AS "DATE_FULL",
     'Monthly' AS DATE_TYPE,
	"_TIMESTAMP"
 FROM DATA_LAKE__NCL.CANCER__EMIS.FIT_MONTHLY

 UNION ALL

  SELECT
	"Organisation" AS ORGANISATION,
	CDB,
	"Population Count" AS POPULATION_COUNT,
	"Parent" AS PARENT,
	"%" AS PERFORMANCE,
	"Males" AS MALES,
	"Females" AS FEMALES,
	"Excluded" AS EXCLUDED,
	"Status" AS STATUS,
     TO_DATE("_Year" || '-' || LPAD("_Month", 2, '0') || '-01') AS "DATE_FULL",
     'Quarterly' AS DATE_TYPE,
	"_TIMESTAMP"
 FROM DATA_LAKE__NCL.CANCER__EMIS.FIT_QUARTERLY
