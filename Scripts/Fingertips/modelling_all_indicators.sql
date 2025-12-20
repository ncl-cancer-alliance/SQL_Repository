create or replace dynamic table DEV__MODELLING.FINGERTIPS.INDICATOR_DATA_ALL_AREAS(
	INDICATOR_ID,
	INDICATOR_NAME,
	AREA_ID,
	AREA_TYPE,
	AREA_CODE,
	AREA_NAME,
	VALUE,
	VALUE_UNIT,
	VALUE_TYPE,
	NUMERATOR,
	DENOMINATOR,
    VALUE_CI_LOWER_95,
    VALUE_CI_UPPER_95,
    VALUE_CI_LOWER_99,
    VALUE_CI_UPPER_99,
	SEX,
	AGE,
	CATEGORY_TYPE,
	CATEGORY,
	DATE_INDICATOR,
	DATE_INDICATOR_TYPE,
	DATE_INDICATOR_RANGE,
	DATE_INDICATOR_SORTABLE
) target_lag = '1 day' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Fingertips Indicator data with duplicate records removed.\nContact: jake.kealey@nhs.net'
 as

SELECT
    cf."Indicator ID" AS INDICATOR_ID,
    cf."Indicator Name" AS INDICATOR_NAME,
    cf.AREA_ID,
    cf."Area Type" AS AREA_TYPE,
    cf."Area Code" AS AREA_CODE,
    cf."Area Name" AS AREA_NAME,
    cf."Value" AS VALUE,
    mi."Unit" AS VALUE_UNIT,
    mi."Value type" AS VALUE_TYPE,
    cf."Count" AS NUMERATOR,
    cf."Denominator" AS DENOMINATOR,
    cf."Lower CI 95.0 limit" AS VALUE_CI_LOWER_95,
    cf."Upper CI 95.0 limit" AS VALUE_CI_UPPER_95,
    cf."Lower CI 99.8 limit" AS VALUE_CI_LOWER_99,
    cf."Upper CI 99.8 limit" AS VALUE_CI_UPPER_99,
    cf."Sex" AS SEX,
    cf."Age" AS AGE,
    cf."Category Type" AS CATEGORY_TYPE,
    cf."Category" AS CATEGORY,
    cf."Time period" AS DATE_INDICATOR,
    mi."Year type" AS DATE_INDICATOR_TYPE,
    cf."Time period range" AS DATE_INDICATOR_RANGE,
    cf."Time period Sortable" AS DATE_INDICATOR_SORTABLE

FROM DATA_LAKE__NCL.FINGERTIPS.INDICATOR_DATA cf

--Join to get indicator metadata
LEFT JOIN DATA_LAKE__NCL.FINGERTIPS.METADATA_INDICATOR mi
ON cf."Indicator ID" = mi."Indicator ID"

--Join to get area metadata
LEFT JOIN DATA_LAKE__NCL.FINGERTIPS.METADATA_AREA ma
ON cf.AREA_ID = ma.AREA_ID

--Join to filter on latest records
INNER JOIN DATA_LAKE__NCL.FINGERTIPS.INDICATOR_UPDATE_LOG iul
ON cf."Indicator ID" = iul.INDICATOR_ID
AND cf.AREA_ID = iul.AREA_ID
AND cf._TIMESTAMP = iul._TIMESTAMP
AND iul.IS_LATEST = True

--Filter to remove double England data
WHERE "Area Code" != COALESCE("Parent Code", '');
;