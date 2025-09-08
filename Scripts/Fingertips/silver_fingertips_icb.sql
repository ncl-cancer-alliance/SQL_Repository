-- This filters ICB level NCL fingertips data into a more usable format.
-- Contact: jake.kealey@nhs.net

CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__FINGERTIPS.INDICATOR_DATA_ICB(
    INDICATOR_ID,
    INDICATOR_NAME,
    ICB_CODE,
    ICB_NAME,
    VALUE,
    VALUE_UNIT,
    VALUE_TYPE,
    NUMERATOR,
    DENOMINATOR,
    DATE_INDICATOR,
    DATE_INDICATOR_TYPE,
    DATE_INDICATOR_RANGE,
    DATE_INDICATOR_SORTABLE
)
TARGET_LAG = '2 hours'
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
COMMENT = 'Dynamic table to format ICB Fingertips Data'
AS

SELECT
    cf."Indicator ID" AS INDICATOR_ID,
    cf."Indicator Name" AS INDICATOR_NAME,
    cf."Area Code" AS ICB_CODE,
    cf."Area Name" AS ICB_NAME,
    cf."Value" AS VALUE,
    mi."Unit" AS VALUE_UNIT,
    mi."Value type" AS VALUE_TYPE,
    cf."Count" AS NUMERATOR,
    cf."Denominator" AS DENOMINATOR,
    cf."Time period" AS DATE_INDICATOR,
    mi."Year type" AS DATE_INDICATOR_TYPE,
    cf."Time period range" AS DATE_INDICATOR_RANGE,
    cf."Time period Sortable" AS DATE_INDICATOR_SORTABLE

FROM DATA_LAKE__NCL.CANCER__FINGERTIPS.CANCER_FINGERTIPS cf

--Join to get indicator metadata
LEFT JOIN DATA_LAKE__NCL.CANCER__FINGERTIPS.METADATA_INDICATOR mi
ON cf."Indicator ID" = mi."Indicator ID"

--Join to get area metadata
LEFT JOIN DATA_LAKE__NCL.CANCER__FINGERTIPS.METADATA_AREA ma
ON cf."Area Type" = ma."Short"

--Join to filter on latest records
INNER JOIN DATA_LAKE__NCL.CANCER__FINGERTIPS.INDICATOR_UPDATE_LOG iul
ON cf."Indicator ID" = iul.INDICATOR_ID
AND cf.DATE_UPDATED_LOCAL = iul.DATE_UPDATED_LOCAL
AND iul.IS_LATEST = True

--Filter to ICB data
WHERE ma.AREA_ID = 221
AND cf."Area Code" = 'nE54000028'