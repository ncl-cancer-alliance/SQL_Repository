-- This filters GP level NCL fingertips data into a more usable format.
-- Contact: jake.kealey@nhs.net

CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__FINGERTIPS.INDICATOR_DATA_GP(
    INDICATOR_ID,
    INDICATOR_NAME,
    GP_PRACTICE_CODE,
    GP_PRACTICE_DESC,
    GP_PRACTICE_DESC_SHORT,
    PCN_CODE,
    PCN_NAME,
    BOROUGH_NAME,
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
COMMENT = 'Dynamic table to format GP Fingertips Data'
AS

SELECT
    cf."Indicator ID" AS INDICATOR_ID,
    cf."Indicator Name" AS INDICATOR_NAME,
    cf."Area Code" AS GP_PRACTICE_CODE,
    gp_ref.GP_PRACTICE_DESC,
    gp_ref.GP_PRACTICE_DESC_SHORT,
    gp_ref.PCN_CODE,
    gp_ref.PCN_NAME,
    gp_ref.BOROUGH AS BOROUGH_NAME,
    cf."Value" AS VALUE,
    mi."Unit" AS VALUE_UNIT,
    mi."Value type" AS VALUE_TYPE,
    cf."Count" AS NUMERATOR,
    cf."Denominator" AS DENOMINATOR,
    cf."Time period" AS DATE_INDICATOR,
    mi."Year type" AS DATE_INDICATOR_TYPE,
    cf."Time period range" AS DATE_INDICATOR_RANGE,
    cf."Time period Sortable" AS DATE_INDICATOR_SORTABLE

FROM DATA_LAKE.CANCER__FINGERTIPS.CANCER_FINGERTIPS cf

--Join to get indicator metadata
LEFT JOIN DATA_LAKE.CANCER__FINGERTIPS.METADATA_INDICATOR mi
ON cf."Indicator ID" = mi."Indicator ID"

--Join to get area metadata
LEFT JOIN DATA_LAKE.CANCER__FINGERTIPS.METADATA_AREA ma
ON cf."Area Type" = ma."Short"

--Join to filter on latest records
INNER JOIN DATA_LAKE.CANCER__FINGERTIPS.INDICATOR_UPDATE_LOG iul
ON cf."Indicator ID" = iul.INDICATOR_ID
AND cf.DATE_UPDATED_LOCAL = iul.DATE_UPDATED_LOCAL
AND iul.IS_LATEST = True

--Join to get NCL-Practice Information
--NOTE THIS IS A PLACEHOLDER UNTIL FINAL GP REFERENCE TABLES ARE AVAILABLE
LEFT JOIN (
    SELECT DISTINCT
        GP_PRACTICE_CODE,
        GP_PRACTICE_DESC,
        GP_PRACTICE_DESC_SHORT,
        PCN_CODE,
        PCN_NAME,
        BOROUGH
    FROM MODELLING.LOOKUP_NCL.GP_PRACTICES
) gp_ref
ON cf."Area Code" = gp_ref.GP_PRACTICE_CODE

--Filter to GP data
WHERE ma.AREA_ID = 7
--Filter to NCL data
AND gp_ref.GP_PRACTICE_CODE IS NOT NULL