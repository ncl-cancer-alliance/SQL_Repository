
CREATE OR REPLACE VIEW DEV__REPORTING.CANCER__RADIOTHERAPY.YEARLY_ACTIVITY 
COMMENT = "View to get yearly activity and calculate year on year growth. Contact: jake.kealey@nhs.net"
AS

--CTE to get Aggregate data for Radiotherapy Networks
WITH YEAR_AGG AS (
SELECT 
    FIN_YEAR, RADIOTHERAPY_NETWORK, CANCER_PATHWAY, 
    SUM(NO_PATIENTS) AS NO_PATIENTS, 
    CAST(RIGHT(FIN_YEAR, 2) AS INT) AS FIN_YEAR_NUM
FROM DEV__REPORTING.CANCER__RADIOTHERAPY.COMPLIANCE
WHERE RADIOTHERAPY_NETWORK IS NOT NULL
GROUP BY ALL
),

--CTE to determine if the latest financial year is incomplete (and should be excluded from the year on year growth calculation)
FILTER_LATEST AS (
    SELECT
        --If latest data is in March then the latest financial year is complete
        CASE 
            WHEN MONTH(MAX(DATE_PERIOD)) = 3 THEN NULL  --Filter nothing
            ELSE MAX(FIN_YEAR)                          --Filter latest financial year
        END AS FILTER_YEAR
    FROM DEV__REPORTING.CANCER__RADIOTHERAPY.COMPLIANCE
    GROUP BY ALL
)

SELECT 
    base.FIN_YEAR, 
    base.RADIOTHERAPY_NETWORK, 
    base.CANCER_PATHWAY,
    base.NO_PATIENTS AS NO_PATIENTS_CUR,
    prev.NO_PATIENTS AS NO_PATIENTS_PREV
FROM YEAR_AGG base

--Join to get previous year's figures
LEFT JOIN YEAR_AGG prev
ON base.RADIOTHERAPY_NETWORK = prev.RADIOTHERAPY_NETWORK
AND base.CANCER_PATHWAY = prev.CANCER_PATHWAY
AND base.FIN_YEAR_NUM = prev.FIN_YEAR_NUM + 1
AND NOT(
    --Filter out the latest year if incomplete
    base.FIN_YEAR IN (SELECT * FROM FILTER_LATEST)
    --Filter out the incomplete 2023-24 year for non-Subsequent data
    OR (
        prev.FIN_YEAR = '2023-24' AND prev.CANCER_PATHWAY != 'Subsequent'
    )
)

ORDER BY base.FIN_YEAR, RADIOTHERAPY_NETWORK;