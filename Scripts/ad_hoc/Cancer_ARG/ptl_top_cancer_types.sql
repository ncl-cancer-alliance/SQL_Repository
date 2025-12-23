create or replace view REPORTING.CANCER__ARG.PTL_TOP_CANCER_TYPES(
	RANK_OVER_62,
	REPORTING_DATE,
	ORG_NAME,
	ORG_CODE,
	CANCER_TYPE,
	CANCER_TYPE_SHORT,
	TOTAL_PTL,
	DAYS_MORE_THAN_62
) COMMENT='Version of the Cancer Types PTL table filtered to the top 5 cancer types with more than 62 Days on the PTL.\nContact:jake.kealey@nhs.net'
 as

--Aggregate to combine other cancer types not in top 5
SELECT
    CASE 
        WHEN RANK_OVER_62 <= 5
        THEN RANK_OVER_62
        ELSE 9
    END AS RANK_OVER_62,
    * EXCLUDE (RANK_OVER_62, TOTAL_PTL, DAYS_MORE_THAN_62),
    SUM(TOTAL_PTL) AS TOTAL_PTL,
    SUM(DAYS_MORE_THAN_62) AS DAYS_MORE_THAN_62

FROM (
    --Get top 5 for each org
    SELECT
        REPORTING_DATE,
        RANK() OVER(
            PARTITION BY ORG_CODE 
            ORDER BY
                DAYS_MORE_THAN_62 DESC, 
                TOTAL_PTL ASC
        ) AS RANK_OVER_62,
    
        ORG_NAME,
        ORG_CODE,
        CASE 
            WHEN RANK_OVER_62 <= 5
            THEN CANCER_TYPE
            ELSE 'Other'
        END AS CANCER_TYPE,
        CASE 
            WHEN RANK_OVER_62 <= 5
            THEN CANCER_TYPE_SHORT
            ELSE 'Other'
        END AS CANCER_TYPE_SHORT,
        TOTAL_PTL,
        DAYS_MORE_THAN_62
        
    FROM (
        --Create seperate NCL level data and append to base data
        SELECT orgs.PROVIDER_SHORTHAND AS ORG_NAME, ptl.* 
        FROM REPORTING.CANCER__ARG.PTL_BY_CANCER_TYPE ptl
    
        LEFT JOIN MODELLING.LOOKUP_NCL.NCL_PROVIDER orgs
        ON ptl.ORG_CODE = orgs.REPORTING_CODE
        
        WHERE ORG_CODE IN ('RAL', 'RAN', 'RRV', 'RKE')
    
        AND TOTAL_PTL > 0
    
        UNION ALL
    
        SELECT 
            'NCL' AS ORG_NAME,
            REPORTING_DATE,
            'QMJ' AS ORG_CODE,
            CANCER_TYPE,
            CANCER_TYPE_SHORT,
            SUM(TOTAL_PTL) AS TOTAL_PTL,
            SUM(DAYS_MORE_THAN_62) AS DAYS_MORE_THAN_62
            FROM REPORTING.CANCER__ARG.PTL_BY_CANCER_TYPE
            GROUP BY ALL
    )
)
GROUP BY ALL
ORDER BY ORG_CODE, RANK_OVER_62;