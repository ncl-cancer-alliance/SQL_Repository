------------------------------
-- Bowel screening query: 
-- 1) get eliglible population: age 50 to 74, NCL-registered AT MONTH END
-- 2) get bowel screening history records (2.5 years until no longer up-to-date) 
-- 3) join screening history to eligible population (some up-to-date patients will be exlucded from numerator if they had de-registered or died at snapshot month_end)
-- 3) aggregate final table of eligible population with flag for those that are up-to-date for screening. Include patient demographics and goegraphies.
---------------------------------

CREATE OR REPLACE TABLE DEV__REPORTING.CANCER__SCREENING.OLIDS_SCREENING_DASHBOARD_BOWEL_HISTORIC AS (

------- get month end dates to build historic table
WITH month_end AS (

SELECT DISTINCT CAST("EndOfMonthDate"AS DATE) AS DATE_MONTH_END

FROM "Dictionary"."dbo"."Dates"

WHERE "FullDate" >= DATEADD(year, -5, CURRENT_DATE) -- Eddie says that registration data is not accurate beyond 5 years 
AND "EndOfMonthDate" <= (SELECT GLOBAL_DATA_REFRESH_DATE FROM MODELLING.OLIDS_UTILITIES.INT_GLOBAL_DATA_REFRESH_DATE)
),

-------- build monthly eligible population denominator
eligible_population AS (
SELECT mon.DATE_MONTH_END
    ,dem.PERSON_ID

-- calculate the age of each person at the month end for filtering
    ,TRUNC(DATEDIFF(MONTH, dem.BIRTH_DATE_APPROX, mon.DATE_MONTH_END) / 12) AS AGE_AT_MONTH_END

    ,CASE   
        WHEN opt.PERSON_ID IS NOT NULL THEN 1
        ELSE 0
    END AS SECONDARY_USE_ALLOWED
    
    
FROM month_end AS mon

-- join to historic demographics table to get birth dates of all historically registered patients
LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS_HISTORICAL AS dem
-- join to demographic rows where patients were registered at the time of each month_end date then use QUALIFY to take the most recent row. this handles filtering on NCL-regietred patients only and pulls in their birth dates. Registration end date also accounts for those that have died.
ON mon.DATE_MONTH_END BETWEEN TO_DATE(dem.REGISTRATION_START_DATE) AND LEAST(COALESCE(TO_DATE(dem.REGISTRATION_END_DATE), '9999-12-31'),COALESCE(TO_DATE(dem.DEATH_DATE_APPROX), '9999-12-31'))

LEFT JOIN REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED opt
ON dem.PERSON_ID = opt.PERSON_ID


WHERE AGE_AT_MONTH_END BETWEEN 50 AND 74 -- bowel screening eligibility

-- only take the most recent demographic info as one month_end can have > 1 record of changes in demographics_historical table
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dem.PERSON_ID, mon.DATE_MONTH_END 
    ORDER BY TO_DATE(dem.REGISTRATION_START_DATE) DESC
    ) = 1
),


------- get historical screening data and add the screening interval and expiry date for each patient. this will then be joined to the eligible pop.
historical_bowel_screening AS (

SELECT DISTINCT bs.PERSON_ID
    ,DATE_TRUNC('month',TO_DATE(age.BIRTH_DATE_APPROX)) AS BIRTH_MONTH
    ,DATE_TRUNC('month',TO_DATE(bs.CLINICAL_EFFECTIVE_DATE)) AS SCREENING_MONTH
    
-- get age at observation to work out the patient's screening interval period
    ,TRUNC(DATEDIFF(MONTH, age.BIRTH_DATE_APPROX, bs.CLINICAL_EFFECTIVE_DATE) / 12) AS AGE_AT_OBSERVATION -- number of months that have passed converted into years and remove decimals
           
-- assign screening interval period if patient is within the eligible screening age at their screening date
    ,CASE
        WHEN AGE_AT_OBSERVATION BETWEEN 50 AND 74 THEN 30 -- 2.5 years converted to months
        ELSE NULL
    END AS INTERVAL_PERIOD
    
-- get month before which another screening must have taken place for patient to be up-to-date
    ,CAST(DATEADD(MONTH, INTERVAL_PERIOD, SCREENING_MONTH) AS DATE) AS EXPIRY_MONTH
    

FROM MODELLING.OLIDS_PROGRAMME.INT_BOWEL_SCREENING_ALL AS bs

LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_AGE AS age
ON bs.PERSON_ID = age.PERSON_ID


WHERE CLINICAL_EFFECTIVE_DATE >= '2015-01-01' -- pick up screenings > 2.5 years before the earliest date of data
AND CLINICAL_EFFECTIVE_DATE <= CURRENT_DATE
AND bs.IS_COMPLETED_SCREENING = 1
),


---------- join historic bowel screening data to monthly eligible population (where month_ends are >= screening months) and work out which patients are up-to-date at each month_end
eligibility_with_status AS (

SELECT elig.DATE_MONTH_END
    ,elig.PERSON_ID
    ,elig.SECONDARY_USE_ALLOWED
    ,elig.AGE_AT_MONTH_END

-- up-to-date when expiry_month is later than month_end and patient has not died
    ,CASE
        WHEN (MAX(hist.EXPIRY_MONTH) >= elig.DATE_MONTH_END) 
            THEN 1
            ELSE 0 
        END AS IS_UP_TO_DATE

FROM eligible_population AS elig

LEFT JOIN historical_bowel_screening AS hist
ON elig.PERSON_ID = hist.PERSON_ID
-- for each month_end, only join screening records that happened before or on the month_end date
AND elig.DATE_MONTH_END >= hist.SCREENING_MONTH

-- to get death date
LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_AGE AS age
ON hist.PERSON_ID = age.PERSON_ID

GROUP BY ALL -- this collapses any duplicates from historical_bowel_screening
),


----------------------- join demographics to patient-level eligible population with screening status 
final_historic_demographics AS (

SELECT elst.DATE_MONTH_END
    ,elst.PERSON_ID
    ,elst.SECONDARY_USE_ALLOWED
    ,CASE
        WHEN elst.AGE_AT_MONTH_END BETWEEN 50 AND 54 THEN '50-54' 
        WHEN elst.AGE_AT_MONTH_END BETWEEN 55 AND 59 THEN '55-59'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 60 AND 64 THEN '60-64'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 65 AND 69 THEN '65-69'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 70 AND 74 THEN '70-74'
        ELSE NULL
    END AS AGE_GROUP
    ,CASE
        WHEN elst.AGE_AT_MONTH_END = 50 THEN '50' -- PAS team want to seperate individual age groups as they have been recently invited
        WHEN elst.AGE_AT_MONTH_END = 51 THEN '51'
        WHEN elst.AGE_AT_MONTH_END = 52 THEN '52'
        WHEN elst.AGE_AT_MONTH_END = 53 THEN '53'
        WHEN elst.AGE_AT_MONTH_END = 54 THEN '54'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 55 AND 59 THEN '55-59'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 60 AND 64 THEN '60-64'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 65 AND 69 THEN '65-69'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 70 AND 74 THEN '70-74'
        ELSE NULL
    END AS AGE_GROUP_50_54_INDIVIDUAL
    ,COALESCE(dem.GENDER, 'Unknown') AS GENDER
    ,dem.PRACTICE_CODE
    ,dem.PRACTICE_NAME
    ,dem.PCN_NAME
    ,dem.NEIGHBOURHOOD_REGISTERED
    ,dem.BOROUGH_REGISTERED
    ,dem.ETHNICITY_SUBCATEGORY
    ,dem.IMD_DECILE_25
    ,dem.IMD_QUINTILE_NUMERIC_25
    ,dem.IMD_QUINTILE_25
    ,elst.IS_UP_TO_DATE

FROM eligibility_with_status AS elst

LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS_HISTORICAL AS dem
ON elst.PERSON_ID = dem.PERSON_ID
-- join to demographic rows that were true at the time of each month_end date then use QUALIFY to take the most recent row. using effective start and end dates and NCL-registered population filters already applied to remove patient IDs that were not registered at each month end. 
AND elst.DATE_MONTH_END BETWEEN TO_DATE(dem.EFFECTIVE_START_DATE) AND COALESCE(TO_DATE(dem.EFFECTIVE_END_DATE), '9999-12-31')

-- only take the most recent demographic info as one month_end can have > 1 record of changes in demographics_historical table
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY elst.PERSON_ID, elst.DATE_MONTH_END 
    ORDER BY TO_DATE(dem.EFFECTIVE_START_DATE) DESC
    ) = 1
),


----------------- aggregate up from patient level to create smallest table possible for PowerBI
aggregated AS (
SELECT DATE_MONTH_END
    ,GENDER
    ,AGE_GROUP
    ,AGE_GROUP_50_54_INDIVIDUAL
    ,PRACTICE_CODE
    ,PRACTICE_NAME
    ,PCN_NAME
    ,NEIGHBOURHOOD_REGISTERED
    ,BOROUGH_REGISTERED
    ,CASE
        WHEN ETHNICITY_SUBCATEGORY = 'Not Recorded' Then 'Not Known'
        WHEN ETHNICITY_SUBCATEGORY = 'Unknown' Then 'Not Known'
        WHEN ETHNICITY_SUBCATEGORY = 'Not stated' Then 'Not Stated'
        WHEN ETHNICITY_SUBCATEGORY = 'Recorded Not Known' Then 'Not Known'
        WHEN ETHNICITY_SUBCATEGORY = 'Refused' Then 'Not Stated'
        WHEN ETHNICITY_SUBCATEGORY IS NULL Then 'Not Known'
        ELSE ETHNICITY_SUBCATEGORY
    END AS ETHNICITY_SUBCATEGORY
    ,COALESCE(IMD_DECILE_25, 0) AS IMD_DECILE
    ,CASE
        WHEN IMD_DECILE_25 = 1 THEN 'Most Deprived 10%'
        WHEN IMD_DECILE_25 = 2 THEN '2'
        WHEN IMD_DECILE_25 = 3 THEN '3'
        WHEN IMD_DECILE_25 = 4 THEN '4'
        WHEN IMD_DECILE_25 = 5 THEN '5'
        WHEN IMD_DECILE_25 = 6 THEN '6'
        WHEN IMD_DECILE_25 = 7 THEN '7'
        WHEN IMD_DECILE_25 = 8 THEN '8'
        WHEN IMD_DECILE_25 = 9 THEN '9'
        WHEN IMD_DECILE_25 = 10 THEN 'Least Deprived 10%'
        ELSE 'Unknown'
    END AS IMD_DECILE_DESC
    ,COALESCE(IMD_QUINTILE_NUMERIC_25, 0) AS IMD_QUINTILE
    ,COALESCE(IMD_QUINTILE_25, 'Unknown') AS IMD_QUINTILE_DESC
    ,COUNT(*) AS BOWEL_ELIGIBLE_COUNT
    ,SUM(IS_UP_TO_DATE) AS BOWEL_UP_TO_DATE_COUNT
    ,SECONDARY_USE_ALLOWED AS SECONDARY_USE_ALLOWED
    ,MAX(CASE
        WHEN PRACTICE_CODE IN ('F83043', 'F85032') THEN 1 -- IN ORDER: 1) ridgmount, 2) southgate 
        ELSE 0
    END) AS PRACTICE_DQ_ISSUE_FLAG

FROM final_historic_demographics

WHERE DATE_MONTH_END < DATE_TRUNC('MONTH', CURRENT_DATE()) -- only pull data from last month and before to get complete months

GROUP BY ALL)

SELECT  *
FROM aggregated

WHERE PRACTICE_CODE != 'Y03103' -- medicus select care is outside the OLIDs enterprise sharing agreement 
);
