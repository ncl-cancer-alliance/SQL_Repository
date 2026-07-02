-------------------------------
-- Cervical screening query: 
-- 1) get eliglible population: aged 25-64 AT MONTH_END, female, NCL-registered AT MONTH END 
-- 2) get screening history records (3.5 or 5.5 years until no longer up-to-date)
-- 3) join screening history to eligible population (some up-to-date patients will be exlucded from numerator if they had de-registered or died at snapshot month_end)
-- 3) aggregate final table of eligible population with flag for those that are up-to-date for screening. Include patient demographics and geographies.
----------------------------------
CREATE OR REPLACE TABLE DEV__REPORTING.CANCER__SCREENING.OLIDS_SCREENING_DASHBOARD_CERVICAL_HISTORIC AS (

------- get month end dates to build historic table
WITH month_end AS (

SELECT DISTINCT CAST("EndOfMonthDate"AS DATE) AS DATE_MONTH_END

FROM "Dictionary"."dbo"."Dates"

WHERE "FullDate" >= DATEADD(year, -5, CURRENT_DATE) -- Eddie says that registration data is not accurate beyond 5 years 
AND "EndOfMonthDate" <= (SELECT GLOBAL_DATA_REFRESH_DATE FROM MODELLING.OLIDS_UTILITIES.INT_GLOBAL_DATA_REFRESH_DATE)
)

-------- build monthly eligible population denominator 
,eligible_population AS (
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
-- join to demographic rows where patients were registered at the time of each month_end date then use QUALIFY to take the most recent row. this handles filtering on NCL-regietred patients only and pulls in their birth dates
ON mon.DATE_MONTH_END BETWEEN TO_DATE(dem.REGISTRATION_START_DATE) AND LEAST(COALESCE(TO_DATE(dem.REGISTRATION_END_DATE), '9999-12-31'),COALESCE(TO_DATE(dem.DEATH_DATE_APPROX), '9999-12-31'))

LEFT JOIN REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED AS opt
ON dem.PERSON_ID = opt.PERSON_ID

WHERE dem.GENDER = 'Female' -- cervical screening eligibility
AND AGE_AT_MONTH_END BETWEEN 25 AND 64 -- cervical screening eligibility

-- only take the most recent demographic info as one month_end can have > 1 record of changes in demographics_historical table
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dem.PERSON_ID, mon.DATE_MONTH_END 
    ORDER BY TO_DATE(dem.REGISTRATION_START_DATE) DESC
    ) = 1
)

----- get the lastest screening status dates as of each month end - needed for unsuitable logic
,screening_history_asof_month AS (

SELECT
    elig.DATE_MONTH_END
    ,elig.PERSON_ID
    
    -- latest completed up to month_end
    ,MAX(CASE 
        WHEN IS_COMPLETED_SCREENING= TRUE
             AND TO_DATE(cs.CLINICAL_EFFECTIVE_DATE) <= elig.DATE_MONTH_END
             AND TO_DATE(cs.CLINICAL_EFFECTIVE_DATE) > DATE '1960-01-01'-- removed DQ issues where screening dates are recorded as happening on '1900-01-01'
             AND TRUNC(DATEDIFF(MONTH, age.BIRTH_DATE_APPROX, cs.CLINICAL_EFFECTIVE_DATE) / 12) > 23 -- remove people who are screened younger than age 24
        THEN TO_DATE(cs.CLINICAL_EFFECTIVE_DATE)
    END) AS latest_completed_date

    -- latest unsuitable up to month_end
    ,MAX(CASE 
        WHEN cs.IS_UNSUITABLE_SCREENING = 1 
             AND TO_DATE(cs.CLINICAL_EFFECTIVE_DATE) <= elig.DATE_MONTH_END
             AND TO_DATE(cs.CLINICAL_EFFECTIVE_DATE) > DATE '1960-01-01'-- removed DQ issues where screening dates are recorded as happening on '1900-01-01'
        THEN TO_DATE(cs.CLINICAL_EFFECTIVE_DATE)
    END) AS latest_unsuitable_date

FROM eligible_population AS elig

LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_CERVICAL_SCREENING_ALL AS cs
ON elig.PERSON_ID = cs.PERSON_ID
AND TO_DATE(cs.CLINICAL_EFFECTIVE_DATE) <= elig.DATE_MONTH_END

LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_AGE AS age
ON elig.PERSON_ID = age.PERSON_ID

GROUP BY
    elig.DATE_MONTH_END,
    elig.PERSON_ID
)


------- get historical screening data and add the screening interval and expiry date for each patient
,historical_cerv_screening AS (

SELECT DISTINCT cs.PERSON_ID
    ,DATE_TRUNC('month',TO_DATE(cs.CLINICAL_EFFECTIVE_DATE)) AS SCREENING_MONTH
    
-- get age at observation to work out the patient's screening interval period
    ,TRUNC(DATEDIFF(MONTH, age.BIRTH_DATE_APPROX, cs.CLINICAL_EFFECTIVE_DATE) / 12) AS AGE_AT_OBSERVATION -- number of months that have passed converted into years and remove decimals
           
-- assign screening interval period depending on age
    ,CASE
        WHEN AGE_AT_OBSERVATION BETWEEN 24 AND 49 THEN 42 -- 3.5 years converted to months. catch 24 year olds that become eligible at month_ends in the future e.g. any person that has been screened at 23 years old AREN't included as the national screening programme doesn't invite people for a smear until they are 24 and this will have been for another reason.
        WHEN AGE_AT_OBSERVATION BETWEEN 50 AND 64 THEN 66 -- 5.5 years converted to months
        ELSE NULL
    END AS INTERVAL_PERIOD
    
-- get month before which another screening must have taken place for patient to be up-to-date
    ,CAST(DATEADD(MONTH, INTERVAL_PERIOD, SCREENING_MONTH) AS DATE) AS EXPIRY_MONTH
    

FROM MODELLING.OLIDS_PROGRAMME.INT_CERVICAL_SCREENING_ALL AS cs

LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_AGE AS age
ON cs.PERSON_ID = age.PERSON_ID

WHERE CLINICAL_EFFECTIVE_DATE >= '2012-01-01' -- pick up screenings 5.5 years before the earliest date of data
AND CLINICAL_EFFECTIVE_DATE <= CURRENT_DATE
AND cs.IS_COMPLETED_SCREENING = 1
)


---------- join expiry logic to determine which eligible patients are up-to-date at each month end (joining screening history to each month end where month_end >= screening history)
,eligibility_with_status AS (

SELECT elig.DATE_MONTH_END
    ,elig.PERSON_ID
    ,elig.SECONDARY_USE_ALLOWED
    ,elig.AGE_AT_MONTH_END

    ,TO_DATE(sh.latest_completed_date)
    ,TO_DATE(sh.latest_unsuitable_date)

   -- unsuitable logic: patient has an unsuitable date and this is more recent (according to month_end) that the latest completed screening date 
    ,CASE
        WHEN TO_DATE(sh.latest_unsuitable_date) IS NOT NULL
             AND (
                 TO_DATE(sh.latest_completed_date) IS NULL
                 OR TO_DATE(sh.latest_unsuitable_date) > TO_DATE(sh.latest_completed_date)
             )
        THEN 1
        ELSE 0
    END AS IS_UNSUITABLE

    -- up-to-date logic
    ,CASE
        WHEN (MAX(hist.EXPIRY_MONTH) >= elig.DATE_MONTH_END)
            AND (
                (MAX(age.DEATH_DATE_APPROX) > elig.DATE_MONTH_END)
                OR MAX(age.DEATH_DATE_APPROX) IS NULL
            )
        -- make sure someone cannot be both unsuitable and screened in the same month. i.e. up-to-date only if NOT unsuitable
            AND NOT (
             TO_DATE(sh.latest_unsuitable_date) IS NOT NULL
             AND (
                 TO_DATE(sh.latest_completed_date) IS NULL
                 OR TO_DATE(sh.latest_unsuitable_date) > TO_DATE(sh.latest_completed_date)
             )
         )
        THEN 1
        ELSE 0
    END AS IS_UP_TO_DATE

    
FROM eligible_population AS elig


LEFT JOIN screening_history_asof_month AS sh
ON elig.PERSON_ID = sh.PERSON_ID
AND elig.DATE_MONTH_END = sh.DATE_MONTH_END

LEFT JOIN historical_cerv_screening AS hist
ON elig.PERSON_ID = hist.PERSON_ID
-- for each month_end, only join screening records that were before or on the month_end date
AND elig.DATE_MONTH_END >= hist.SCREENING_MONTH 

-- to get death date
LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_AGE AS age
ON hist.PERSON_ID = age.PERSON_ID

GROUP BY ALL -- this collapses any duplicates from historical_cerv_screening
)

----------------------- join demographics to patient-level up-to-date data
,final_historic_demographics AS (

SELECT elst.DATE_MONTH_END
    ,elst.PERSON_ID
    ,elst.SECONDARY_USE_ALLOWED
    ,CASE
        WHEN elst.AGE_AT_MONTH_END BETWEEN 25 AND 29 THEN '25-29'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 30 AND 34 THEN '30-34'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 35 AND 39 THEN '35-39'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 40 AND 44 THEN '40-44'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 45 AND 49 THEN '45-49'
        WHEN elst.AGE_AT_MONTH_END = 50 THEN '50' -- PAS team want to seperate individual age groups for bowel as they have been recently invited
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
    ,CASE
        WHEN elst.AGE_AT_MONTH_END BETWEEN 25 AND 29 THEN '25-29'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 30 AND 34 THEN '30-34'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 35 AND 39 THEN '35-39'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 40 AND 44 THEN '40-44'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 45 AND 49 THEN '45-49'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 50 AND 54 THEN '50-54'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 55 AND 59 THEN '55-59'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 60 AND 64 THEN '60-64'
        ELSE NULL
    END AS AGE_GROUP_AT_MONTH_END
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
    ,elst.IS_UNSUITABLE

FROM eligibility_with_status AS elst

LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS_HISTORICAL AS dem
ON elst.PERSON_ID = dem.PERSON_ID
-- join to demographic rows that were true at the time of each month_end date then use QUALIFY to take the most recent row. using effective start and end dates and NCL-registered population filters already applied to remove patient IDs that were not registered at each month end. 
AND elst.DATE_MONTH_END BETWEEN TO_DATE(dem.EFFECTIVE_START_DATE) AND COALESCE(TO_DATE(dem.EFFECTIVE_END_DATE), '9999-12-31')

-- only take the most recent demographic info as one month_end can be joined to > 1 record in demographics_historical table
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY elst.PERSON_ID, elst.DATE_MONTH_END 
    ORDER BY TO_DATE(dem.EFFECTIVE_START_DATE) DESC
    ) = 1
)

----------------- aggregate up from patient level to create final table for PowerBI
,aggregated AS (
SELECT DATE_MONTH_END
    ,AGE_GROUP_50_54_INDIVIDUAL
    ,AGE_GROUP_AT_MONTH_END
    ,PRACTICE_CODE
    ,PRACTICE_NAME
    ,PCN_NAME
    ,NEIGHBOURHOOD_REGISTERED
    ,BOROUGH_REGISTERED
    ,CASE
        WHEN ETHNICITY_SUBCATEGORY = 'Not Recorded' Then 'Unknown'
        WHEN ETHNICITY_SUBCATEGORY = 'Unknown' Then 'Unknown'
        WHEN ETHNICITY_SUBCATEGORY = 'Not stated' Then 'Unknown'
        WHEN ETHNICITY_SUBCATEGORY = 'Not Stated' Then 'Unknown'
        WHEN ETHNICITY_SUBCATEGORY = 'Recorded Not Known' Then 'Unknown'
        WHEN ETHNICITY_SUBCATEGORY IS NULL Then 'Unknown'
        ELSE ETHNICITY_SUBCATEGORY
    END AS ETHNICITY_SUBCATEGORY
    ,COALESCE(IMD_DECILE_25,0) AS IMD_DECILE
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
    ,COALESCE(IMD_QUINTILE_NUMERIC_25,0) AS IMD_QUINTILE
    ,COALESCE(IMD_QUINTILE_25, 'Unknown') AS IMD_QUINTILE_DESC
    ,COUNT(*) AS CERVICAL_ELIGIBLE_COUNT
    ,SUM(IS_UP_TO_DATE) AS CERVICAL_UP_TO_DATE_COUNT
    ,SUM(IS_UNSUITABLE) AS CERVICAL_UNSUITABLE_COUNT
    ,MAX(SECONDARY_USE_ALLOWED) AS SECONDARY_USE_ALLOWED
    ,MAX(CASE
        WHEN PRACTICE_CODE IN ('F83043', 'F85032') THEN 1 -- IN ORDER: 1) ridgmount, 2) southgate 
        ELSE 0
    END) AS PRACTICE_DQ_ISSUE_FLAG

FROM final_historic_demographics

WHERE DATE_MONTH_END < DATE_TRUNC('MONTH', CURRENT_DATE()) -- only pull data from last month and before to get complete months


GROUP BY ALL)

SELECT *
FROM aggregated

WHERE PRACTICE_CODE != 'Y03103' -- medicus select care is outside the OLIDs enterprise sharing agreement 
)