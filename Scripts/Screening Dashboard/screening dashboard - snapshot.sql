-------------------------------
-- New snapshot query to use the latest month in our cervical and bowel historic tables rather than pre-made one in OLIDs
-- 1) build cervical latest month_end snapshot
-- 2) build bowel latest month_end snapshot
-- 3) union two month_end snapshots together 
----------------------------------
CREATE OR REPLACE TABLE DEV__REPORTING.CANCER__SCREENING.OLIDS_SCREENING_DASHBOARD_SNAPSHOT AS (

------- get latest month_end date
WITH month_end AS (

SELECT DISTINCT CAST("EndOfMonthDate"AS DATE) AS DATE_MONTH_END

FROM "Dictionary"."dbo"."Dates"

WHERE "EndOfMonthDate" = (SELECT MAX(DATE_MONTH_END) FROM DEV__REPORTING.CANCER__SCREENING.OLIDS_SCREENING_DASHBOARD_CERVICAL_HISTORIC)
)

-----------------------------------------------
-- 1) build cervical latest month_end snapshot
------------------------------------------------

-------- build cervical eligible population denominator 
,cervical_eligible_population AS (

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
-- join to demographic rows which are relevant at the time of month_end date then use QUALIFY to take the most recent row. This join handles filtering on NCL-regietred patients only and patients that are are not dead
ON mon.DATE_MONTH_END BETWEEN TO_DATE(dem.REGISTRATION_START_DATE) AND LEAST(COALESCE(TO_DATE(dem.REGISTRATION_END_DATE), '9999-12-31'),COALESCE(TO_DATE(dem.DEATH_DATE_APPROX), '9999-12-31'))

LEFT JOIN REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED AS opt
ON dem.PERSON_ID = opt.PERSON_ID

WHERE dem.GENDER = 'Female' -- cervical screening eligibility
AND AGE_AT_MONTH_END BETWEEN 25 AND 64 -- cervical screening eligibilitY
AND mon.DATE_MONTH_END = (SELECT MAX(DATE_MONTH_END) FROM DEV__REPORTING.CANCER__SCREENING.OLIDS_SCREENING_DASHBOARD_CERVICAL_HISTORIC) -- dynamic to make sure only the most recent month end is used matching the cervical historic script)

-- only take the most recent demographic info as one month_end can have > 1 record of changes in demographics_historical table
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dem.PERSON_ID, mon.DATE_MONTH_END 
    ORDER BY TO_DATE(dem.REGISTRATION_START_DATE) DESC
    ) = 1
)

----- get the lastest screening status dates at month end - this is required for the 'unsuitable' logic that cervical has but bowel does not.
,cervical_screening_history_asof_month AS (

SELECT
    elig.DATE_MONTH_END
    ,elig.PERSON_ID

    -- latest completed up to month_end
    ,MAX(CASE 
        WHEN cs.IS_COMPLETED_SCREENING = TRUE
             AND TO_DATE(cs.CLINICAL_EFFECTIVE_DATE) <= elig.DATE_MONTH_END
             AND TO_DATE(cs.CLINICAL_EFFECTIVE_DATE) > DATE '1960-01-01'-- removes DQ issues where screening dates are recorded as happening on '1900-01-01' 
             AND TRUNC(DATEDIFF(MONTH, age.BIRTH_DATE_APPROX, cs.CLINICAL_EFFECTIVE_DATE) / 12) > 23 -- remove people who are screened younger than age 24 as this would not count as part of the national screening programme.
        THEN TO_DATE(cs.CLINICAL_EFFECTIVE_DATE)
    END) AS latest_completed_date

    -- latest unsuitable up to month_end
    ,MAX(CASE 
        WHEN cs.IS_UNSUITABLE_SCREENING = 1 
             AND TO_DATE(cs.CLINICAL_EFFECTIVE_DATE) <= elig.DATE_MONTH_END
             AND TO_DATE(cs.CLINICAL_EFFECTIVE_DATE) > DATE '1960-01-01'-- removed DQ issues where screening dates are recorded as happening on '1900-01-01'
        THEN TO_DATE(cs.CLINICAL_EFFECTIVE_DATE)
    END) AS latest_unsuitable_date

FROM cervical_eligible_population AS elig

LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_CERVICAL_SCREENING_ALL AS cs
ON elig.PERSON_ID = cs.PERSON_ID
AND TO_DATE(cs.CLINICAL_EFFECTIVE_DATE) <= elig.DATE_MONTH_END

LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_AGE AS age
ON elig.PERSON_ID = age.PERSON_ID

GROUP BY
    elig.DATE_MONTH_END,
    elig.PERSON_ID
)

------- get historical cervical screening data and add the screening interval and expiry date for each record 
,cervical_historical_screening AS (

SELECT DISTINCT cs.PERSON_ID
    ,DATE_TRUNC('month',TO_DATE(cs.CLINICAL_EFFECTIVE_DATE)) AS SCREENING_MONTH
    
-- get age at observation to work out the patient's screening interval period
    ,TRUNC(DATEDIFF(MONTH, age.BIRTH_DATE_APPROX, cs.CLINICAL_EFFECTIVE_DATE) / 12) AS AGE_AT_OBSERVATION -- number of months that have passed converted into years and remove decimals
           
-- assign screening interval period depending on age
    ,CASE
        WHEN AGE_AT_OBSERVATION BETWEEN 24 AND 49 THEN 42 -- 3.5 years converted to months. catch 24 year olds that are screened as part of the programme (the programme invites poeple from age 24.5) before the eligibility criteria begin and then become eligible at month_ends in the future e.g. any person that has been screened at 23 years old AREN'T included as the national screening programme doesn't invite people for a smear until they are 24 and this will have been for another reason.
        WHEN AGE_AT_OBSERVATION BETWEEN 50 AND 64 THEN 66 -- 5.5 years converted to months
        ELSE NULL
    END AS INTERVAL_PERIOD
    
-- get month before which another screening must have taken place for patient to be up-to-date
    ,CAST(DATEADD(MONTH, INTERVAL_PERIOD, SCREENING_MONTH) AS DATE) AS EXPIRY_MONTH

-- get death date
    ,age.DEATH_DATE_APPROX

FROM MODELLING.OLIDS_PROGRAMME.INT_CERVICAL_SCREENING_ALL AS cs

LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_AGE AS age
ON cs.PERSON_ID = age.PERSON_ID

WHERE CLINICAL_EFFECTIVE_DATE >= '1960-01-01' -- pick up screenings far back so you you can distinguish overdue from never screened in early months
AND CLINICAL_EFFECTIVE_DATE <= CURRENT_DATE
AND cs.IS_COMPLETED_SCREENING = 1
)


---------- join expiry logic to determine which eligible patients are up-to-date at month_end and to determine what the latest_completed and latest_unsuitable dates are as these need to be compared to determine unsuitability.
,cervical_eligibility_with_status AS (

SELECT DISTINCT elig.DATE_MONTH_END
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

-- up-to-date logic: expiriy month is later than the month end and they are not unsuitable
    ,CASE
        WHEN (MAX(hist.EXPIRY_MONTH) >= elig.DATE_MONTH_END)
        -- make sure patient is not dead
            AND (
                (MAX(age.DEATH_DATE_APPROX) > elig.DATE_MONTH_END)
                OR MAX(age.DEATH_DATE_APPROX) IS NULL
            )
-- make sure someone cannot be both unsuitable AND up to date i.e. up-to-date only if NOT unsuitable
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

-- overdue logic: patient has been screened and the expiry date is before month_end 
     ,CASE
        WHEN TO_DATE(sh.latest_completed_date) IS NOT NULL
            AND (MAX(hist.EXPIRY_MONTH) < elig.DATE_MONTH_END)
-- make sure someone cannot be both unsuitable and overdue i.e. overdue only if NOT unsuitable
            AND NOT (
             TO_DATE(sh.latest_unsuitable_date) IS NOT NULL
             AND (
                 TO_DATE(sh.latest_completed_date) IS NULL
                 OR TO_DATE(sh.latest_unsuitable_date) > TO_DATE(sh.latest_completed_date)
               )
               )
            THEN 1
            ELSE 0
        END AS IS_OVERDUE

-- never screened logic: patient has never had a record of completed_screening
    ,CASE 
        WHEN TO_DATE(sh.latest_completed_date) IS NULL AND TO_DATE(sh.latest_unsuitable_date) IS NULL 
            THEN 1 
        ELSE 0
    END AS IS_NEVER_SCREENED
    
    
FROM cervical_eligible_population AS elig


LEFT JOIN cervical_screening_history_asof_month AS sh
ON elig.PERSON_ID = sh.PERSON_ID
AND elig.DATE_MONTH_END = sh.DATE_MONTH_END

LEFT JOIN cervical_historical_screening AS hist
ON elig.PERSON_ID = hist.PERSON_ID
-- only join screening records that were before or on the month_end date
AND elig.DATE_MONTH_END >= hist.SCREENING_MONTH 

-- to get death date
LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_AGE AS age
ON hist.PERSON_ID = age.PERSON_ID

GROUP BY ALL -- this collapses any duplicates from cervical_historical_screening
)

---------------- flag for attended appointments in last 3,5,10 years 
,attended_appointment AS (
SELECT
    a.PERSON_ID

    ,CASE 
        WHEN MAX(a.START_DATE) >= CURRENT_DATE() - INTERVAL '3 years' 
            THEN 'Within the last 3 years'
        WHEN MAX(a.START_DATE) >= CURRENT_DATE() - INTERVAL '5 years' 
            THEN 'Within the last 5 years'
        WHEN MAX(a.START_DATE) >= CURRENT_DATE() - INTERVAL '10 years' 
            THEN 'Within the last 10 years'
        ELSE 'Longer than 10 years ago'
    END AS LATEST_GP_APPOINTMENT_ATTENDED

FROM STAGING.OLIDS.STG_OLIDS_APPOINTMENT AS a

WHERE a.START_DATE <= CURRENT_DATE()
  AND APPOINTMENT_STATUS_CODE = 5 -- appointment was attended

GROUP BY
    a.PERSON_ID)

----------------------- join demographics to patient-level up-to-date data
,cervical_final_historic_demographics AS (

SELECT 'Cervical' AS SCREENING_PROGRAMME
    ,ref.GLOBAL_DATA_REFRESH_DATE AS DATE_DATA_REFRESH
    ,elst.DATE_MONTH_END -- instead of DATE_DASHBOARD_QUERY_REFRESH
    ,elst.PERSON_ID
    ,CASE
        WHEN IS_UP_TO_DATE = 1 THEN 'Up to Date'
        WHEN IS_UNSUITABLE = 1 THEN 'Unsuitable'
        WHEN IS_OVERDUE = 1 THEN 'Overdue'
        WHEN IS_NEVER_SCREENED = 1 THEN 'Never Screened'
        ELSE 'Other'
    END AS SCREENING_STATUS
    ,CASE
        WHEN (IS_OVERDUE = 1 OR IS_NEVER_SCREENED = 1) THEN 'Never Screened or Overdue'
        ELSE SCREENING_STATUS
    END AS SCREENING_STATUS_GROUP
    ,CASE
        WHEN (CAST(ld.is_on_register AS NUMBER) = 1 AND CAST(smi.is_on_register AS NUMBER) = 1) THEN 'LD & SMI'
        WHEN CAST(ld.is_on_register AS NUMBER) = 1 THEN 'LD'
        WHEN CAST(smi.is_on_register AS NUMBER) = 1 THEN 'SMI'
        ELSE 'Neither LD or SMI'
    END AS LD_SMI_REGISTERED_POPULATION
    ,COALESCE(dia.DIABETES_TYPE, 'No diabetes') AS DIABETES_TYPE
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
    END AS AGE_BAND_5Y
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
    END AS AGE_GROUP_MIXED
    ,dem.PRACTICE_CODE
    ,dem.PRACTICE_NAME
    ,post."Latitude" AS GP_PRACTICE_LATITUDE
    ,post."Longitude" AS GP_PRACTICE_LONGITUDE
    ,dem.PCN_NAME
    ,dem.NEIGHBOURHOOD_REGISTERED
    ,dem.BOROUGH_REGISTERED
    ,CASE
        WHEN dem.BOROUGH_RESIDENT = 'Islington' THEN dem.NEIGHBOURHOOD_RESIDENT -- borough_Resident only contains London boroughs but ward fields has all wards
        WHEN dem.BOROUGH_RESIDENT = 'Enfield' THEN dem.NEIGHBOURHOOD_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Haringey' THEN dem.NEIGHBOURHOOD_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Camden' THEN dem.NEIGHBOURHOOD_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Barnet' THEN dem.NEIGHBOURHOOD_RESIDENT
        WHEN dem.BOROUGH_RESIDENT IS NOT NULL THEN 'Non-NCL in London'
        WHEN dem.LSOA_CODE_21 IS NOT NULL AND dem.BOROUGH_RESIDENT IS NULL THEN 'Outside London'
        WHEN dem.LSOA_CODE_21 IS NULL THEN 'Unknown'
    END AS NEIGHBOURHOOD_RESIDENT_NCL_GROUP
    ,CASE
        WHEN dem.BOROUGH_RESIDENT = 'Islington' THEN dem.BOROUGH_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Enfield' THEN dem.BOROUGH_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Haringey' THEN dem.BOROUGH_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Camden' THEN dem.BOROUGH_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Barnet' THEN dem.BOROUGH_RESIDENT
        WHEN dem.BOROUGH_RESIDENT IS NOT NULL THEN 'Non-NCL in London'
        WHEN dem.LSOA_CODE_21 IS NOT NULL AND dem.BOROUGH_RESIDENT IS NULL THEN 'Outside London'
        WHEN dem.LSOA_CODE_21 IS NULL THEN 'Unknown'
    END AS BOROUGH_RESIDENT_NCL_GROUP
    ,dem.WARD_CODE
    ,CASE
        WHEN dem.BOROUGH_RESIDENT = 'Islington' THEN dem.WARD_NAME
        WHEN dem.BOROUGH_RESIDENT = 'Enfield' THEN dem.WARD_NAME
        WHEN dem.BOROUGH_RESIDENT = 'Haringey' THEN dem.WARD_NAME
        WHEN dem.BOROUGH_RESIDENT = 'Camden' THEN dem.WARD_NAME
        WHEN dem.BOROUGH_RESIDENT = 'Barnet' THEN dem.WARD_NAME
        WHEN dem.BOROUGH_RESIDENT IS NOT NULL THEN 'Non-NCL in London'
        WHEN dem.LSOA_CODE_21 IS NOT NULL AND dem.BOROUGH_RESIDENT IS NULL THEN 'Outside London'
        WHEN dem.LSOA_CODE_21 IS NULL THEN 'Unknown'
    END AS WARD_RESIDENT_NCL_GROUP
    ,COALESCE(dem.GENDER, 'Unknown') AS GENDER
    ,dem.ETHNICITY_CATEGORY
    ,CASE
        WHEN dem.ETHNICITY_SUBCATEGORY = 'Not stated' THEN 'Not Stated'
        WHEN dem.ETHNICITY_SUBCATEGORY = 'Recorded Not Known' THEN 'Not Known'
        WHEN dem.ETHNICITY_SUBCATEGORY = 'Refused' THEN 'Not Stated'
        WHEN dem.ETHNICITY_SUBCATEGORY = 'Unknown' THEN 'Not Known'
        WHEN dem.ETHNICITY_SUBCATEGORY = 'Not Recorded' THEN 'Not Known'
        ELSE dem.ETHNICITY_SUBCATEGORY
    END AS ETHNICITY_SUBCATEGORY
    ,dem.MAIN_LANGUAGE
    ,dem.LSOA_CODE_21 AS RESIDENT_LSOA_21
    ,COALESCE(dem.IMD_DECILE_25, 0) AS IMD_DECILE
    ,CASE 
        WHEN dem.IMD_DECILE_25 = 1 THEN '10 % Most Deprived'
        WHEN dem.IMD_DECILE_25 = 2 THEN '2'
        WHEN dem.IMD_DECILE_25 = 3 THEN '3'
        WHEN dem.IMD_DECILE_25 = 4 THEN '4'
        WHEN dem.IMD_DECILE_25 = 5 THEN '5'
        WHEN dem.IMD_DECILE_25 = 6 THEN '6'
        WHEN dem.IMD_DECILE_25 = 7 THEN '7'
        WHEN dem.IMD_DECILE_25 = 8 THEN '8'
        WHEN dem.IMD_DECILE_25 = 9 THEN '9'
        WHEN dem.IMD_DECILE_25 = 10 THEN '10% Least Deprived'
    ELSE 'Unknown'
    END AS IMD_DECILE_DESC
    ,COALESCE(dem.IMD_QUINTILE_NUMERIC_25, 0) AS IMD_QUINTILE
    ,dem.IMD_QUINTILE_25 AS IMD_QUINTILE_DESC
    ,COALESCE(ap.LATEST_GP_APPOINTMENT_ATTENDED, 'No history of attended appointment') AS LATEST_GP_APPOINTMENT_ATTENDED
    ,CAST(CASE
        WHEN (COALESCE(ltc.TOTAL_QOF_CONDITIONS, 0)) = 0 THEN '0'
        WHEN (COALESCE(ltc.TOTAL_QOF_CONDITIONS, 0)) = 1 THEN '1'
        WHEN (COALESCE(ltc.TOTAL_QOF_CONDITIONS, 0)) = 2 THEN '2'
        WHEN (COALESCE(ltc.TOTAL_QOF_CONDITIONS, 0)) = 3 THEN '3'
        WHEN (COALESCE(ltc.TOTAL_QOF_CONDITIONS, 0)) >= 4 THEN '4+'
    END AS VARCHAR(10)) AS LTC_COUNT_GROUP
    ,(COALESCE(ltc.BMI_CATEGORY, 'Unknown')) AS BMI_CATEGORY
    ,(COALESCE(ltc.SMOKING_STATUS, 'Unknown')) AS SMOKING_STATUS
    ,(COALESCE(ltc.ALCOHOL_STATUS, 'Unknown')) AS ALCOHOL_STATUS
    ,(COALESCE(ltc.ALCOHOL_RISK_SORT_KEY, 10)) AS ALCOHOL_STATUS_SORT
    ,CASE 
        WHEN hom.CODE_DESCRIPTION IS NOT NULL THEN 1 
        ELSE 0 
    END AS FLAG_HOMELESS
    ,CASE 
        WHEN car.IS_CARER = TRUE 
        THEN 1 
        ELSE 0 
    END AS FLAG_CARER
    ,elst.IS_UP_TO_DATE
    ,elst.IS_UNSUITABLE
    ,elst.IS_OVERDUE
    ,elst.IS_NEVER_SCREENED
    ,1 AS IS_ELIGIBLE
    ,elst.SECONDARY_USE_ALLOWED
    ,CASE
        WHEN dem.PRACTICE_CODE IN ('F83043', 'F85032') THEN 1 -- IN ORDER: 1) ridgmount, 2) southgate 
        ELSE 0
    END AS PRACTICE_DQ_ISSUE_FLAG

FROM cervical_eligibility_with_status AS elst

LEFT JOIN REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_LEARNING_DISABILITY_REGISTER AS ld
ON elst.PERSON_ID = ld.PERSON_ID

LEFT JOIN REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_SMI_REGISTER AS smi
ON elst.PERSON_ID = smi.PERSON_ID

LEFT JOIN REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_DIABETES_REGISTER AS dia 
ON elst.PERSON_ID = dia.PERSON_ID

LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS_HISTORICAL AS dem
ON elst.PERSON_ID = dem.PERSON_ID
-- join to demographic rows that were true at the time of each month_end date then use QUALIFY to take the most recent row. using effective start and end dates and NCL-registered population filters already applied to remove patient IDs that were not registered at each month end. 
AND elst.DATE_MONTH_END BETWEEN TO_DATE(dem.EFFECTIVE_START_DATE) AND COALESCE(TO_DATE(dem.EFFECTIVE_END_DATE), '9999-12-31')

LEFT JOIN MODELLING.OLIDS_UTILITIES.INT_GLOBAL_DATA_REFRESH_DATE AS ref -- latest refresh date

LEFT JOIN "Dictionary"."dbo"."Organisation" AS org
ON dem.PRACTICE_CODE = org."Organisation_Code"

LEFT JOIN "Dictionary"."dbo"."Postcode" AS post 
ON org."SK_Postcode_ID" = post."SK_PostcodeID"

LEFT JOIN attended_appointment AS ap
ON elst.PERSON_ID = ap.PERSON_ID

LEFT JOIN PUBLISHED_REPORTING__SECONDARY_USE.OLIDS_POP_HEALTH_NEEDS.POPULATION_HEALTH_NEEDS_BASE AS ltc 
on elst.PERSON_ID = ltc.PERSON_ID

LEFT JOIN DEV__REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_HOMELESS AS hom
ON elst.PERSON_ID = hom.PERSON_ID

LEFT JOIN DEV__REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_IS_CARER AS car
ON elst.PERSON_ID = car.PERSON_ID

WHERE dem.PRACTICE_CODE != 'Y03103' -- medicus select care is outside the OLIDs enterprise sharing agreement 

-- only take the most recent demographic info as one month_end can be joined to > 1 record in demographics_historical table
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY elst.PERSON_ID, elst.DATE_MONTH_END 
    ORDER BY TO_DATE(dem.EFFECTIVE_START_DATE) DESC
    ) = 1
)

--------------------------------------------
-- 2) build bowel latest month_end snapshot
--------------------------------------------

-------- build bowel eligible population denominator
,bowel_eligible_population AS (
SELECT mon.DATE_MONTH_END
    ,dem.PERSON_ID

-- calculate the age of each person at  month_end for filtering on eligible cohort
    ,TRUNC(DATEDIFF(MONTH, dem.BIRTH_DATE_APPROX, mon.DATE_MONTH_END) / 12) AS AGE_AT_MONTH_END

    ,CASE   
        WHEN opt.PERSON_ID IS NOT NULL THEN 1
        ELSE 0
    END AS SECONDARY_USE_ALLOWED
    
FROM month_end AS mon

-- join to historic demographics table to get birth dates of all historically registered patients
LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS_HISTORICAL AS dem
-- join to demographic rows which are relevant at the time of month_end date then use QUALIFY to take the most recent row. This join handles filtering on NCL-regietred patients only and patients that are are not dead
ON mon.DATE_MONTH_END BETWEEN TO_DATE(dem.REGISTRATION_START_DATE) AND LEAST(COALESCE(TO_DATE(dem.REGISTRATION_END_DATE), '9999-12-31'),COALESCE(TO_DATE(dem.DEATH_DATE_APPROX), '9999-12-31'))

LEFT JOIN REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED AS opt
ON dem.PERSON_ID = opt.PERSON_ID

WHERE AGE_AT_MONTH_END BETWEEN 50 AND 74 -- bowel screening eligibility
AND mon.DATE_MONTH_END = (SELECT MAX(DATE_MONTH_END) FROM DEV__REPORTING.CANCER__SCREENING.OLIDS_SCREENING_DASHBOARD_BOWEL_HISTORIC) -- dynamic to make sure only the most recent month end is used that matches the bowel historic script)

-- only take the most recent demographic info as one month_end can have > 1 record of changes in demographics_historical table
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dem.PERSON_ID, mon.DATE_MONTH_END 
    ORDER BY TO_DATE(dem.REGISTRATION_START_DATE) DESC
    ) = 1
)

------- get bowel historical screening data and add the screening interval and expiry date for each patient. this will then be joined to the eligible pop.
,bowel_historical_screening AS (

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


WHERE CLINICAL_EFFECTIVE_DATE >= '1960-01-01'  -- pick up screenings far back so you you can distinguish overdue from never screened in early months
AND CLINICAL_EFFECTIVE_DATE <= CURRENT_DATE
AND bs.IS_COMPLETED_SCREENING = 1
)

---------- join historic bowel screening data to eligible population (where month_end is >= screening months) and work out which patients are up-to-date
,bowel_eligibility_with_status AS (

SELECT elig.DATE_MONTH_END
    ,elig.PERSON_ID
    ,elig.SECONDARY_USE_ALLOWED
    ,elig.AGE_AT_MONTH_END

-- up-to-date when expiry_month is later than month_end and patient has not died
    ,CASE
        WHEN (MAX(hist.EXPIRY_MONTH) >= elig.DATE_MONTH_END)
         -- make sure patient is not dead
            AND (
                (MAX(age.DEATH_DATE_APPROX) > elig.DATE_MONTH_END) 
                OR MAX(age.DEATH_DATE_APPROX) IS NULL
                ) 
            THEN 1
        ELSE 0 
    END AS IS_UP_TO_DATE

-- overdue logic: expiry month is earlier than month_end date
    ,CASE
        WHEN (MAX(hist.EXPIRY_MONTH) < elig.DATE_MONTH_END) THEN 1
        ELSE 0
    END AS IS_OVERDUE

-- never screened logic: patient has never had a record of screening and therefore never received an expiry date
    ,CASE
        WHEN MAX(hist.EXPIRY_MONTH) IS NULL THEN 1
        ELSE 0
    END AS IS_NEVER_SCREENED

FROM bowel_eligible_population AS elig

LEFT JOIN bowel_historical_screening AS hist
ON elig.PERSON_ID = hist.PERSON_ID
-- only join screening records that happened before or on the month_end date
AND elig.DATE_MONTH_END >= hist.SCREENING_MONTH

-- to get death date
LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_AGE AS age
ON hist.PERSON_ID = age.PERSON_ID

GROUP BY ALL -- this collapses any duplicates from bowel_historical_screening
)


----------------------- join demographics to patient-level eligible population with screening status 
,bowel_final_historic_demographics AS (

SELECT 'Bowel' AS SCREENING_PROGRAMME
    ,ref.GLOBAL_DATA_REFRESH_DATE AS DATE_DATA_REFRESH
    ,elst.DATE_MONTH_END -- instead of DATE_DASHBOARD_REFRESH
    ,elst.PERSON_ID
    ,CASE
        WHEN IS_UP_TO_DATE = 1 THEN 'Up to Date'
        WHEN IS_OVERDUE = 1 THEN 'Overdue'
        WHEN IS_NEVER_SCREENED = 1 THEN 'Never Screened'
        ELSE 'Other'
    END AS SCREENING_STATUS
    ,CASE
        WHEN (IS_OVERDUE = 1 OR IS_NEVER_SCREENED = 1) THEN 'Never Screened or Overdue'
        ELSE SCREENING_STATUS
    END AS SCREENING_STATUS_GROUP
    ,CASE
        WHEN (CAST(ld.is_on_register AS NUMBER) = 1 AND CAST(smi.is_on_register AS NUMBER) = 1) THEN 'LD & SMI'
        WHEN CAST(ld.is_on_register AS NUMBER) = 1 THEN 'LD'
        WHEN CAST(smi.is_on_register AS NUMBER) = 1 THEN 'SMI'
        ELSE 'Neither LD or SMI'
    END AS LD_SMI_REGISTERED_POPULATION
    ,COALESCE(dia.DIABETES_TYPE, 'No diabetes') AS DIABETES_TYPE
    ,CASE
        WHEN elst.AGE_AT_MONTH_END BETWEEN 50 AND 54 THEN '50-54' 
        WHEN elst.AGE_AT_MONTH_END BETWEEN 55 AND 59 THEN '55-59'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 60 AND 64 THEN '60-64'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 65 AND 69 THEN '65-69'
        WHEN elst.AGE_AT_MONTH_END BETWEEN 70 AND 74 THEN '70-74'
        ELSE NULL
    END AS AGE_BAND_5Y
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
    END AS AGE_GROUP_MIXED
    ,dem.PRACTICE_CODE
    ,dem.PRACTICE_NAME
    ,post."Latitude" AS GP_PRACTICE_LATITUDE
    ,post."Longitude" AS GP_PRACTICE_LONGITUDE
    ,dem.PCN_NAME
    ,dem.NEIGHBOURHOOD_REGISTERED
    ,dem.BOROUGH_REGISTERED
    ,CASE
        WHEN dem.BOROUGH_RESIDENT = 'Islington' THEN dem.NEIGHBOURHOOD_RESIDENT -- borough_Resident only contains London boroughs but ward fields has all wards
        WHEN dem.BOROUGH_RESIDENT = 'Enfield' THEN dem.NEIGHBOURHOOD_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Haringey' THEN dem.NEIGHBOURHOOD_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Camden' THEN dem.NEIGHBOURHOOD_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Barnet' THEN dem.NEIGHBOURHOOD_RESIDENT
        WHEN dem.BOROUGH_RESIDENT IS NOT NULL THEN 'Non-NCL in London'
        WHEN dem.LSOA_CODE_21 IS NOT NULL AND dem.BOROUGH_RESIDENT IS NULL THEN 'Outside London'
        WHEN dem.LSOA_CODE_21 IS NULL THEN 'Unknown'
    END AS NEIGHBOURHOOD_RESIDENT_NCL_GROUP
    ,CASE
        WHEN dem.BOROUGH_RESIDENT = 'Islington' THEN dem.BOROUGH_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Enfield' THEN dem.BOROUGH_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Haringey' THEN dem.BOROUGH_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Camden' THEN dem.BOROUGH_RESIDENT
        WHEN dem.BOROUGH_RESIDENT = 'Barnet' THEN dem.BOROUGH_RESIDENT
        WHEN dem.BOROUGH_RESIDENT IS NOT NULL THEN 'Non-NCL in London'
        WHEN dem.LSOA_CODE_21 IS NOT NULL AND dem.BOROUGH_RESIDENT IS NULL THEN 'Outside London'
        WHEN dem.LSOA_CODE_21 IS NULL THEN 'Unknown'
    END AS BOROUGH_RESIDENT_NCL_GROUP
    ,dem.WARD_CODE
    ,CASE
        WHEN dem.BOROUGH_RESIDENT = 'Islington' THEN dem.WARD_NAME
        WHEN dem.BOROUGH_RESIDENT = 'Enfield' THEN dem.WARD_NAME
        WHEN dem.BOROUGH_RESIDENT = 'Haringey' THEN dem.WARD_NAME
        WHEN dem.BOROUGH_RESIDENT = 'Camden' THEN dem.WARD_NAME
        WHEN dem.BOROUGH_RESIDENT = 'Barnet' THEN dem.WARD_NAME
        WHEN dem.BOROUGH_RESIDENT IS NOT NULL THEN 'Non-NCL in London'
        WHEN dem.LSOA_CODE_21 IS NOT NULL AND dem.BOROUGH_RESIDENT IS NULL THEN 'Outside London'
        WHEN dem.LSOA_CODE_21 IS NULL THEN 'Unknown'
    END AS WARD_RESIDENT_NCL_GROUP
    ,COALESCE(dem.GENDER, 'Unknown') AS GENDER
    ,dem.ETHNICITY_CATEGORY
    ,CASE
        WHEN dem.ETHNICITY_SUBCATEGORY = 'Not stated' THEN 'Not Stated'
        WHEN dem.ETHNICITY_SUBCATEGORY = 'Recorded Not Known' THEN 'Not Known'
        WHEN dem.ETHNICITY_SUBCATEGORY = 'Refused' THEN 'Not Stated'
        WHEN dem.ETHNICITY_SUBCATEGORY = 'Unknown' THEN 'Not Known'
        WHEN dem.ETHNICITY_SUBCATEGORY = 'Not Recorded' THEN 'Not Known'
        ELSE dem.ETHNICITY_SUBCATEGORY
    END AS ETHNICITY_SUBCATEGORY
    ,dem.MAIN_LANGUAGE
    ,dem.LSOA_CODE_21 AS RESIDENT_LSOA_21
    ,COALESCE(dem.IMD_DECILE_25, 0) AS IMD_DECILE
    ,CASE 
        WHEN dem.IMD_DECILE_25 = 1 THEN '10 % Most Deprived'
        WHEN dem.IMD_DECILE_25 = 2 THEN '2'
        WHEN dem.IMD_DECILE_25 = 3 THEN '3'
        WHEN dem.IMD_DECILE_25 = 4 THEN '4'
        WHEN dem.IMD_DECILE_25 = 5 THEN '5'
        WHEN dem.IMD_DECILE_25 = 6 THEN '6'
        WHEN dem.IMD_DECILE_25 = 7 THEN '7'
        WHEN dem.IMD_DECILE_25 = 8 THEN '8'
        WHEN dem.IMD_DECILE_25 = 9 THEN '9'
        WHEN dem.IMD_DECILE_25 = 10 THEN '10% Least Deprived'
    ELSE 'Unknown'
    END AS IMD_DECILE_DESC
    ,COALESCE(dem.IMD_QUINTILE_NUMERIC_25, 0) AS IMD_QUINTILE
    ,dem.IMD_QUINTILE_25 AS IMD_QUINTILE_DESC
    ,COALESCE(ap.LATEST_GP_APPOINTMENT_ATTENDED, 'No history of attended appointment') AS LATEST_GP_APPOINTMENT_ATTENDED
    ,CAST(CASE
        WHEN (COALESCE(ltc.TOTAL_QOF_CONDITIONS, 0)) = 0 THEN '0'
        WHEN (COALESCE(ltc.TOTAL_QOF_CONDITIONS, 0)) = 1 THEN '1'
        WHEN (COALESCE(ltc.TOTAL_QOF_CONDITIONS, 0)) = 2 THEN '2'
        WHEN (COALESCE(ltc.TOTAL_QOF_CONDITIONS, 0)) = 3 THEN '3'
        WHEN (COALESCE(ltc.TOTAL_QOF_CONDITIONS, 0)) >= 4 THEN '4+'
    END AS VARCHAR(10)) AS LTC_COUNT_GROUP
    ,(COALESCE(ltc.BMI_CATEGORY, 'Unknown')) AS BMI_CATEGORY
    ,(COALESCE(ltc.SMOKING_STATUS, 'Unknown')) AS SMOKING_STATUS
    ,(COALESCE(ltc.ALCOHOL_STATUS, 'Unknown')) AS ALCOHOL_STATUS
    ,(COALESCE(ltc.ALCOHOL_RISK_SORT_KEY, 10)) AS ALCOHOL_STATUS_SORT
    ,CASE 
        WHEN hom.CODE_DESCRIPTION IS NOT NULL THEN 1 
        ELSE 0 
    END AS FLAG_HOMELESS
    ,CASE 
        WHEN car.IS_CARER = TRUE 
        THEN 1 
        ELSE 0 
    END AS FLAG_CARER
    ,elst.IS_UP_TO_DATE
    ,NULL AS IS_UNSUITABLE
    ,elst.IS_OVERDUE
    ,elst.IS_NEVER_SCREENED
    ,1 AS IS_ELIGIBLE
    ,elst.SECONDARY_USE_ALLOWED
    ,CASE
        WHEN dem.PRACTICE_CODE IN ('F83043', 'F85032') THEN 1 -- IN ORDER: 1) ridgmount, 2) southgate 
        ELSE 0
    END AS PRACTICE_DQ_ISSUE_FLAG

FROM bowel_eligibility_with_status AS elst

LEFT JOIN REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_LEARNING_DISABILITY_REGISTER AS ld
ON elst.PERSON_ID = ld.PERSON_ID

LEFT JOIN REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_SMI_REGISTER AS smi
ON elst.PERSON_ID = smi.PERSON_ID

LEFT JOIN REPORTING.OLIDS_DISEASE_REGISTERS.FCT_PERSON_DIABETES_REGISTER AS dia 
ON elst.PERSON_ID = dia.PERSON_ID

LEFT JOIN REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS_HISTORICAL AS dem
ON elst.PERSON_ID = dem.PERSON_ID
-- join to demographic rows that were true at the time of month_end date then use QUALIFY to take the most recent row. using effective start and end dates and NCL-registered population filters already applied to remove patient IDs that were not registered at each month end. 
AND elst.DATE_MONTH_END BETWEEN TO_DATE(dem.EFFECTIVE_START_DATE) AND COALESCE(TO_DATE(dem.EFFECTIVE_END_DATE), '9999-12-31')

LEFT JOIN MODELLING.OLIDS_UTILITIES.INT_GLOBAL_DATA_REFRESH_DATE AS ref -- latest refresh date

LEFT JOIN "Dictionary"."dbo"."Organisation" AS org
ON dem.PRACTICE_CODE = org."Organisation_Code"

LEFT JOIN "Dictionary"."dbo"."Postcode" AS post 
ON org."SK_Postcode_ID" = post."SK_PostcodeID"

LEFT JOIN attended_appointment AS ap
ON elst.PERSON_ID = ap.PERSON_ID

LEFT JOIN PUBLISHED_REPORTING__SECONDARY_USE.OLIDS_POP_HEALTH_NEEDS.POPULATION_HEALTH_NEEDS_BASE AS ltc 
on elst.PERSON_ID = ltc.PERSON_ID

LEFT JOIN DEV__REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_HOMELESS AS hom
ON elst.PERSON_ID = hom.PERSON_ID

LEFT JOIN DEV__REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_IS_CARER AS car
ON elst.PERSON_ID = car.PERSON_ID

WHERE dem.PRACTICE_CODE != 'Y03103' -- medicus select care is outside the OLIDs enterprise sharing agreement 

-- only take the most recent demographic info as month_end can have > 1 record of changes in demographics_historical table
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY elst.PERSON_ID, elst.DATE_MONTH_END 
    ORDER BY TO_DATE(dem.EFFECTIVE_START_DATE) DESC
    ) = 1
)


----------------------
-- 3) union scripts
----------------------

SELECT *
FROM cervical_final_historic_demographics

UNION ALL 

SELECT *
FROM bowel_final_historic_demographics
);