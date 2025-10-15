-- Dynamic table to prepare HPV data for use in the Primary Care Dashboard. London Indicator hardcoded until this data can be accessed in a refrence table.
-- Contact: eric.pinto@nhs.net

create or replace dynamic table DEV__REPORTING.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__VACCINATIONS__HPV(
	BOROUGH_NAME,
	YEAR_GROUP_NUMBER,
	GENDER_NAME,
	STUDENTS_TOTAL,
	STUDENTS_VACCINATED,
	ACADEMIC_YEAR_END_DATE,
	ACADEMIC_YEAR_TEXT,
	DATE_EXTRACT,
	IS_NCL,
	IS_LONDON,
	IS_ENGLAND
) target_lag = '2 hours' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to prepare HPV data for use in the Primary Care Dashboard. London Indicator hardcoded until this data can be accessed in a refrence table.'
 as

 SELECT
    h.*,  -- Select all columns from Hpv_Data
    CASE 
        WHEN h.BOROUGH_NAME IN ('Barnet', 'Enfield', 'Haringey', 'Camden', 'Islington') 
        THEN '1' ELSE '0' 
    END AS NCL_Ind,
    
	CASE 
		 WHEN BOROUGH_NAME IN (
			'Barking And Dagenham', 'Barnet', 'Bexley', 'Brent', 'Bromley',
			'Camden', 'City Of London', 'Croydon', 'Ealing', 'Enfield',
			'Greenwich', 'Hackney', 'Hammersmith And Fulham', 'Haringey',
			'Harrow', 'Havering', 'Hillingdon', 'Hounslow', 'Islington',
			'Kensington And Chelsea', 'Kingston Upon Thames', 'Lambeth',
			'Lewisham', 'Merton', 'Newham', 'Redbridge', 'Richmond Upon Thames',
			'Southwark', 'Sutton', 'Tower Hamlets', 'Waltham Forest',
			'Wandsworth', 'Westminster'
			) THEN '1'
		ELSE '0'
    END AS London_Ind, -- hard coded indicator to show Local Authorities in Greater London

    '1' AS England_Ind  -- Static indicator for England

    FROM DEV__MODELLING.CANCER__VACCINATION.HPV_UPTAKE h;