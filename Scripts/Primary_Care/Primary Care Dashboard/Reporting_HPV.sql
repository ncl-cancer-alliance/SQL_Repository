-- Dynamic table to prepare HPV data for use in the Primary Care Dashboard. London Indicator hardcoded until this data can be accessed in a refrence table.
-- Contact: eric.pinto@nhs.net

create or replace dynamic table DEV__REPORTING.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__VACCINATIONS__HPV(

    LOCAL_AUTHORITY,
	YEAR_GROUP,
	GENDER,
	"NUMBER",
	NUMBER_VACCINATED,
	ACADEMIC_YEAR_END_DATE,
	ACADEMIC_YEAR_TEXT,
    IS_NCL,
    IS_LONDON,
    IS_ENGLAND

) target_lag = '2 hours' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to prepare HPV data for use in the Primary Care Dashboard. London Indicator hardcoded until this data can be accessed in a refrence table.'
 as

 SELECT
    h.*,  -- Select all columns from Hpv_Data
    CASE 
        WHEN h.Local_Authority IN ('Barnet', 'Enfield', 'Haringey', 'Camden', 'Islington') 
        THEN '1' ELSE '0' 
    END AS NCL_Ind,
    
	CASE 
		 WHEN Local_Authority IN (
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

    FROM MODELLING.CANCER__PRIMARY_CARE.HPV_UPTAKE h