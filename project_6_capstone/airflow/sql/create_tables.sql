
CREATE TABLE IF NOT EXISTS public.country (
	country_code VARCHAR(3) NOT NULL,
	country_name VARCHAR(100),
	country_code_iso_2 VARCHAR(2),
    continent VARCHAR(50),

	CONSTRAINT country_code_pkey PRIMARY KEY (country_code)
) DISTSTYLE ALL;


CREATE TABLE IF NOT EXISTS public.state (
	state_code VARCHAR(2) NOT NULL,
	state_name VARCHAR(100) NOT NULL,

	CONSTRAINT state_code_pkey PRIMARY KEY (state_code)
) DISTSTYLE ALL;


CREATE TABLE IF NOT EXISTS public.visa_type (
	visa_type VARCHAR,
	visa_category VARCHAR

) DISTSTYLE ALL;


CREATE TABLE IF NOT EXISTS public.gdp (
	country_code VARCHAR(3) NOT NULL,
	year INT,
    gdp_value FLOAT,

	CONSTRAINT gdp_country_code_pkey PRIMARY KEY (country_code)
	
) DISTSTYLE ALL;


CREATE TABLE IF NOT EXISTS public.immigration (
	admnum BIGINT,
	arrival_date DATE,
	year INT,
	month INT,
	day INT,
	country_citizenship VARCHAR(3),
	country_residence VARCHAR(3),
 	destination_state VARCHAR(2),
	age INT,
	gender VARCHAR,
	visa_type VARCHAR,
	visa_category VARCHAR,
	num_previous_stays INT,
	unrestricted_stay BOOL,
	is_overstay BOOL

	-- CONSTRAINT immigration_pkey PRIMARY KEY (
    --     admnum, 
    --     arrival_date,
	-- 	   country_citizenship
    -- )
) 
DISTKEY(country_citizenship) 
SORTKEY(arrival_date)
;

