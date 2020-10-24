CREATE KEYSPACE IF NOT EXISTS {{params.project_keyspace}} WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS immigration (
    country_citizenship text,
    year int, 
    month int,
    day int,
    count int,
    PRIMARY KEY ((country_citizenship), year, month, day)
    -- used country_citizenship as partitioning key so that we can do group by on it
    -- we can only do group by on the whole partitioning key
);


CREATE TABLE state (
    state_code text, 
    state_name text,

    PRIMARY KEY (state_code)
);


CREATE TABLE country (
	country_code text,
	country_name text,
	country_code_iso_2 text,
    continent text,

    PRIMARY KEY (country_code)
);


CREATE TABLE visa_type (
	visa_type text,
	visa_category text,

    PRIMARY KEY (visa_type)
);