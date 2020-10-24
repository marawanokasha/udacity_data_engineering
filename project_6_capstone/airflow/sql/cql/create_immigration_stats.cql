CREATE TABLE IF NOT EXISTS immigration_stats (
    country_code_iso_2 text,
    year int, 
    month int,
    day int,
    count int,
    PRIMARY KEY ((country_code_iso_2), year, month, day)
    -- used country_code_iso_2 as partitioning key so that we can do group by on it
    -- we can only do group by on the whole partitioning key
);