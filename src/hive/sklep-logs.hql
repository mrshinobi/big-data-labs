CREATE EXTERNAL TABLE intermediate_access_logs (
    ip STRING,
    date_string STRING,
    method STRING,
    url STRING,
    http_version STRING,
    code1 STRING,
    code2 STRING,
    dash STRING,
    user_agent STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    'input.regex' = '([^ ]*) - - \\[([^\\]]*)\\] "([^\ ]*) ([^\ ]*) ([^\ ]*)" (\\d*) (\\d*) "([^"]*)" "([^"]*)"',
    'output.format.string' = "%1$$s %2$$s %3$$s %4$$s %5$$s %6$$s %7$$s %8$$s %9$$s")
LOCATION 's3://uwm-bigdata/data/sklep/logs/original_access_logs/';


ADD JAR /usr/lib/hive/lib/hive-contrib.jar;


INSERT OVERWRITE TABLE tokenized_access_logs
SELECT * FROM intermediate_access_logs;
