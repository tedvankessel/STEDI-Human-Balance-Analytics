CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_trusted` (
  `user` string,
  `timestamp` bigint,
  `x` float,
  `y` float,
  `z` float
) COMMENT "table of trusted accelerometer data with consent"
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://tvkbucket/accelerometer/trusted/'
TBLPROPERTIES ('classification' = 'json');