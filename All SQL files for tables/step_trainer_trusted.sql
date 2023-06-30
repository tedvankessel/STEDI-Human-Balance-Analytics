CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_trusted` (
  `sensorreadingtime` bigint,
  `serialnumber` string,
  `distancefromobject` int
) COMMENT "trusted step trainer data table"
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://tvkbucket/step_trainer/trusted/'
TBLPROPERTIES ('classification' = 'json');