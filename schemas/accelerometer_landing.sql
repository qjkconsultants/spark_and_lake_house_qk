CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
  user string,
  timestamp bigint,
  x double,
  y double,
  z double
)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-lake-house-qk/accelerometer/landing/'
TBLPROPERTIES (
  'classification'='json'
);