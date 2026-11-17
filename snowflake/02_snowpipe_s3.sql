-- Optional: replace the Airflow PUT + COPY step with Snowpipe auto-ingest from S3.
-- Airflow (or any producer) writes the NDJSON files to S3; Snowpipe loads each file
-- within about a minute of it landing, on serverless compute.

USE ROLE ACCOUNTADMIN;

-- 1) Let Snowflake read the bucket through an IAM role (no keys in SQL)
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_LANDING_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account_id>:role/<snowflake_read_role>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<landing-bucket>/customers/');
-- DESC INTEGRATION S3_LANDING_INT;  -> put STORAGE_AWS_IAM_USER_ARN / EXTERNAL_ID into the IAM role trust policy

GRANT USAGE ON INTEGRATION S3_LANDING_INT TO ROLE SYSADMIN;
USE ROLE SYSADMIN;

-- 2) External stage pointing at the landing folder
CREATE STAGE IF NOT EXISTS LENDING.RAW.CUSTOMERS_S3_STAGE
  URL = 's3://<landing-bucket>/customers/'
  STORAGE_INTEGRATION = S3_LANDING_INT
  FILE_FORMAT = LENDING.RAW.NDJSON_FORMAT;

-- 3) The pipe: the same COPY as the Airflow task, triggered by S3 events
CREATE PIPE IF NOT EXISTS LENDING.RAW.CUSTOMERS_PIPE AUTO_INGEST = TRUE AS
  COPY INTO LENDING.RAW.CUSTOMERS_RAW (change_type, source_database, customer_id, doc, extracted_epoch,
                                       _source_file, _source_row, _loaded_at)
  FROM (
      SELECT $1:op::string, $1:source_database::string, $1:customer_id::string, $1:doc, $1:extracted_epoch::number,
             METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
      FROM @LENDING.RAW.CUSTOMERS_S3_STAGE
  );

-- 4) SHOW PIPES -> copy notification_channel (an SQS ARN) into the bucket's
--    "object created" event notification for the customers/ prefix.
SHOW PIPES LIKE 'CUSTOMERS_PIPE' IN SCHEMA LENDING.RAW;

-- 5) Monitoring
-- SELECT SYSTEM$PIPE_STATUS('LENDING.RAW.CUSTOMERS_PIPE');
-- SELECT * FROM TABLE(LENDING.INFORMATION_SCHEMA.COPY_HISTORY(
--     TABLE_NAME => 'RAW.CUSTOMERS_RAW', START_TIME => DATEADD(hour, -24, CURRENT_TIMESTAMP())));
