-- ================================================
-- METADATA VIEWER FULL SETUP SCRIPT
-- Combines external volume paths and external stage paths
-- Author: Bhavin Palan
-- ================================================

-- ------------------------------------------------
-- STEP 1: Switch to SYSADMIN role
-- Needed for database, schema, and table creation
-- ------------------------------------------------
USE ROLE SYSADMIN;

-- ------------------------------------------------
-- STEP 2: Create Database and Schema if they do not exist
-- ------------------------------------------------
CREATE DATABASE IF NOT EXISTS METADATA_VIEWER_DB;
CREATE SCHEMA IF NOT EXISTS METADATA_VIEWER_DB.APP_SETUP;

-- Set the context to the newly created schema
USE SCHEMA METADATA_VIEWER_DB.APP_SETUP;

-- ------------------------------------------------
-- STEP 3: Create tables
-- 3a: Table to store external volume information
-- ------------------------------------------------
CREATE OR REPLACE TABLE EXTERNAL_VOLUME_PATHS (
  VOLUME_NAME    STRING,
  STORAGE_REGION STRING,
  S3_PATH        STRING
);

-- 3b: Table to store external stage information
CREATE OR REPLACE TABLE STAGE_PATHS (
  STAGE_NAME        STRING,
  DATABASE_NAME     STRING,
  SCHEMA_NAME       STRING,
  STORAGE_REGION    STRING,
  STORAGE_PROVIDER  STRING,
  STAGE_URL         STRING
);

-- ------------------------------------------------
-- STEP 4: External Volume Metadata Extraction
-- Switch to ACCOUNTADMIN role to access volume metadata
-- ------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Declare variables for volume processing
DECLARE
  res1 RESULTSET;     -- stores result of DESCRIBE EXTERNAL VOLUME
  res2 RESULTSET;     -- stores parsed JSON of S3 paths and regions
  sql_vol VARCHAR;    -- string variable for dynamic SQL
  rpt VARIANT;        -- final nested JSON object
  rpt_int VARIANT;    -- intermediate JSON object for each volume

BEGIN
  -- Initialize main JSON object
  rpt := object_construct();

  -- SQL to parse S3 paths from external volume metadata
  sql_vol := '
    SELECT 
      PROPERTY, 
      VALUE:"NAME"::VARCHAR AS NAME, 
      PARSE_JSON(VALUE:"STORAGE_ALLOWED_LOCATIONS") AS S3_PATHS,
      VALUE:"STORAGE_REGION"::VARCHAR AS STORAGE_REGION
    FROM (
      SELECT PARSE_JSON(T."property_value") AS VALUE, T."property" AS PROPERTY
      FROM TABLE(RESULT_SCAN(last_query_id())) T
      WHERE T."property_type" = ''String''
        AND T."property" != ''ACTIVE''
        AND VALUE:"STORAGE_PROVIDER" = ''S3''
    )
  ';

  -- List all external volumes
  SHOW EXTERNAL VOLUMES;

  -- Cursor for looping through volumes
  LET c1 CURSOR FOR SELECT * FROM TABLE(RESULT_SCAN(last_query_id()));
  OPEN c1;

  FOR record IN c1 DO
    -- Describe each external volume
    res1 := (EXECUTE IMMEDIATE 'DESCRIBE EXTERNAL VOLUME ' || record."name");

    -- Extract S3 paths
    res2 := (EXECUTE IMMEDIATE :sql_vol);

    -- Initialize intermediate JSON object for this volume
    rpt_int := object_construct();

    -- Cursor for looping through S3 paths and regions
    LET c2 CURSOR FOR res2;
    OPEN c2;

    FOR inner_record IN c2 DO
      -- Organize by region -> array of paths
      rpt_int := object_insert(
                    rpt_int, 
                    inner_record.STORAGE_REGION, 
                    inner_record.S3_PATHS
                 );
    END FOR;

    -- Insert intermediate object into main object keyed by volume name
    rpt := object_insert(rpt, record."name", rpt_int);
  END FOR;

  -- Return final nested JSON object
  RETURN rpt;
END;

-- Display JSON object generated for external volumes
SELECT $1 AS VARIANT_OUTPUT FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ------------------------------------------------
-- STEP 5: Insert external volume paths into table
-- Optional: truncate table first for full refresh
-- ------------------------------------------------
-- TRUNCATE TABLE EXTERNAL_VOLUME_PATHS;

INSERT INTO EXTERNAL_VOLUME_PATHS (VOLUME_NAME, STORAGE_REGION, S3_PATH)
SELECT
  root.key::STRING        AS VOLUME_NAME,
  region.key::STRING      AS STORAGE_REGION,
  path.value::STRING      AS S3_PATH
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) t,
LATERAL FLATTEN(INPUT => t.VARIANT_OUTPUT) root,   -- flatten by volume
LATERAL FLATTEN(INPUT => root.value) region,       -- flatten by region
LATERAL FLATTEN(INPUT => region.value) path;       -- flatten by S3 path

-- Verify inserted data
SELECT * FROM EXTERNAL_VOLUME_PATHS;

-- ------------------------------------------------
-- STEP 6: External Stage Metadata Extraction
-- Switch to ACCOUNTADMIN role for stages
-- ------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Create stored procedure to refresh stage paths
CREATE OR REPLACE PROCEDURE METADATA_VIEWER_DB.APP_SETUP.LOAD_STAGE_PATHS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Start fresh
  TRUNCATE TABLE METADATA_VIEWER_DB.APP_SETUP.STAGE_PATHS;

  -- Capture external stages
  SHOW STAGES IN ACCOUNT;

  INSERT INTO METADATA_VIEWER_DB.APP_SETUP.STAGE_PATHS
    (STAGE_NAME, DATABASE_NAME, SCHEMA_NAME, STORAGE_REGION, STORAGE_PROVIDER, STAGE_URL)
  SELECT 
    "name"::STRING        AS STAGE_NAME,
    "database_name"::STRING,
    "schema_name"::STRING,
    "region"::STRING      AS STORAGE_REGION,
    "cloud"::STRING       AS STORAGE_PROVIDER,
    "url"::STRING         AS STAGE_URL
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE "type" = 'EXTERNAL';

  RETURN 'Stage paths refreshed successfully';
END;
$$;

-- Call the procedure to populate STAGE_PATHS table
CALL METADATA_VIEWER_DB.APP_SETUP.LOAD_STAGE_PATHS();

-- Verify inserted data
SELECT * FROM METADATA_VIEWER_DB.APP_SETUP.STAGE_PATHS;

-- ------------------------------------------------
-- STEP 7: Grant necessary permissions
-- ------------------------------------------------
GRANT USAGE ON SCHEMA METADATA_VIEWER_DB.APP_SETUP TO ROLE SYSADMIN;
GRANT SELECT ON TABLE METADATA_VIEWER_DB.APP_SETUP.EXTERNAL_VOLUME_PATHS TO ROLE SYSADMIN;
GRANT SELECT ON TABLE METADATA_VIEWER_DB.APP_SETUP.STAGE_PATHS TO ROLE SYSADMIN;

-- Switch back to SYSADMIN role for regular operations
USE ROLE SYSADMIN;

-- Verify data
SELECT * FROM EXTERNAL_VOLUME_PATHS;
SELECT * FROM STAGE_PATHS;
