/********************************************************************************************
-- Script Summary:
-- This Snowflake script automates the process of extracting metadata from external volumes
-- (S3) and storing it in a structured table for further use.
--
-- Key Steps:
-- 1. Create database and schema if they do not exist.
-- 2. Create a table to store external volume information.
-- 3. List all external volumes and extract their S3 paths and storage regions.
-- 4. Construct a nested JSON object representing volumes -> regions -> S3 paths.
-- 5. Flatten the JSON and insert it into the EXTERNAL_VOLUME_PATHS table.
-- 6. Grant necessary permissions for SYSADMIN role.
-- 7. Verify the data.
--
-- Placeholders / Notes:
--   - Replace database/schema/table names if needed.
--   - Script assumes S3 external volumes are already created and accessible.
--   - Optional: truncate table before full refresh if required.
********************************************************************************************/

---------------------------------------------------------------------------------------------
-- Step 1: Switch to SYSADMIN role to perform database/schema/table operations
USE ROLE SYSADMIN;

-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS METADATA_VIEWER_DB;

-- Create a schema within the database if it doesn't exist
CREATE SCHEMA IF NOT EXISTS METADATA_VIEWER_DB.APP_SETUP;

-- Set the context to the newly created schema
USE SCHEMA METADATA_VIEWER_DB.APP_SETUP;

-- Create a table to store external volume information (volume name, storage region, S3 path)
CREATE OR REPLACE TABLE EXTERNAL_VOLUME_PATHS (
  VOLUME_NAME    STRING,
  STORAGE_REGION STRING,
  S3_PATH        STRING
);

-- Switch to ACCOUNTADMIN role, needed to describe external volumes and access metadata
USE ROLE ACCOUNTADMIN;

-- Declare variables to store intermediate query results
DECLARE
  res1 RESULTSET;     -- stores result of DESCRIBE EXTERNAL VOLUME
  res2 RESULTSET;     -- stores parsed JSON of S3 paths and regions
  sql_vol VARCHAR;    -- string variable for the dynamic SQL query
  rpt VARIANT;        -- stores the final nested JSON object of volumes
  rpt_int VARIANT;    -- stores the intermediate JSON for each volume

BEGIN
  -- Initialize the main JSON object
  rpt := object_construct();

  -- SQL query to extract S3 paths and storage regions from external volume metadata
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

  -- Store the results of the last query in a cursor
  LET c1 CURSOR FOR SELECT * FROM TABLE(RESULT_SCAN(last_query_id()));
  OPEN c1;

  -- Loop through each external volume
  FOR record IN c1 DO
    -- Get detailed description of the external volume
    res1 := (EXECUTE IMMEDIATE 'DESCRIBE EXTERNAL VOLUME ' || record."name");

    -- Execute the S3 extraction query
    res2 := (EXECUTE IMMEDIATE :sql_vol);

    -- Initialize intermediate JSON object for this volume
    rpt_int := object_construct();

    -- Cursor for inner loop through S3 paths and regions
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

    -- Insert the intermediate object into the main object keyed by volume name
    rpt := object_insert(rpt, record."name", rpt_int);
  END FOR;

  -- Return the final nested JSON object
  RETURN rpt;
END;

-- Display the returned JSON object from the previous block
SELECT $1 as VARIANT_OUTPUT FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


-- Optionally truncate table for full refresh
-- TRUNCATE TABLE EXTERNAL_VOLUME_PATHS;

-- Insert the flattened JSON data into the table
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

-- Grant necessary permissions to SYSADMIN for schema usage and table select
GRANT USAGE ON SCHEMA METADATA_VIEWER_DB.APP_SETUP TO ROLE SYSADMIN;
GRANT SELECT ON TABLE METADATA_VIEWER_DB.APP_SETUP.EXTERNAL_VOLUME_PATHS TO ROLE SYSADMIN;

-- Switch back to SYSADMIN role
USE ROLE SYSADMIN;

-- Verify data
SELECT * FROM EXTERNAL_VOLUME_PATHS;
