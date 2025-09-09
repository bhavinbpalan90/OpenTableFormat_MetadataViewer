/********************************************************************************************
-- Script Summary:
-- This script sets up an S3 external stage in Snowflake, including:
-- 1. Creating a storage integration for AWS S3.
-- 2. Granting usage on the integration to a role.
-- 3. Creating a database and schema if they do not exist.
-- 4. Creating a stage pointing to the S3 bucket.
-- 5. Listing the files in the stage to verify setup.
--
-- Placeholders to update before running:
--    <STORAGE_INTEGRATION_NAME> -> Name of the storage integration
--    <AWS_ROLE_ARN> -> AWS IAM Role ARN with S3 access
--    <S3_BUCKET_URL> -> Your S3 bucket URL (Bucket level, not prefix)
--    <ROLE_TO_GRANT> -> Role to grant usage on integration
--    <STAGE_NAME> -> Name of the stage to create
********************************************************************************************/

-- Step 1: Switch to ACCOUNTADMIN to create storage integration
USE ROLE ACCOUNTADMIN;

-- Step 2: Create a storage integration for S3
CREATE OR REPLACE STORAGE INTEGRATION <STORAGE_INTEGRATION_NAME>
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = '<AWS_ROLE_ARN>'  -- Replace with your AWS IAM Role ARN
  STORAGE_ALLOWED_LOCATIONS = ('<S3_BUCKET_URL>');  -- Replace with your S3 bucket URL

-- Step 3: Grant usage on the storage integration to a role
GRANT USAGE ON INTEGRATION <STORAGE_INTEGRATION_NAME> TO ROLE <ROLE_TO_GRANT>;  -- Replace role name

-- Step 4: Verify storage integration details
DESC STORAGE INTEGRATION <STORAGE_INTEGRATION_NAME>;

---------------------------------------------------------------------------------------------
-- Step 5: Switch to SYSADMIN (or appropriate role) to create database/schema and stage
USE ROLE SYSADMIN;

-- Step 6: Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS METADATA_VIEWER_DB;  -- Replace with your database name

-- Step 7: Create schema within the database if it doesn't exist
CREATE SCHEMA IF NOT EXISTS METADATA_VIEWER_DB.APP_SETUP;  -- Replace schema name

-- Step 8: Set the context to the newly created schema
USE SCHEMA METADATA_VIEWER_DB.APP_SETUP;

-- Step 9: Create a stage pointing to the S3 bucket
CREATE OR REPLACE STAGE <STAGE_NAME>
  URL = '<S3_BUCKET_URL>'  -- Replace with your S3 bucket URL
  STORAGE_INTEGRATION = <STORAGE_INTEGRATION_NAME>;  -- Storage integration name

-- Step 10: List files in the stage to verify
LS @<STAGE_NAME>;
