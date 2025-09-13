# 🔍 Metadata Explorer (Iceberg / Delta)

**Metadata Explorer** is a Streamlit application to safely explore Iceberg metadata and preview JSON, AVRO, and Parquet files from your Snowflake external stages. The project includes **Snowflake setup scripts** and an interactive **Streamlit app**.

---

## 📌 Table of Contents

- [Overview](#overview)  
- [Prerequisites](#prerequisites)  
- [Setup Sequence](#setup-sequence)  
  - [1️⃣ Setup Snowflake Storage Integration & Stage](#1-setup-snowflake-storage-integration--stage)  
  - [2️⃣ Extract External Volume Information](#2-extract-external-volume-information)  
  - [3️⃣ Run the Streamlit App](#3-run-the-streamlit-app)  
- [Usage](#usage)  
- [Notes](#notes)  
- [Media / Screenshots](#media--screenshots)  

---

## 📝 Overview

This project allows you to:

- Interactively explore Iceberg tables and preview JSON, AVRO, and Parquet files.  
- Generate AI-based summaries of file contents for quick insights.

![Demo](app_demo.gif)

---

## ⚙️ Prerequisites

- Snowflake account with roles: `ACCOUNTADMIN`, `SYSADMIN` if External Stages is not already created. Else Usage on Stages would be enough.
- AWS S3 bucket(s) and an IAM Role with access for External Stage Setup 
- Streamlit feature enabled in Snowflake (required packages are to be selected in SiS).  

---

## 🛠 Setup Sequence

The setup must be executed in the following order.

---

### 1️⃣ Setup Snowflake Storage Integration & Stage 

(Required only if External Stages Pointing to existing External Volume doesn't exist)

The setup script for creating the storage integration and stage is provided in the `setup` directory:  

[📄 Storage Integration & Stage Setup Script](./setup/1_StorageIntegration_StageSetup.sql)

**Instructions:**

1. Open the SQL script in Snowflake.  
2. Update the placeholders with your S3 bucket, IAM role ARN, stage name, and storage integration name.  
3. Run the script to create the storage integration and stage.  
4. Verify the stage by listing files in Snowflake.

> **Note:** One stage is required per S3 bucket. Selecting the correct stage is the user's responsibility.

---

### 2️⃣ Extract External Volume & Stage Path Information

The setup script to extract external volume info and populate the metadata table is in the `setup` directory:

[📄 External Volume & Stage URL Details Extraction Script](./setup/2_External_Volume_and_Stage_SetupScript.sql)

**Instructions:**

1. Open the script in Snowflake.  
2. Execute it to populate the `EXTERNAL_VOLUME_PATHS` & `STAGE_PATHS` table in your database.  
3. Ensure the SYSADMIN or Streamlit role has the necessary usage privileges.

---

### 3️⃣ Run the Streamlit App

The Streamlit app is provided in the `app` directory:

[📄 Open Table Format Metadata Viewer Streamlit App Script](./app/OpenTableFormat_MetadataViewet.py)


**Instructions:**

1. Open the Streamlit app in Snowflake’s environment.  
2. Launch the app using Streamlit.  
3. Steps inside the app:

- Choose the database and Iceberg table you want to explore.  
- The app automatically resolves `BASE_LOCATION` and the external volume path.  
- List files and preview JSON, AVRO, or Parquet files.  
- Optionally generate AI-based summaries of the file contents.

---

## 🖥 Usage

- **Database/Table Selection:** Select the Iceberg table.  
- **File Viewing:** Filter files by dropdown or search. Preview metadata or sample data depending on the file type.  
- **Parquet Files:** Display metadata, sample data, or both.  
- **AI Summary:** Expand the AI summary section for a concise summary.

---

## ⚠️ Notes

- `.crc` and `.bin` files from Delta logs are ignored in previews.  
- Ensure the stage exist and authorized that matches the S3 path of the external volume.  
- Supported file types: JSON, AVRO, Parquet.  

