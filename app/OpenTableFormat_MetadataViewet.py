import streamlit as st
st.set_page_config(layout="wide")

from snowflake.snowpark.context import get_active_session
import pandas as pd
import re
import os
import json
import tempfile
import shutil
import pyarrow.parquet as pq
import pyarrow
import fastavro

# ---------- Session & UI header ----------
session = get_active_session()
st.title("🔍 Metadata Explorer (Iceberg / Delta)")
st.write("Safely explore Iceberg metadata and preview JSON, AVRO, and Parquet files from your Snowflake external stages.")

# ---------- Helpers ----------
def format_bytes(size):
    for unit in ['B','KB','MB','GB','TB']:
        if size is None:
            return "N/A"
        try:
            size=float(size)
        except Exception:
            return str(size)
        if size<1024:
            return f"{size:.2f} {unit}"
        size/=1024
    return f"{size:.2f} PB"

def format_dates(dt_str):
    try:
        return pd.to_datetime(dt_str).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return str(dt_str)

def cleanse_for_cortex(record,max_len=2000):
    try:
        s=json.dumps(record,default=str,ensure_ascii=False)
    except Exception:
        s=str(record)
    s=s.replace("'", "''")
    s=re.sub(r'[\n\r\t]+',' ',s)
    s=re.sub(r'[\x00-\x1f\x7f-\x9f]+',' ',s)
    s=re.sub(r'\s{2,}',' ',s)
    return s[:max_len]

def safe_cortex_call(record):
    try:
        safe_text=cleanse_for_cortex(record,max_len=3000)
        sql_ai=f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large',
                'Can you summarize the output here in bullets?: {safe_text}'
            ) AS MODEL_OUTPUT;
        """
        df_ai=session.sql(sql_ai).to_pandas()
        if 'MODEL_OUTPUT' in df_ai.columns:
            return df_ai.iloc[0]['MODEL_OUTPUT']
        return df_ai.iloc[0,0]
    except Exception as e:
        return f"AI summary skipped: {e}"



# ---------- Utility: Parquet metadata ----------
def show_parquet_metadata(local_file_path: str):
    parquet_file = pq.ParquetFile(local_file_path)
    meta = parquet_file.metadata

    overview = {
        "file_path": local_file_path,
        "created_by": getattr(meta, "created_by", None),
        "num_rows": getattr(meta, "num_rows", None),
        "num_columns": getattr(meta, "num_columns", None),
        "num_row_groups": getattr(meta, "num_row_groups", None)
    }

    schema_fields = []
    arrow_schema = parquet_file.schema_arrow
    for f in arrow_schema:
        schema_fields.append({
            "column": f.name,
            "type": str(f.type),
            "nullable": getattr(f, "nullable", None)
        })
    schema_df = pd.DataFrame(schema_fields)

    kv = {}
    try:
        if meta.metadata:
            for k, v in meta.metadata.items():
                k_str = k.decode("utf-8", "ignore") if isinstance(k, (bytes, bytearray)) else str(k)
                v_str = v.decode("utf-8", "ignore") if isinstance(v, (bytes, bytearray)) else str(v)
                kv[k_str] = v_str
    except Exception:
        kv = {}

    row_groups = []
    for i in range(meta.num_row_groups):
        rg = meta.row_group(i)
        rg_info = {
            "num_rows": rg.num_rows,
            "total_byte_size": rg.total_byte_size,
            "columns": []
        }
        for j in range(rg.num_columns):
            col = rg.column(j)
            col_info = {
                "path_in_schema": col.path_in_schema,
                "physical_type": col.physical_type,
                "compression": col.compression,
                "encodings": col.encodings,
                "statistics": None
            }
            try:
                if col.statistics is not None:
                    col_info["statistics"] = {
                        "null_count": getattr(col.statistics, "null_count", None),
                        "distinct_count": getattr(col.statistics, "distinct_count", None),
                        "min": getattr(col.statistics, "min", None),
                        "max": getattr(col.statistics, "max", None),
                        "num_values": getattr(col.statistics, "num_values", None)
                    }
            except (NotImplementedError, pyarrow.lib.ArrowNotImplementedError):
                col_info["statistics"] = None
            rg_info["columns"].append(col_info)
        row_groups.append(rg_info)

    return {
        "overview": overview,
        "schema_df": schema_df,
        "kv": kv,
        "row_groups": row_groups
    }

def render_parquet_view(metadata_dict):
    st.subheader("📊 Parquet Metadata")
    with st.expander("Overview", expanded=False):
        st.json(metadata_dict["overview"])
    with st.expander("Schema (columns)", expanded=False):
        if not metadata_dict["schema_df"].empty:
            st.dataframe(metadata_dict["schema_df"])
        else:
            st.write("No schema available.")
    with st.expander("Key-Value Metadata", expanded=False):
        if metadata_dict["kv"]:
            st.json(metadata_dict["kv"])
        else:
            st.write("No key-value metadata present.")
    for i, rg in enumerate(metadata_dict["row_groups"], start=1):
        with st.expander(f"Row Group {i}", expanded=False):
            rg_header = {
                "num_rows": rg.get("num_rows"),
                "total_byte_size": rg.get("total_byte_size")
            }
            st.write(rg_header)
            cols = rg.get("columns", [])
            rows = []
            for c in cols:
                stats = c.get("statistics") or {}
                rows.append({
                    "path_in_schema": c.get("path_in_schema"),
                    "physical_type": c.get("physical_type"),
                    "compression": c.get("compression"),
                    "encodings": ", ".join(c.get("encodings", []) or []),
                    "null_count": stats.get("null_count"),
                    "distinct_count": stats.get("distinct_count"),
                    "min": stats.get("min"),
                    "max": stats.get("max"),
                    "num_values": stats.get("num_values")
                })
            if rows:
                st.dataframe(pd.DataFrame(rows))
            else:
                st.write("No column-level details available for this row group.")
    with st.expander("Raw Metadata JSON", expanded=False):
        st.json({
            "schema": str(metadata_dict["schema_df"]),
            "row_groups": metadata_dict["row_groups"]
        })

# ---------- Utility: JSON / AVRO view ----------
def render_json_avro_view(records:list, file_path:str):
    st.subheader("📄 File Information")
    with st.expander("Raw File View", expanded=False):
        try:
            with open(file_path,"r",encoding="utf-8") as f:
                st.json(json.load(f) if file_path.lower().endswith(".json") else records)
        except Exception:
            st.write(records)
    with st.expander("Tabular Format", expanded=False):
        if records:
            st.dataframe(pd.DataFrame(records))
        else:
            st.write("No records available.")
    

# ---------- Sidebar: Stage & Submit ----------
with st.sidebar:
    st.subheader("🗂 Stage & File Access")
    with st.spinner("Fetching stages..."):
        try:
            stages_df=session.sql("SHOW STAGES IN ACCOUNT").to_pandas()
            stages_df.columns=[c.strip('"').upper() for c in stages_df.columns]
            stages_df=stages_df[stages_df['TYPE']=='EXTERNAL']
            stages_df['FULL_NAME']=stages_df['DATABASE_NAME']+'.'+stages_df['SCHEMA_NAME']+'.'+stages_df['NAME']
            stage_names=['Select One']+sorted(stages_df['FULL_NAME'].tolist())
        except Exception as e:
            st.error(f"Error retrieving stages: {e}")
            st.stop()

    if "selected_stage" not in st.session_state:
        st.session_state.selected_stage=None

    stage_choice=st.selectbox("Select Stage", stage_names,key="stage_choice")
    if st.button("Submit Stage"):
        if stage_choice!="Select One":
            st.session_state.selected_stage=stage_choice
        else:
            st.warning("Please select a valid stage before submitting.")

    if st.session_state.get("selected_stage"):
        st.sidebar.success(f"✅ Finalized Stage: {st.session_state['selected_stage']}")

    selected_stage=st.session_state.get("selected_stage")

# ---------- Database selection ----------
st.subheader("📦 Select Iceberg Table")
with st.spinner("Fetching databases..."):
    try:
        databases=session.sql("SHOW DATABASES").to_pandas()
        if '"name"' in databases.columns:
            db_list=databases['"name"'].tolist()
        elif 'name' in databases.columns:
            db_list=databases['name'].tolist()
        else:
            db_list=databases.iloc[:,0].tolist()
        db_names=['Select One']+sorted(db_list)
    except Exception as e:
        st.error(f"Error retrieving databases: {e}")
        st.stop()

selected_db=st.selectbox("Select a Database",db_names,key="selected_db")
if selected_db=="Select One":
    st.info("Please select a database to continue.")
    st.stop()

# ---------- Iceberg Tables ----------
with st.spinner("Fetching Iceberg tables..."):
    try:
        iceberg_query=f"""
            SELECT (TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME) AS TABLE_NAME,
                   ROW_COUNT,BYTES,CREATED,LAST_DDL,IS_DYNAMIC
            FROM {selected_db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE' AND IS_ICEBERG='YES'
        """
        iceberg_tables=session.sql(iceberg_query).to_pandas()
        table_names=['Select One']+sorted(iceberg_tables['TABLE_NAME'].tolist())
    except Exception as e:
        st.error(f"Error retrieving Iceberg tables: {e}")
        st.stop()

if "selected_table_final" not in st.session_state:
    st.session_state.selected_table_final=None

selected_table_choice=st.selectbox("Select an Iceberg Table",table_names,key="selected_table_choice")
if st.button("Submit Table"):
    if selected_table_choice!="Select One":
        st.session_state.selected_table_final=selected_table_choice
        st.sidebar.success(f"✅ Finalized Table: {selected_table_choice}")
    else:
        st.warning("Please select an Iceberg table before submitting.")

selected_table=st.session_state.get("selected_table_final")
if not selected_table:
    st.info("Please select an Iceberg table and click 'Submit Table' to continue.")
    st.stop()

# ---------- Show selected table metadata ----------
table_info = iceberg_tables[iceberg_tables['TABLE_NAME']==selected_table].iloc[0]
st.subheader(f"Metadata for {selected_table}")
st.write(f"**Table Name:** {table_info['TABLE_NAME']}")
st.write(f"**Row Count:** {table_info['ROW_COUNT']}")
st.write(f"**Size:** {format_bytes(table_info['BYTES'])}")
st.write(f"**Created:** {format_dates(table_info['CREATED'])}")
st.write(f"**Last DDL:** {format_dates(table_info['LAST_DDL'])}")
st.write(f"**Is Dynamic:** {table_info['IS_DYNAMIC']}")

# ---------- BASE_LOCATION ----------
with st.spinner("Getting BASE_LOCATION..."):
    try:
        ddl_df=session.sql(f"SELECT GET_DDL('TABLE','{selected_table}') AS DDL").to_pandas()
        ddl=ddl_df['DDL'][0]
        match=re.search(r"BASE_LOCATION\s*=\s*['\"]([^'\"]+)['\"]",ddl,re.IGNORECASE|re.MULTILINE)
        base_location=match.group(1) if match else None
        if not base_location:
            for line in ddl.splitlines():
                if "BASE_LOCATION" in line.upper():
                    base_location=line.split("=")[1].strip(" '\"\t,")
                    break
        if not base_location:
            st.error("❌ BASE_LOCATION not found in DDL.")
            st.code(ddl)
            st.stop()
        base_location=base_location.rstrip("/")
        st.success(f"📂 BASE_LOCATION: `{base_location}`")
    except Exception as e:
        st.error(f"Error extracting BASE_LOCATION from DDL: {e}")
        st.stop()

# ---------- Resolve External Volume and LS pattern ----------
with st.spinner("Resolving External Volume and file prefix..."):
    try:
        ev_match = re.search(r"EXTERNAL_VOLUME\s*=\s*['\"]([^'\"]+)['\"]", ddl, re.IGNORECASE)
        external_volume_name = ev_match.group(1) if ev_match else None
        if not external_volume_name:
            st.error("❌ EXTERNAL_VOLUME not found in table DDL.")
            st.code(ddl)
            st.stop()

        ev_sql = f"""
            SELECT S3_PATH
            FROM METADATA_VIEWER_DB.APP_SETUP.EXTERNAL_VOLUME_PATHS
            WHERE VOLUME_NAME='{external_volume_name}'
        """
        ev_df = session.sql(ev_sql).to_pandas()
        ev_s3_path = ev_df['S3_PATH'].iloc[0].rstrip("/") if not ev_df.empty else None
        if not ev_s3_path:
            st.error(f"❌ S3_PATH not found for External Volume: {external_volume_name}")
            st.stop()

        ev_s3_path = ev_s3_path.rstrip("/").rstrip("/*").rstrip("/")
        stage_row = stages_df.loc[stages_df['FULL_NAME'] == selected_stage]
        stage_url = stage_row['URL'].iloc[0].rstrip("/") if not stage_row.empty else ""
        stage_url_slash = stage_url + '/' if not stage_url.endswith('/') else stage_url
        if ev_s3_path.startswith(stage_url_slash):
            relative_prefix = ev_s3_path[len(stage_url_slash):]
        else:
            relative_prefix = ev_s3_path.split('/', 3)[-1]
        if base_location:
            relative_prefix = "/".join([relative_prefix.rstrip("/"), base_location.lstrip("/")])
        ls_pattern = f"{relative_prefix}.*"
        st.success(f"Resolved LS pattern: `{ls_pattern}`")
    except Exception as e:
        st.error(f"Error resolving External Volume path: {e}")
        st.stop()

# ---------- List files ----------
with st.spinner("Listing files..."):
    try:
        if not selected_stage:
            st.warning("Please select and submit a Stage to list files.")
            st.stop()
        ls_sql = f"LS @{selected_stage} PATTERN='{ls_pattern}'"
        files_df = session.sql(ls_sql).to_pandas()
        if files_df.empty:
            st.warning("No files found at the base location.")
            st.stop()

        files_df.columns = [c.strip('"').upper() for c in files_df.columns]
        files_df = files_df[~files_df['NAME'].str.endswith(('.crc', '.bin'))]
        files_df['LABEL'] = files_df['NAME'] + " | " + files_df['LAST_MODIFIED'].astype(str)
        file_labels = sorted(files_df['LABEL'].tolist())
    except Exception as e:
        st.error(f"Error listing files from stage: {e}")
        st.stop()

# ---------- File Selection ----------
st.subheader("📄 Select or Search File")
file_selection_method=st.radio("Choose file selection method:",["Dropdown","Search by Name"],horizontal=True)
selected_file_only_path=None

if file_selection_method=="Dropdown":
    selected_file=st.selectbox("Select a File to View",['Select One']+file_labels,key="selected_file")
    if selected_file=="Select One":
        st.info("Please select a file to view.")
        st.stop()
    selected_file_only_path=selected_file.split(" | ")[0]
    if selected_file_only_path.startswith(stage_url):
        selected_file_only_path=selected_file_only_path[len(stage_url):].lstrip("/")
else:
    partial_name=st.text_input("Enter full or partial file name")
    if partial_name:
        matched_files=files_df[files_df['NAME'].str.contains(partial_name,case=False,na=False)]
        if matched_files.empty:
            st.warning("No files match the input. Adjust your search.")
            st.stop()
        elif len(matched_files)==1:
            selected_file_only_path=matched_files['NAME'].iloc[0]
            st.info(f"Auto-selected single match: {selected_file_only_path}")
            if selected_file_only_path.startswith(stage_url):
                selected_file_only_path=selected_file_only_path[len(stage_url):].lstrip("/")
        else:
            selected_choice=st.selectbox("Multiple matches found — choose one",matched_files['LABEL'].tolist())
            selected_file_only_path=selected_choice.split(" | ")[0]
            if selected_file_only_path.startswith(stage_url):
                selected_file_only_path=selected_file_only_path[len(stage_url):].lstrip("/")

# ---------- Parquet View Choice ----------
parquet_view_choice=None
if selected_file_only_path and selected_file_only_path.lower().endswith(".parquet"):
    parquet_view_choice=st.radio(
        "What do you want to display for this Parquet file?",
        ("Metadata Only","Sample Data Only","Both Metadata & Sample Data")
    )

# ---------- Read and analyze file ----------
if st.button("📖 Read File"):
    if not selected_file_only_path:
        st.warning("No file selected.")
        st.stop()
    st.success(f"Selected File: {selected_file_only_path}")
    tmp_dir=tempfile.mkdtemp(prefix="sf_stage_")
    local_file_path=os.path.join(tmp_dir,os.path.basename(selected_file_only_path))
    file_ext=os.path.splitext(local_file_path)[1].lower().lstrip(".")

    try:
        with st.spinner("Downloading file from stage..."):
            session.file.get(f"@{selected_stage}/{selected_file_only_path}", tmp_dir)

        with st.spinner("Reading and analyzing file..."):
            if file_ext in ["json","ndjson"]:
                records=[]
                with open(local_file_path,"r",encoding="utf-8") as f:
                    try:
                        obj=json.load(f)
                        records=obj if isinstance(obj,list) else [obj]
                    except json.JSONDecodeError:
                        for line in f:
                            line=line.strip()
                            if not line:
                                continue
                            try:
                                records.append(json.loads(line))
                            except Exception:
                                pass
                render_json_avro_view(records, local_file_path)

                with st.expander("📌 AI Summary", expanded=False):
                    ai_summary=safe_cortex_call(records)
                    st.text(ai_summary)

            elif file_ext=="avro":
                records=[]
                with open(local_file_path,'rb') as f:
                    reader = fastavro.reader(f)
                    for i, record in enumerate(reader):
                        records.append(record)
                        if i>=9999999:  # effectively all records
                            break
                render_json_avro_view(records, local_file_path)

                with st.expander("📌 AI Summary", expanded=False):
                    ai_summary=safe_cortex_call(records)
                    st.text(ai_summary)

            elif file_ext=="parquet":
                if parquet_view_choice in ["Metadata Only","Both Metadata & Sample Data"]:
                    metadata_dict = show_parquet_metadata(local_file_path)
                    render_parquet_view(metadata_dict)

                if parquet_view_choice in ["Sample Data Only","Both Metadata & Sample Data"]:
                    st.subheader("📑 Sample Data")
                    df_sample=pd.read_parquet(local_file_path)
                    st.dataframe(df_sample.head(100))

                with st.expander("📌 AI Summary", expanded=False):
                    metadata_dict = show_parquet_metadata(local_file_path)
                    ai_summary=safe_cortex_call(metadata_dict)
                    st.text(ai_summary)

            else:
                st.warning(f"Unsupported file type: .{file_ext}")

    except Exception as e:
        st.error(f"Error reading or analyzing file: {e}")
    finally:
        try:
            shutil.rmtree(tmp_dir)
        except Exception:
            pass
