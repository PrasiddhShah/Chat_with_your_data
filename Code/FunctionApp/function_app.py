import logging

import base64, json, requests, azure.functions as func
import urllib.parse
import pyodbc
from dateutil import parser
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime
from azure.identity import ClientSecretCredential

import pandas as pd
from io import StringIO as StrongIO

 # Configure logging once
 # This is a Python Azure Function that connects to an Azure SQL database, fetches metadata from Documentum, and uploads files to Azure Blob Storage.
keyvault_url = ""
credential = DefaultAzureCredential()
keyvault_name = ""
secret_client = SecretClient(vault_url=keyvault_url, credential=credential)

execution_logs = []

def log_function(function_name, start, end):
    duration = (end - start).total_seconds()
    entry = f"{function_name} | {start} | {end} | {duration:.2f}"
    execution_logs.append(entry)
def save_logs_to_adls(AZURE_CONNECT, filename="execution_logs.txt"):
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECT)
    blob_client = blob_service.get_blob_client(container="raw", blob=filename)
    log_data = "\n".join(execution_logs)
    blob_client.upload_blob(log_data, overwrite=True)

def odbc_connect(client_id, client_secret):
    """Establishes a connection to the SQL database."""
    conn = pyodbc.connect(
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:czv-g-d-gendif-sql-06.database.usgovcloudapi.net,1433;"
    "Database=DataIngestionDev-DB-01;"
    "Authentication=ActiveDirectoryServicePrincipal;"
    f"UID={client_id};"
    f"Pwd={client_secret};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
    )
    return conn
def pipeline_connect():
    TenantID = ""
    SynapseSPNClientID = ""
    client_secret = ""
    synapse_url = ""
    pipline_name = "Documentum"
    api_version = "2021-06-01"

    url = f"{synapse_url}/pipelines/Documentum/createRun?api-version={api_version}"

    credential = ClientSecretCredential(
        tenant_id=TenantID,
        client_id=SynapseSPNClientID,
        client_secret=client_secret
    )
    token = credential.get_token("")
    headers = {
        "Authorization": f"Bearer {token.token}",
        "Content-Type": "application/json"
    }   
    request = requests.post(url, headers=headers, json={})
    logging.info(f"Pipeline run response: {request.status_code} - {request.text}")

def load_config(conn):

    get_table = """
    SELECT 
        mode, 
        FORMAT(load_ts, 'MM-dd-yyyy HH:mm:ss') AS formatted_load_ts
    FROM dbo.Config;
 """

    ans = conn.execute(get_table).fetchall()
    if ans[0][0] == "Selective":
        return ans[0][0]
    else:
        date = ans[0][1] if ans else None
        return date


def doc_list(documentum_tables, date, username, password, page_num=1) -> list:
    """Fetches Documentum object IDs from the provided URLs."""
    start = datetime.utcnow()
    base_url = ""
    credentials = f"{username}:{password}"
    count = 0
    file_id = list()
    last_page = True
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    headers = {
        "Accept": "application/vnd.emc.documentum+json",
        "Authorization": f"Basic {encoded_credentials}"
    }
    urls = []
    for documentum_table in documentum_tables:
        dql = f"SELECT r_object_id, r_modify_date FROM {documentum_table} WHERE acl_name LIKE '%Issued' AND r_modify_date >= DATE('{date}', 'MM-dd-yyyy HH:mm:ss') ORDER BY r_object_id"
        encoded_dql = urllib.parse.quote(dql, safe="")          # URL-encode the entire query
        full_url = f"{base_url}?dql={encoded_dql}"  
        urls.append(full_url)
    
    for url in urls:
        page_num = 1  # reset per URL
        while last_page:
            content_url = f"{url}&page={page_num}"
            response = requests.get(content_url, headers=headers)
            data = response.json()
            if not data.get('entries'):
                break
            for entry in data.get('entries', []):
                file_id.append(entry["content"]["properties"]["r_object_id"])
                count += 1
            page_num += 1
    end = datetime.utcnow()
    log_function("Doc_id_extraction", start, end)
    return file_id, count


def metadata_extraction(username, password, doc_id, conn):
    """Extracts metadata from Documentum objects based on their IDs."""
    start = datetime.utcnow()
    cursor = conn.cursor()
    for id in doc_id:
        content_url = f""
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        headers = {
            "Accept": "application/vnd.emc.documentum+json",
            "Authorization": f"Basic {encoded_credentials}"
        }
        response = requests.get(content_url, headers=headers)
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            continue
        data = response.json()
        properties = data.get("properties", {})    
        created_date_raw = properties.get("r_creation_date")
        if created_date_raw:
            created_date = parser.isoparse(created_date_raw).strftime('%m/%d/%Y %H:%M:%S')
        else:
            created_date = None

        updated_date_raw = properties.get("r_modify_date")
        if updated_date_raw:
            updated_date = parser.isoparse(updated_date_raw).strftime('%m/%d/%Y %H:%M:%S')
        else:
            updated_date = None

        status_date_raw = properties.get("status_date")
        if status_date_raw:
            status_date = parser.isoparse(status_date_raw).strftime('%m/%d/%Y %H:%M:%S')
        else:
            status_date = None

        doc_date_raw = properties.get("doc_date")
        if doc_date_raw:
            doc_date = parser.isoparse(doc_date_raw).strftime('%m/%d/%Y %H:%M:%S')
        else:
            doc_date = None  # or a default like "01/01/1900 00:00:00"

        r_creation_date_raw = properties.get("r_creation_date")
        if r_creation_date_raw:
            r_creation_date = parser.isoparse(r_creation_date_raw).strftime('%m/%d/%Y %H:%M:%S')
        else:
            r_creation_date = None  # or a default like "01/01/1900 00:00:00"

        metadata = {
            "doc_id": properties.get("r_object_id", "No r_object_id found"),
            "cabinet": properties.get("i_cabinet_id", "No i_cabinet_id found"),
            "folder_id": properties.get("i_folder_id", [None])[0],
            "doc_name": properties.get("object_name", "No object_name found"),
            "doc_number": properties.get("doc_number", "No doc_number found"),
            "doc_format": properties.get("a_content_type", "No a_content_type found"),
            "created_date": created_date,
            "updated_date": updated_date,
            "created_by": properties.get("r_creator_name", "No r_creator found"),
            "facility": properties.get("facility", "No facility found"),
            "doc_type": properties.get("doc_type", "No doc_type found"),
            "sub_type": properties.get("sub_type", "No sub_type found"),
            "doc_number": properties.get("doc_number", "No doc_number found"),
            "sheet": properties.get("sheet", "No sheet found"),
            "major_rev": properties.get("major_rev", "No major_rev found"),
            "minor_rev": properties.get("minor_rev", "No minor_rev found"),
            "status_date": status_date,
            "status": properties.get("status", "No status found"),
            "doc_date": doc_date,
            "unit": properties.get("unit", "No unit found"),
            "d_code": properties.get("d_code", "No d_code found"),
            "rev_index": properties.get("rev_index", "No rev_index found"),
            "sec_flag": properties.get("sec_flag", "No sec_flag found"),
            "r_version_label":",".join(data.get("properties", {}).get("r_version_label", "No r_version_label found")),
            "title": properties.get("title", "No title found"),
            "subject": properties.get("subject", "No subject found"),
            "authors": ','.join(data.get("properties", {}).get("authors", "No authors found")),
            "r_content_size": properties.get("r_content_size", 0)/1024,
            "owner_name": properties.get("owner_name", "No owner_name found"),
            "r_modifier": properties.get("r_modifier", "No r_modifier found"),
            "r_access_date": r_creation_date,
            "a_storage_type": properties.get("a_storage_type", "No a_storage_type found"),
            "r_immutable_flag": properties.get("r_immutable_flag", "No r_immutable_flag found"),

            "active_flag": "active"
        }
        columns = list(metadata.keys())
        update_columns = [col for col in columns if col != "doc_id"]
        column_list = ', '.join(columns)
        insert_placeholders = ', '.join(['?'] * len(columns))
        update_clause = ', '.join([f"{col} = source.{col}" for col in update_columns])
        merge_sql = f"""
        MERGE dbo.DocumentumFileMetadata AS target
        USING (SELECT ? AS {columns[0]}""" + ''.join([f", ? AS {col}" for col in columns[1:]]) + f""") AS source
        ON target.doc_id = source.doc_id
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({column_list})
            VALUES ({insert_placeholders});
        """

        params = list(metadata.values()) * 2  # For SELECT and INSERT
        cursor = conn.cursor()
        cursor.execute(merge_sql, params)
        conn.commit()
    end = datetime.utcnow()
    log_function("Save_Metadata", start, end)



def source_raw(username, password, CONTAINER, conn, AZURE_CONNECT, start_date, mode="none"):
    start = datetime.utcnow()
    count = 0
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    headers = {
        "Accept": "application/vnd.emc.documentum+json",
        "Authorization": f"Basic {encoded_credentials}"
    }
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECT)
    if mode == "Selective":
        query = "SELECT * FROM dbo.DocumentumFileMetadata WHERE active_flag = 'active'"
        df = pd.read_sql(query, conn)
        object_ids = df['doc_id'].tolist()
    else:
        query = f"""
            SELECT doc_id
            FROM dbo.DocumentumFileMetadata
            WHERE updated_date > '{start_date}'
            """
        df = pd.read_sql(query, conn)
        object_ids = df['doc_id'].tolist()
        
    for object_id in object_ids:
        count += 1
        failure_count = 0
        failure_count_download = 0
        logging.info("Processing OBJECT_ID: %s (%d/%d)", object_id, count, len(object_ids))
        meta_url = f""
        r = requests.get(meta_url, headers=headers)
        if r.status_code != 200:
            logging.error("Metadata call failed for %s: %s", object_id, r.text)
            failure_count += 1
            continue
        meta = r.json()
        file_name = meta.get("properties", {}).get("object_name", object_id)
        file_type = meta.get("properties", {}).get("dos_extension", "unknown")
        file_download_url = None
        for link in meta.get("links", []):
            if link.get("rel") == "http://identifiers.emc.com/linkrel/content-media":
                file_download_url = link.get("href")
                break
        if not file_download_url:
            logging.warning("No download URL found for %s", object_id)
            continue
        blob_name = f"{file_type}/{file_name}.{file_type}"
        f = requests.get(file_download_url, headers=headers)
        logging.info("Downloading %s", file_download_url)
        if f.status_code != 200 or f.status_code == 500:
            logging.error("Download failed %s", file_download_url)
            failure_count_download += 1
            continue
        blob_service.get_blob_client(CONTAINER, blob_name).upload_blob(f.content, overwrite=True)
        logging.info("Uploaded %s", blob_name)
    logging.info("Processed %d files with %d failures and %d download failures", count, failure_count, failure_count_download)
    end = datetime.utcnow()
    log_function("File_Download", start, end)

def try_parse_datetime(value, fmt="%m-%d-%Y %H:%M:%S"):

    try:

        return datetime.strptime(value, fmt)

    except (ValueError, TypeError):

        return None

 

    
 
app = func.FunctionApp()
 
@app.function_name(name="http_trigger")
@app.route(route="fetch-and-upload", auth_level=func.AuthLevel.FUNCTION)  # use ANONYMOUS for open access


def http_trigger(req: func.HttpRequest) -> func.HttpResponse:

    try:

        logging.info("HTTP-triggered function started")
        username = secret_client.get_secret("DOCUMENTUM-USERNAME").value
        password = secret_client.get_secret("DOCUMENTUM-PASSWORD").value
        client_id = secret_client.get_secret("DIF-AzureSQL-DataIngestion-Virginia-ClientId").value
        client_secret = secret_client.get_secret("DIF-AzureSQL-DataIngestion-Virginia-ClientSecret").value
        AZURE_CONNECT = (
        )
        CONTAINER = "raw"
        file_name = "metadata.json"
        documentum_table = ["exelon_training", "exelon_procedure", "exelon_design"]
        conn = odbc_connect(client_id, client_secret)
        config_value = load_config(conn)
        start_date = try_parse_datetime(config_value)

        if start_date: 
            start_date = config_value
            doc_id = doc_list(documentum_table, start_date, username, password)
            metadata_extraction(username, password, doc_id[0], conn)
            #source_raw(username, password, CONTAINER, conn, AZURE_CONNECT, start_date, mode="none")
        else:
            mode = config_value
            start_date = None
            source_raw(username, password, CONTAINER, conn, AZURE_CONNECT, start_date, mode)
        #save_logs_to_adls(AZURE_CONNECT)
        #pipeline_connect()
        conn.close()
        return func.HttpResponse("Function completed successfully.", status_code=200)

    except Exception as e:

        logging.error(f"Function failed: {str(e)}")

        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

 