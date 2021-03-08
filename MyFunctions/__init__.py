from azure.storage.blob import ContainerPermissions
from datetime import datetime, timedelta
import pyodbc
import pandas as pd
import logging

def get_SAS_URL(fileURL,
                block_blob_service,
                container):

    sasTokenRead = block_blob_service.generate_container_shared_access_signature(
    container_name=container,
    permission=ContainerPermissions.READ,
    expiry=datetime.utcnow() + timedelta(days=1)
    )
    return f"{fileURL}?{sasTokenRead}"

def get_df_from_sqlQuery(
    sqlQuery,
    database
):
    ## Create connection string
    connectionString = get_connection_string(database)
    logging.info(f'Connection string created: {connectionString}')
    ## Execute SQL query and get results into df 
    with pyodbc.connect(connectionString) as conn:
        ## Get SQL table in pandas DataFrame
        df = pd.read_sql(sql=sqlQuery,
                            con=conn)
    return df

def get_connection_string(database):
    username = 'matt.shepherd'
    password = "4rsenal!PG01"
    driver = '{ODBC Driver 17 for SQL Server}'
    # driver = 'SQL Server Native Client 11.0'
    server = "fse-inf-live-uk.database.windows.net"
    # database = 'AzureCognitive'
    ## Create connection string
    connectionString = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    
    return connectionString

def get_VideoIndexerSplits_rows(
    subclipFileName
):
    q = f"""
    SELECT      *
    FROM        VideoIndexerSplits
    WHERE       VideoName = '{subclipFileName}'
    """
    df = get_df_from_sqlQuery(
        sqlQuery=q,
        database="AzureCognitive"
    )
    return df

def add_VideoIndexerSplits_row(
    subclipFileName
):
    q = f"""
    INSERT INTO VideoIndexerSplits (VideoName)
    VALUES ('{subclipFileName}')
    """

    run_sql_command(
        sqlQuery=q,
        database="AzureCognitive"
    )

def run_sql_command(
    sqlQuery,
    database
):
    ## Create connection string
    connectionString = get_connection_string(database)
    ## Run query
    with pyodbc.connect(connectionString) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sqlQuery)