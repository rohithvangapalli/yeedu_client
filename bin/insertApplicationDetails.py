import sys
import psycopg2
import base64
import json
from jproperties import Properties
import logging


def logging_bootstrap():
    logging.basicConfig(format='[%(asctime)s] \t %(levelname)s:%(message)s', level=logging.DEBUG)


"""
    Inserts job details into the Kosh database.

    This function reads the configuration from the nabu_common.properties file, establishes a connection
    to the Kosh database using the retrieved configuration, and inserts the job details into the
    nabu.spark_application_details table. The job details include the application ID, process ID, batch ID,
    retry number, additional information in JSON format, and the current timestamp.
    Returns:
        None
"""
def insert_job_details():
    configs = Properties()
    # Load configuration from nabu_common.properties file
    with open(F'{NABU_SPARK_BOT_HOME}/../common-lib/src/main/resources/nabu_common.properties', 'rb') as config_file:
        configs.load(config_file)
    # Establish connection to the Kosh database
    conn = psycopg2.connect(database=configs.get("NABU_KOSH_SERVICE_DATABASE").data, user=configs.get("NABU_KOSH_SERVICE_ACCOUNT").data, password=configs.get("NABU_KOSH_SERVICE_ACCOUNT_PASS").data, host=configs.get("NABU_KOSH_SERVICE_ENDPOINT").data, port= configs.get("NABU_KOSH_SERVICE_PORT").data)
    cursor = conn.cursor()
    # Construct and execute the SQL query to insert job details
    sql = """
        INSERT INTO nabu.spark_application_details 
        (application_id, process_id, batch_id, retry_num, additional_info, cru_ts) 
        VALUES (%s, %s, %s, %s, %s::json, current_timestamp);
    """

    if compute_engine == 'yeedu':
        data = (application_id, process_id, batch_id, retry_num,
                '{{"job_id": "{}", "job_name": "{}", "run_id": "{}", "job_type": "{}", "RunPageURL": "{}", "S3Logs": "{}", "CloudWatchLogs": "{}", "yeedu_tenant_id": "{}", "yeedu_workspace_id": "{}"}}'.format(job_id, job_name, run_id, job_type, run_page_url, s3_logs_url, cloudwatch_logs_url, yeedu_tenant_id, yeedu_workspace_id))
    else:
        data = (application_id, process_id, batch_id, retry_num,
                '{{"job_id": "{}", "job_name": "{}", "run_id": "{}", "job_type": "{}", "RunPageURL": "{}", "S3Logs": "{}", "CloudWatchLogs": "{}"}}'.format(job_id, job_name, run_id, job_type, run_page_url, s3_logs_url, cloudwatch_logs_url))

    cursor.execute(sql, data)
    conn.commit()

    logging.info("[+] Inserted job details into Kosh")
    # Close the database connection
    conn.close()


# main class
if __name__ == "__main__":
    logging_bootstrap()
    NABU_SPARK_BOT_HOME = sys.argv[1]
    process_id = sys.argv[2]
    batch_id = sys.argv[3]
    retry_num = sys.argv[4]
    job_name=sys.argv[5]
    job_id = sys.argv[6]
    run_id = sys.argv[7]
    application_id = sys.argv[8]
    job_type = sys.argv[9]
    run_page_url = sys.argv[10]
    s3_logs_url = sys.argv[11]
    cloudwatch_logs_url = sys.argv[12]
    base64_encoded_json = sys.argv[13]
    decoded_additional_info_json = base64.b64decode(base64_encoded_json).decode('utf-8')
    additional_info_json = json.loads(decoded_additional_info_json)
    compute_engine = additional_info_json.get('compute_engine')

    if compute_engine == 'yeedu':
        yeedu_tenant_id = additional_info_json.get('yeedu_tenant_id')
        yeedu_workspace_id = additional_info_json.get('yeedu_workspace_id')

    insert_job_details()
