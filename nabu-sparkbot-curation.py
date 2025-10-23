import base64
import json
import logging
import os
import sys
import time
import datetime
import requests
import subprocess
from Crypto.Cipher import AES
from jproperties import Properties

from client.workspace_manager import WorkspaceManager
from client.cluster_manager import ClusterManager
from client.object_storage import ObjectStorageManager
from client.jobs import JobsManager
from client.user import UserManager


def logging_bootstrap():
    try:
        logging.basicConfig(format='[%(asctime)s] \t %(levelname)s:%(message)s', level=logging.DEBUG)
        logging.info(F"[+] Logging nabu-sparkbot-curation Job ")
    except Exception as e:
        logging.error(f"Failed during logging bootstrap: {e}")
        sys.exit(1)

MAX_API_RETRIES = 3
RETRY_DELAY_SEC = 5

def api_call_with_retry(func, *args, **kwargs):
    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"[Attempt {attempt}] API call failed: {e}")
            if attempt < MAX_API_RETRIES:
                logging.info(f"Retrying in {RETRY_DELAY_SEC} seconds...")
                time.sleep(RETRY_DELAY_SEC)
            else:
                logging.error("Max retries reached, exiting.")
                sys.exit(1)


def execute_cmd(cmd):
    current_retry = 0

    while current_retry < 3:
        try:
            result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            data, err = result.communicate()

            if result.returncode == 0:
                return data, result.returncode
            else:
                current_retry += 1
                logging.error(F"[+] Exception:  Failed to run the command {cmd}. Retrying ({current_retry} of 3)")
                logging.error(F"[+] Error output: {err.decode('utf-8')}")
                time.sleep(2)

        except Exception as e:
            current_retry += 1
            logging.error(
                F"[+] An error occurred while executing the command {cmd}: {str(e)}. Retrying ({current_retry} of 3)")

    logging.error(F"[+] Exception:  Failed to run the command {cmd} after 3 retries.")
    sys.exit(1)

def get_var(varname, NABU_SPARK_BOT_HOME):
    try:
        get_var_cmd = F'echo $(source {NABU_SPARK_BOT_HOME}/yeedu/conf/small/nabu_spark_conf.sh; echo $%s)' % varname
        result = subprocess.Popen(get_var_cmd, stdout=subprocess.PIPE, shell=True, executable='/bin/bash',
                                  universal_newlines=True)
        return result.stdout.readlines()[0].strip()
    except Exception as e:
        logging.error(f"Error in get_var for {varname}: {e}")
        sys.exit(1)

def get_property_value(property_name, NABU_SPARK_BOT_HOME):
    try:
        configs = Properties()
        with open(F'{NABU_SPARK_BOT_HOME}/../common-lib/src/main/resources/nabu_common.properties', 'rb') as config_file:
            configs.load(config_file)
            value = configs.get(property_name).data
        return value
    except Exception as e:
        logging.error(f"Error in get_property_value for {property_name}: {e}")
        sys.exit(1)

def extract_key_value_pairs(extra_conf_json):
    try:
        config_map = extra_conf_json.get("extraConfigMap", {})
        entries = config_map.items()
        return [f"{key}={value}" for key, value in entries]
    except Exception as e:
        logging.error(f"Error in extract_key_value_pairs: {e}")
        sys.exit(1)

def AESDecryption(hex_str, NABU_SPARK_BOT_HOME):
    try:
        encrypted = bytes.fromhex(hex_str)
        key = open(f"{NABU_SPARK_BOT_HOME}/keys/keys/aesKey", "rb").read()
        cipher = AES.new(key, AES.MODE_ECB)
        decrypted = cipher.decrypt(encrypted)
        return decrypted.decode("utf-8").rstrip("\x00")
    except Exception as e:
        logging.error(f"Error in AESDecryption: {e}")
        sys.exit(1)

def decode_credentials(b64_json, NABU_SPARK_BOT_HOME):
    try:
        obj = json.loads(base64.b64decode(b64_json).decode("utf-8"))
        credential_id = obj.get("credential_id", "")
        credential_type_id = obj.get("credential_type_id", "")
        token_hex = obj.get("token", "")
        token = AESDecryption(token_hex, NABU_SPARK_BOT_HOME)
        endpoint = obj.get("credential_endpoint_url")
        return credential_id, credential_type_id, token, endpoint
    except Exception as e:
        logging.error(f"Error in decode_credentials: {e}")
        sys.exit(1)

def decode_hive_credentials(b64_json):
    try:
        obj = json.loads(base64.b64decode(b64_json).decode("utf-8"))
        return obj.get("credential_id", "").strip(), obj.get("credential_type_id", "").strip()
    except Exception as e:
        logging.error(f"Error in decode_hive_credentials: {e}")
        sys.exit(1)

def getCredentials(b64_compute_eng_creds_json, NABU_SPARK_BOT_HOME):
    try:
        credential_id, credential_type_id, token, credential_endpoint_url = decode_credentials(
            b64_compute_eng_creds_json, NABU_SPARK_BOT_HOME)

        payload = json.dumps({
            "credential_id": credential_id,
            "credential_type_id": credential_type_id
        })
        headers = {
            'Authorization': token,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logging.info("payload: ", payload)

        response = requests.request("POST", credential_endpoint_url, headers=headers, data=payload)
        logging.info(response)
        response_json = response.json()
        if response.status_code != 200 or 'expired' in str(response_json.get("data")):
            if response.status_code == 401:
                error_msg = response_json.get("error_msg")
                raise Exception(
                    f"Failed to fetch credentials, status code: {response.status_code}, response: {error_msg}")
            else:
                data = response_json.get("data")
                raise Exception(
                    f"Failed to fetch credentials, status code: {response.status_code}, response: {data}")
        elif response.status_code == 200 and 'User does not have ' in str(response_json.get("data")):
            data = response_json.get("data")
            raise Exception(f"Failed to fetch credentials, status code: {response.status_code}, response: {data}")

        credential_details = response_json
        logging.info(credential_details)
        tenant_id = credential_details['data']['tenant_id']
        token = credential_details['data']['token']

        return tenant_id,token
    except (ValueError, Exception) as e:
        logging.error(F"[+] Exception:  Error has occurred: {e}")
        sys.exit(1)


def getHiveCredentials(b64_hivecreds_json, NABU_SPARK_BOT_HOME):
    try:
        credential_id, credential_type_id, token, credential_endpoint_url = decode_credentials(b64_hivecreds_json,
                                                                                               NABU_SPARK_BOT_HOME)


        payload = json.dumps({
            "credential_id": credential_id,
            "credential_type_id": credential_type_id
        })
        headers = {
            'Authorization': token,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", credential_endpoint_url, headers=headers, data=payload)
        response_json = response.json()
        if response.status_code != 200 or 'expired' in str(response_json.get("data")):
            if response.status_code == 401:
                error_msg = response_json.get("error_msg")
                raise Exception(
                    f"Failed to fetch credentials, status code: {response.status_code}, response: {error_msg}")
            else:
                data = response_json.get("data")
                raise Exception(f"Failed to fetch credentials, status code: {response.status_code}, response: {data}")
        elif response.status_code == 200 and 'User does not have ' in str(response_json.get("data")):
            data = response_json.get("data")
            raise Exception(f"Failed to fetch credentials, status code: {response.status_code}, response: {data}")

        credential_details = response_json
        principal = credential_details['data']['principal']
        keytab = credential_details['data']['keytab']

        return principal, keytab
    except (ValueError, Exception) as e:
        logging.error(F"[+] Exception:  Error has occurred: {e}")
        sys.exit(1)


def upload_temp_resources(object_storage_mgr, osm_name, NABU_SPARK_BOT_HOME):
    try:
        logging.info("[+] Uploading temporary resources")

        temp_resources_list = [
            f"{NABU_SPARK_BOT_HOME}/yeedu/conf/small/application.conf",
            f"{NABU_SPARK_BOT_HOME}/keys/privateKey"
        ]

        for resource_path in temp_resources_list:
            if not os.path.exists(resource_path):
                logging.warning(f"File does not exist: {resource_path}")
                continue

            try:
                logging.info(f"Uploading temporary resource: {os.path.basename(resource_path)}")
                response = api_call_with_retry(object_storage_mgr.upload_file,
                                               file_path=resource_path,
                                               osm_name=osm_name,
                                               overwrite=True)
                logging.info(f"[+] Uploaded temporary resource {resource_path}: {response}")
            except Exception as e:
                logging.error(f"Failed to upload {resource_path}: {e}")
                sys.exit(1)
    except Exception as e:
        logging.error(f"upload_temp_resources failed: {e}")
        sys.exit(1)



def reupload_selected_jars(object_storage_mgr, osm_name):
    original_jars = ['file:///yeedu/object-storage-manager/mongo-spark-connector_2.12-10.4.1.jar',
                     'file:///yeedu/object-storage-manager/postgresql-42.7.3.jar',
                     f'file:///yeedu/object-storage-manager/hadoop-aws-{hadoop_version}.jar',
                     f'file:///yeedu/object-storage-manager/{NABU_SPARK_BOT_REFLECTION_3_2_JAR}',
                     'file:///yeedu/object-storage-manager/carolina-jdbc-2.4.4.jar',
                     'file:///yeedu/object-storage-manager/ojdbc8-23.5.0.24.07.jar',
                     'file:///yeedu/object-storage-manager/mssql-jdbc-8.2.0.jre8.jar',
                     'file:///yeedu/object-storage-manager/aws-java-sdk-1.11.163.jar',
                     'file:///yeedu/object-storage-manager/jdbcparallel-almaren_2.12-0.0.4-3.2.jar',
                     'file:///yeedu/object-storage-manager/almaren-framework_2.12-0.9.9-3.2.jar',
                     'file:///yeedu/object-storage-manager/redshift-jdbc42-2.1.0.28.jar',
                     'file:///yeedu/object-storage-manager/snowflake-jdbc-3.24.0.jar',
                     'file:///yeedu/object-storage-manager/spark-snowflake_2.12-3.1.1.jar',
                     'file:///yeedu/object-storage-manager/mysql-connector-j-8.4.0.jar',
                     'file:///yeedu/object-storage-manager/jtds-1.3.1.jar',
                     'file:///yeedu/object-storage-manager/spark-bigquery-with-dependencies_2.12-0.25.2.jar',
                     'file:///yeedu/object-storage-manager/ngdbc-2.11.17.jar',
                     'file:///yeedu/object-storage-manager/vertica-jdbc-12.0.2-0.jar',
                     f'file:///yeedu/object-storage-manager/hadoop-azure-datalake-{hadoop_version}.jar',
                     f'file:///yeedu/object-storage-manager/hadoop-azure-{hadoop_version}.jar',
                     'file:///yeedu/object-storage-manager/gcs-connector-hadoop3-latest.jar',
                     'file:///yeedu/object-storage-manager/spark-avro_2.12-3.2.4.jar',
                     'file:///yeedu/object-storage-manager/terajdbc-20.00.00.10.jar',
                     'file:///yeedu/object-storage-manager/jcc-11.5.9.0.jar',
                     'file:///yeedu/object-storage-manager/spark-hadoop-cloud_2.12-3.5.1.jar',
                     'file:///yeedu/object-storage-manager/delta-core_2.12-2.0.0.jar',
                     'file:///yeedu/object-storage-manager/azure-data-lake-store-sdk-2.3.8.jar',
                     'file:///yeedu/object-storage-manager/wildfly-openssl-1.0.7.Final.jar',
                     'file:///yeedu/object-storage-manager/delta-storage-2.0.0.jar',
                     'file:///yeedu/object-storage-manager/kafka-clients-3.7.1.jar',
                     'file:///yeedu/object-storage-manager/spark-sql-kafka-0-10_2.12-3.2.4.jar',
                     'file:///yeedu/object-storage-manager/spark-token-provider-kafka-0-10_2.12-3.2.4.jar',
                     'file:///yeedu/object-storage-manager/glow-spark3_2.12-1.2.1.jar',
                     'file:///yeedu/object-storage-manager/jdbi-2.63.1.jar',
                     'file:///yeedu/object-storage-manager/htsjdk-3.0.1.jar',
                     'file:///yeedu/object-storage-manager/hadoop-bam-7.10.0.jar',
                     'file:///yeedu/object-storage-manager/tagsoup-1.2.1.jar',
                     'file:///yeedu/object-storage-manager/xmpcore-6.1.11.jar',
                     'file:///yeedu/object-storage-manager/pdfbox-2.0.26.jar',
                     'file:///yeedu/object-storage-manager/metadata-extractor-2.19.0.jar',
                     'file:///yeedu/object-storage-manager/poi-5.2.2.jar',
                     'file:///yeedu/object-storage-manager/pdfbox-tools-2.0.26.jar',
                     'file:///yeedu/object-storage-manager/tika-parsers-1.28.5.jar',
                     'file:///yeedu/object-storage-manager/fontbox-2.0.26.jar',
                     'file:///yeedu/object-storage-manager/commons-exec-1.3.jar',
                     'file:///yeedu/object-storage-manager/tika-core-1.28.5.jar',
                     'file:///yeedu/object-storage-manager/jempbox-1.8.16.jar',
                     'file:///yeedu/object-storage-manager/commons-csv-1.10.0.jar',
                     'file:///yeedu/object-storage-manager/spark-xml_2.12-0.14.0.jar',
                     'file:///yeedu/object-storage-manager/commons-pool2-2.11.1.jar',
                     'file:///yeedu/object-storage-manager/azure-eventhubs-spark_2.12-2.3.22.jar',
                     'file:///yeedu/object-storage-manager/azure-eventhubs-3.3.0.jar',
                     'file:///yeedu/object-storage-manager/scala-java8-compat_3-1.0.2.jar',
                     'file:///yeedu/object-storage-manager/proton-j-0.34.1.jar',
                     'file:///yeedu/object-storage-manager/jetty-util-ajax-11.0.25.jar',
                     'file:///yeedu/object-storage-manager/azure-storage-8.6.6.jar',
                     'file:///yeedu/object-storage-manager/jetty-util-9.4.43.v20210629.jar',
                     'file:///yeedu/object-storage-manager/iceberg-spark-runtime_3_2.jar'
                     ]

    local_jar_map = {
        "carolina-jdbc-2.4.4.jar": f"{NABU_SPARK_BOT_HOME}/jars/carolina-jdbc-2.4.4.jar",
        f"hadoop-aws-{hadoop_version}.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/{hadoop_version}/hadoop-aws-{hadoop_version}.jar",
        "wildfly-openssl-1.0.7.Final.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar",
        "azure-data-lake-store-sdk-2.3.8.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/microsoft/azure/azure-data-lake-store-sdk/2.3.8/azure-data-lake-store-sdk-2.3.8.jar",
        f"{NABU_SPARK_BOT_REFLECTION_3_2_JAR}": f"{NABU_SPARK_BOT_HOME}/jars/2.12/{NABU_SPARK_BOT_REFLECTION_3_2_JAR}",
        "gcs-connector-hadoop3-latest.jar": f"{NABU_SPARK_BOT_HOME}/jars/gcs-connector-hadoop3-latest.jar",
        "ojdbc8-23.5.0.24.07.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/23.5.0.24.07/ojdbc8-23.5.0.24.07.jar",
        "mssql-jdbc-8.2.0.jre8.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/8.2.0.jre8/mssql-jdbc-8.2.0.jre8.jar",
        "aws-java-sdk-1.11.163.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.163/aws-java-sdk-1.11.163.jar",
        "almaren-framework_2.12-0.9.9-3.2.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/github/music-of-the-ainur/almaren-framework_2.12/0.9.9-3.2/almaren-framework_2.12-0.9.9-3.2.jar",
        "jdbcparallel-almaren_2.12-0.0.4-3.2.jar": f"{NABU_SPARK_BOT_HOME}/jars/jdbcparallel-almaren_2.12-0.0.4-3.2.jar",
        "tagsoup-1.2.1.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/ccil/cowan/tagsoup/tagsoup/1.2.1/tagsoup-1.2.1.jar",
        "xmpcore-6.1.11.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/adobe/xmp/xmpcore/6.1.11/xmpcore-6.1.11.jar",
        "pdfbox-2.0.26.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/pdfbox/pdfbox/2.0.26/pdfbox-2.0.26.jar",
        "metadata-extractor-2.19.0.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/drewnoakes/metadata-extractor/2.19.0/metadata-extractor-2.19.0.jar",
        "poi-5.2.2.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/poi/poi/5.2.2/poi-5.2.2.jar",
        "pdfbox-tools-2.0.26.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/pdfbox/pdfbox-tools/2.0.26/pdfbox-tools-2.0.26.jar",
        "tika-parsers-1.28.5.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/tika/tika-parsers/1.28.5/tika-parsers-1.28.5.jar",
        "fontbox-2.0.26.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/pdfbox/fontbox/2.0.26/fontbox-2.0.26.jar",
        "commons-exec-1.3.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/commons/commons-exec/1.3/commons-exec-1.3.jar",
        "tika-core-1.28.5.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/tika/tika-core/1.28.5/tika-core-1.28.5.jar",
        "jempbox-1.8.16.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/pdfbox/jempbox/1.8.16/jempbox-1.8.16.jar",
        "postgresql-42.7.3.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar",
        "mysql-connector-j-8.4.0.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar",
        "jtds-1.3.1.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/net/sourceforge/jtds/jtds/1.3.1/jtds-1.3.1.jar",
        "spark-bigquery-with-dependencies_2.12-0.25.2.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.25.2/spark-bigquery-with-dependencies_2.12-0.25.2.jar",
        "redshift-jdbc42-2.1.0.28.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/2.1.0.28/redshift-jdbc42-2.1.0.28.jar",
        "snowflake-jdbc-3.24.0.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.24.0/snowflake-jdbc-3.24.0.jar",
        "spark-snowflake_2.12-3.1.1.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/3.1.1/spark-snowflake_2.12-3.1.1.jar",
        "ngdbc-2.11.17.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/sap/cloud/db/jdbc/ngdbc/2.11.17/ngdbc-2.11.17.jar",
        "spark-avro_2.12-3.2.4.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.2.4/spark-avro_2.12-3.2.4.jar",
        f"hadoop-azure-{hadoop_version}.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/{hadoop_version}/hadoop-azure-{hadoop_version}.jar",
        f"hadoop-azure-datalake-{hadoop_version}.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/{hadoop_version}/hadoop-azure-datalake-{hadoop_version}.jar",
        "mongo-spark-connector_2.12-10.4.1.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.4.1/mongo-spark-connector_2.12-10.4.1.jar",
        "vertica-jdbc-12.0.2-0.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.2-0/vertica-jdbc-12.0.2-0.jar",
        "terajdbc-20.00.00.10.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/teradata/jdbc/terajdbc/20.00.00.10/terajdbc-20.00.00.10.jar",
        "jcc-11.5.9.0.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar",
        "spark-hadoop-cloud_2.12-3.5.1.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/3.5.1/spark-hadoop-cloud_2.12-3.5.1.jar",
        "delta-core_2.12-2.0.0.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/io/delta/delta-core_2.12/2.0.0/delta-core_2.12-2.0.0.jar",
        "delta-storage-2.0.0.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/io/delta/delta-storage/2.0.0/delta-storage-2.0.0.jar",
        "kafka-clients-3.7.1.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.1/kafka-clients-3.7.1.jar",
        "spark-sql-kafka-0-10_2.12-3.2.4.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.4/spark-sql-kafka-0-10_2.12-3.2.4.jar",
        "spark-token-provider-kafka-0-10_2.12-3.2.4.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.2.4/spark-token-provider-kafka-0-10_2.12-3.2.4.jar",
        "glow-spark3_2.12-1.2.1.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/io/projectglow/glow-spark3_2.12/1.2.1/glow-spark3_2.12-1.2.1.jar",
        "jdbi-2.63.1.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/jdbi/jdbi/2.63.1/jdbi-2.63.1.jar",
        "htsjdk-3.0.1.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/github/samtools/htsjdk/3.0.1/htsjdk-3.0.1.jar",
        "hadoop-bam-7.10.0.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/seqdoop/hadoop-bam/7.10.0/hadoop-bam-7.10.0.jar",
        "commons-csv-1.10.0.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/org/apache/commons/commons-csv/1.10.0/commons-csv-1.10.0.jar",
        "spark-xml_2.12-0.14.0.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.14.0/spark-xml_2.12-0.14.0.jar",
        "protostuff-core-1.7.4.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/io/protostuff/protostuff-core/1.7.4/protostuff-core-1.7.4.jar",
        "protostuff-api-1.7.4.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/io/protostuff/protostuff-api/1.7.4/protostuff-api-1.7.4.jar",
        "protostuff-runtime-1.7.4.jar": f"{NABU_SPARK_BOT_HOME}/jars/https/repo1.maven.org/maven2/io/protostuff/protostuff-runtime/1.7.4/protostuff-runtime-1.7.4.jar",
    }

    try:
        existing_files_resp = api_call_with_retry(object_storage_mgr.list_files, osm_name=osm_name)
        existing_paths = [item["full_file_path"] for item in existing_files_resp.get("data", [])]
        logging.info("Existing files in object storage:")
        logging.info(existing_paths)
    except Exception as e:
        logging.error(f"Failed to fetch file list from OSM: {e}")
        return

    for jar_uri in original_jars:
        jar_name = os.path.basename(jar_uri)
        if jar_uri not in existing_paths:
            local_path = local_jar_map.get(jar_name)
            if not local_path or not os.path.exists(local_path):
                logging.warning(f"Local JAR not found for {jar_name}: {local_path}")
                continue

            try:
                logging.info(f"Uploading missing jar: {jar_name}")
                response = api_call_with_retry(object_storage_mgr.upload_file,
                                               file_path=local_path,
                                               osm_name=osm_name,
                                               overwrite=True)
                logging.info(f"Uploaded {jar_name}: {response}")
            except Exception as e:
                logging.error(f"Failed to upload {jar_name}: {e}")
        else:
            logging.info(f"{jar_name} already exists in object storage, skipping.")


def create_job(jobs_client):
    external = json.loads(base64.b64decode(b64_external_jars_map).decode("utf-8"))
    jars_list = []
    if external.get("external_jars"):
        ext_list = external.get("external_jars_list", [])
        jars_list = [f"/yeedu/object-storage-manager/{j}" for j in ext_list]

    total_jars = [
        "/yeedu/object-storage-manager/mongo-spark-connector_2.12-10.4.1.jar",
        "/yeedu/object-storage-manager/postgresql-42.7.3.jar",
        f"/yeedu/object-storage-manager/hadoop-aws-{hadoop_version}.jar",
        f"/yeedu/object-storage-manager/{NABU_SPARK_BOT_REFLECTION_3_2_JAR}",
        "/yeedu/object-storage-manager/carolina-jdbc-2.4.4.jar",
        "/yeedu/object-storage-manager/ojdbc8-23.5.0.24.07.jar",
        "/yeedu/object-storage-manager/mssql-jdbc-8.2.0.jre8.jar",
        "/yeedu/object-storage-manager/aws-java-sdk-1.11.163.jar",
        "/yeedu/object-storage-manager/jdbcparallel-almaren_2.12-0.0.4-3.2.jar",
        "/yeedu/object-storage-manager/almaren-framework_2.12-0.9.9-3.2.jar",
        "/yeedu/object-storage-manager/redshift-jdbc42-2.1.0.28.jar",
        "/yeedu/object-storage-manager/snowflake-jdbc-3.24.0.jar",
        "/yeedu/object-storage-manager/spark-snowflake_2.12-3.1.1.jar",
        "/yeedu/object-storage-manager/mysql-connector-j-8.4.0.jar",
        "/yeedu/object-storage-manager/jtds-1.3.1.jar",
        "/yeedu/object-storage-manager/spark-bigquery-with-dependencies_2.12-0.25.2.jar",
        "/yeedu/object-storage-manager/ngdbc-2.11.17.jar",
        "/yeedu/object-storage-manager/vertica-jdbc-12.0.2-0.jar",
        f"/yeedu/object-storage-manager/hadoop-azure-datalake-{hadoop_version}.jar",
        f"/yeedu/object-storage-manager/hadoop-azure-{hadoop_version}.jar",
        "/yeedu/object-storage-manager/gcs-connector-hadoop3-latest.jar",
        "/yeedu/object-storage-manager/spark-avro_2.12-3.2.4.jar",
        "/yeedu/object-storage-manager/terajdbc-20.00.00.10.jar",
        "/yeedu/object-storage-manager/jcc-11.5.9.0.jar",
        "/yeedu/object-storage-manager/spark-hadoop-cloud_2.12-3.5.1.jar",
        "/yeedu/object-storage-manager/delta-core_2.12-2.0.0.jar",
        "/yeedu/object-storage-manager/azure-data-lake-store-sdk-2.3.8.jar",
        "/yeedu/object-storage-manager/wildfly-openssl-1.0.7.Final.jar",
        "/yeedu/object-storage-manager/delta-storage-2.0.0.jar",
        "/yeedu/object-storage-manager/kafka-clients-3.7.1.jar",
        "/yeedu/object-storage-manager/spark-sql-kafka-0-10_2.12-3.2.4.jar",
        "/yeedu/object-storage-manager/spark-token-provider-kafka-0-10_2.12-3.2.4.jar",
        "/yeedu/object-storage-manager/glow-spark3_2.12-1.2.1.jar",
        "/yeedu/object-storage-manager/jdbi-2.63.1.jar",
        "/yeedu/object-storage-manager/htsjdk-3.0.1.jar",
        "/yeedu/object-storage-manager/hadoop-bam-7.10.0.jar",
        "/yeedu/object-storage-manager/tagsoup-1.2.1.jar",
        "/yeedu/object-storage-manager/xmpcore-6.1.11.jar",
        "/yeedu/object-storage-manager/pdfbox-2.0.26.jar",
        "/yeedu/object-storage-manager/metadata-extractor-2.19.0.jar",
        "/yeedu/object-storage-manager/poi-5.2.2.jar",
        "/yeedu/object-storage-manager/pdfbox-tools-2.0.26.jar",
        "/yeedu/object-storage-manager/tika-parsers-1.28.5.jar",
        "/yeedu/object-storage-manager/fontbox-2.0.26.jar",
        "/yeedu/object-storage-manager/commons-exec-1.3.jar",
        "/yeedu/object-storage-manager/tika-core-1.28.5.jar",
        "/yeedu/object-storage-manager/jempbox-1.8.16.jar",
        "/yeedu/object-storage-manager/commons-csv-1.10.0.jar",
        "/yeedu/object-storage-manager/spark-xml_2.12-0.14.0.jar",
        "/yeedu/object-storage-manager/commons-pool2-2.11.1.jar",
        "/yeedu/object-storage-manager/azure-eventhubs-spark_2.12-2.3.22.jar",
        "/yeedu/object-storage-manager/azure-eventhubs-3.3.0.jar",
        "/yeedu/object-storage-manager/scala-java8-compat_3-1.0.2.jar",
        "/yeedu/object-storage-manager/proton-j-0.34.1.jar",
        "/yeedu/object-storage-manager/jetty-util-ajax-11.0.25.jar",
        "/yeedu/object-storage-manager/azure-storage-8.6.6.jar",
        "/yeedu/object-storage-manager/jetty-util-9.4.43.v20210629.jar",
        "/yeedu/object-storage-manager/iceberg-spark-runtime_3_2.jar"
    ]

    jars_list.extend(total_jars)

    fmt = json.loads(base64.b64decode(b64_file_format_json).decode("utf-8"))
    isIceberg = fmt.get("isIcebergeEnabled", False)
    isDelta = fmt.get("isDeltaEnabled", False)


    driver_memory = executor_memory = driver_cores = executor_cores = num_executors = total_executor_cores = None
    principal = keytab = None

    if cluster_ui in ("STANDALONE", "CLUSTER"):
        spark_cfg = {}
        if input_args_json.get("spark_configs"):
            spark_cfg = json.loads(base64.b64decode(input_args_json["spark_configs"]).decode("utf-8"))
            driver_memory = spark_cfg.get("driver_memory")
            executor_memory = spark_cfg.get("executor_memory")
            driver_cores = spark_cfg.get("driver_cores")
            executor_cores = spark_cfg.get("executor_cores")
            num_executors = spark_cfg.get("num_executors")
            total_executor_cores = spark_cfg.get("total_executor_cores")
    elif cluster_ui == "YEEDU" and cluster_hive_enable not in (None, "None"):
        hive_b64 = input_args_json.get("b64_hive_creds_json")
        if hive_b64:
            principal, keytab = getHiveCredentials(hive_b64, NABU_SPARK_BOT_HOME)

    conf_list = [
        "spark.ui.enabled=false",
        "spark.ui.retainedTasks=100",
        "spark.driver.maxResultSize=1G",
        "spark.sql.crossJoin.enabled=true",
        "spark.ui.retainedStages=100",
        "spark.ui.retainedJobs=100",
        "spark.yarn.maxAppAttempts=1",
        "spark.executor.heartbeatInterval=60s",
        "spark.network.timeout=500s",
        "spark.sql.parquet.page.size.row.check.min=20",
        "spark.hadoop.fs.s3a.fast.upload=true",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
        "spark.dynamicAllocation.enabled=true",
        "spark.dynamicAllocation.shuffleTracking.enabled=true",
        "spark.dynamicAllocation.minExecutors=1",
        "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled=true",
        "spark.driver.nabu_privateKey=/yeedu/object-storage-manager/privateKey",
        "spark.driver.extraClassPath=/yeedu/object-storage-manager/",
        f"spark.driver.nabu_fireshots_url={NABU_FIRESHOTS_URL}",
    ]

    conf_list.extend(key_value_pairs)

    # Add resource configs if available
    if driver_memory:
        conf_list.append(f"spark.driver.memory={driver_memory}")
    if executor_memory:
        conf_list.append(f"spark.executor.memory={executor_memory}")
    if driver_cores:
        conf_list.append(f"spark.driver.cores={driver_cores}")
    if executor_cores:
        conf_list.append(f"spark.executor.cores={executor_cores}")
    if num_executors:
        conf_list.append(f"spark.executor.instances={num_executors}")

    # Add Iceberg or Delta configurations
    if isDelta:
        conf_list.append("spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension")
        conf_list.append("spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")
    if isIceberg:
        conf_list.append("spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        conf_list.append("spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog")

    # Add Hive credentials
    if principal and keytab:
        conf_list.append(f"spark.hadoop.hive.metastore.kerberos.principal={principal}")
        conf_list.append(f"spark.hadoop.hive.metastore.kerberos.keytab.file={keytab}")


    files_list = ["/yeedu/object-storage-manager/application.conf"]
    if keytab:
        files_list.append(keytab)

    timestamp = str(int(datetime.datetime.now().timestamp()))

    if cluster_ui in ("STANDALONE", "CLUSTER"):
        driver_java_options = (
            f"-Dderby.system.home=/yeedu/spark_metastores/{timestamp}-{batch_id} "
            f"-Djava.library.path=/usr/local/lib/python{python_version}/dist-packages/jep/"
        )
    else:
        driver_java_options = f"-Djava.library.path=/usr/local/lib/python{python_version}/dist-packages/jep/"

    logging.info("jars_list: ",jars_list)

    job_data_dict = {
        "name": f"spark_curation_job_v18_{batch_id}_{retry_num}_{process_id}",
        "cluster_id": cluster_id,
        "max_concurrency": 100,
        "job_class_name": "com.modak.BootstrapCuration",
        "job_command": f"file:///yeedu/object-storage-manager/{NABU_SPARK_BOT_REFLECTION_3_2_JAR}",
        "job_arguments": b64_input_json,
        "job_rawScalaCode": None,
        "job_type": "Jar",
        "job_timeout_min": None,
        "files": files_list,
        "properties_file": [],
        "conf": conf_list,
        "packages": [],
        "repositories": [],
        "jars": jars_list,
        "archives": [],
        "driver_memory": driver_memory,
        "driver_java_options": driver_java_options,
        "driver_library_path": None,
        "driver_class_path": "/yeedu/object-storage-manager/",
        "executor_memory": executor_memory,
        "driver_cores": driver_cores,
        "total_executor_cores": total_executor_cores,
        "executor_cores": executor_cores,
        "num_executors": num_executors,
        "principal": principal,
        "keytab": keytab,
        "queue": None,
        "should_append_params": False
    }

    logging.info(job_data_dict)
    logging.info(workspace_id)
    resp = api_call_with_retry(jobs_client.create_job, job_data_dict, workspace_id)
    return resp.get("job_id")


def monitor_job(jobs_client):
    is_inserted = False
    application_id = None
    insert_cmd = F"python3 {NABU_SPARK_BOT_HOME}/bin/insertApplicationDetails.py {NABU_SPARK_BOT_HOME} {process_id} {batch_id} {retry_num} 'nabu-sparkbot-curation-{process_id}-{retry_num}' {job_id} NULL NULL 'nabu-sparkbot-curation' NULL NULL NULL {base64_encoded_json}"
    logging.info(insert_cmd)
    execute_cmd(insert_cmd)

    try:
        while True:
            # Step 1: Get current job run details
            run_details = api_call_with_retry(jobs_client.get_job_run_details, run_id, workspace_id)
            job_status = run_details.get("run_status")
            logging.info(f"Current run status for Spark Job Instance ID {run_id}: {job_status}")

            # Step 2: Check if job is RUNNING and app_id is still not inserted
            if job_status == "RUNNING" and not is_inserted:
                application_id = run_details.get("application_id")

                # Step 3: Wait for application_id to become available
                while not application_id:
                    logging.info("Waiting for application_id...")
                    time.sleep(10)

                    run_details = api_call_with_retry(jobs_client.get_job_run_details, run_id, workspace_id)
                    job_status = run_details.get("run_status")
                    application_id = run_details.get("application_id")

                    if job_status in ["ERROR", "STOPPED", "TERMINATED", "FAILED"]:
                        logging.error("Job ended before application_id became available.")
                        sys.exit(1)

                update_cmd = f"python3 {NABU_SPARK_BOT_HOME}/bin/updateApplicationDetails.py {NABU_SPARK_BOT_HOME} {process_id} {batch_id} {retry_num} '{application_id}'"
                logging.info(update_cmd)
                execute_cmd(update_cmd)
                logging.info(f"application_id is: {application_id}")
                is_inserted = True

            if job_status in ["DONE", "ERROR", "STOPPED", "FAILED", "TERMINATED"]:
                logging.info(f"Job reached final state: {job_status}")
                if job_status == "DONE":
                    sys.exit(0)
                else:
                    sys.exit(1)

            time.sleep(5)

    except Exception as e:
        logging.error(f"wait_for_application_id failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    logging_bootstrap()
    NABU_SPARK_BOT_HOME = sys.argv[1]
    b64_input_args_json = sys.argv[2]
    b64_file_format_json = sys.argv[3]
    bytes_string = base64.b64decode(b64_input_args_json)
    decoded_input_args_json = bytes_string.decode("utf-8")
    input_args_json = json.loads(decoded_input_args_json)
    cluster_name = input_args_json['cluster_name']
    workspace_name = input_args_json['workspace_name']
    b64_input_json = input_args_json['b64_input_json']
    input_json_bytes = base64.b64decode(b64_input_json)
    decoded_input_json = input_json_bytes.decode("utf-8")
    input_json = json.loads(decoded_input_json)
    process_id = input_args_json['process_id']
    batch_id = input_args_json['batch_id']
    logging.info(F"[+] The process id for the job is {process_id}")
    logging.info(F"[+] The batch id for the job is {batch_id}")
    BASE_URL = input_args_json['rest_url']
    b64_extra_config = input_args_json['extra_config']
    input_extra_config_bytes = base64.b64decode(b64_extra_config)
    decoded_extra_config_json = input_extra_config_bytes.decode("utf-8")
    extra_conf_json = json.loads(decoded_extra_config_json)
    cluster_ui = input_args_json['cluster_ui']
    retry_num = input_args_json['retry_num']
    b64_compute_eng_creds_json = input_args_json['compute_eng_creds_json']
    logging.info("b64_compute_eng_creds_json:", b64_compute_eng_creds_json)
    b64_external_jars_map = input_args_json['b64_external_jars_map']
    key_value_pairs = extract_key_value_pairs(extra_conf_json)
    logging.info(key_value_pairs)
    NABU_FIRESHOTS_URL = get_var('NABU_FIRESHOTS_URL', NABU_SPARK_BOT_HOME)
    NABU_SPARK_BOT_REFLECTION_3_2_JAR = get_var('NABU_SPARK_BOT_REFLECTION_3_2_JAR', NABU_SPARK_BOT_HOME)
    TENANT_ID, TOKEN = getCredentials(b64_compute_eng_creds_json, NABU_SPARK_BOT_HOME)

    user_mgr = api_call_with_retry(UserManager, BASE_URL, TOKEN)
    osm = api_call_with_retry(ObjectStorageManager, BASE_URL, TOKEN)
    jobs = api_call_with_retry(JobsManager, BASE_URL, TOKEN)
    cluster_mgr = api_call_with_retry(ClusterManager, BASE_URL, TOKEN)
    workspace_mgr = api_call_with_retry(WorkspaceManager, BASE_URL, TOKEN)

    associate_resp = api_call_with_retry(user_mgr.associate_tenant, tenant_id=TENANT_ID)
    logging.info("Tenant associated:", associate_resp)

    workspace_resp = api_call_with_retry(workspace_mgr.get_workspace, workspace_name=workspace_name)
    workspace_id = workspace_resp.get("workspace_id") if workspace_resp else None
    logging.info("Workspace ID:", workspace_id)

    cluster_resp = api_call_with_retry(cluster_mgr.get_cluster, cluster_name=cluster_name)
    cluster_id = cluster_resp.get("cluster_id")
    cluster_type = cluster_resp.get("cluster_type")
    cluster_ui = cluster_resp.get("cluster_ui")
    cluster_conf = cluster_resp.get("cluster_conf", {})
    cluster_hive_enable = cluster_conf.get("enable_hive")
    hadoop_version = cluster_resp.get("spark_infra_version", {}).get("hadoop_version")
    python_version = cluster_resp.get("spark_infra_version", {}).get("python_version")
    object_storage_manager_name = cluster_resp.get("object_storage_manager", {}).get("object_storage_manager_name")

    additional_info = {
        "compute_engine": "yeedu",
        "yeedu_tenant_id": TENANT_ID,
        "yeedu_workspace_id": workspace_id
    }
    additional_info_json = json.dumps(additional_info)
    base64_encoded_json = base64.b64encode(additional_info_json.encode()).decode()

    logging.info("Cluster Info:", cluster_id, cluster_type, cluster_ui, cluster_hive_enable, hadoop_version, python_version, object_storage_manager_name)

    upload_temp_resources(osm, osm_name=object_storage_manager_name, NABU_SPARK_BOT_HOME=NABU_SPARK_BOT_HOME)
    reupload_selected_jars(osm, osm_name=object_storage_manager_name)

    job_id = int(create_job(jobs))

    logging.info(f"Job submitted with ID: {job_id}")

    run_data_dict = {
        "job_id": job_id
    }

    logging.info(run_data_dict)

    run = api_call_with_retry(jobs.run_job, run_data_dict, workspace_id)
    logging.info(f"Run started: {run}")

    run_id = int(run.get("run_id"))
    logging.info("run_id: ",run_id)

    monitor_job(jobs)

    logging.info("Job execution finished.")

