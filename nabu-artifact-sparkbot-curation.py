import base64
import datetime
import os
import re
import subprocess
import sys
import json
import logging
import time
from Crypto.Cipher import AES
import requests
import string
import psycopg2
from jproperties import Properties
from client.workspace_manager import WorkspaceManager

from client.cluster_manager import ClusterManager
from client.object_storage import ObjectStorageManager
from client.jobs import JobsManager
from client.user import UserManager

NABU_SPARK_BOT_REFLECTION_3_2_JAR = "sample"
NABU_SPARK_BOT_HOME = "sample2"
BASE_URL = "https://dev-onprem-004.yeedu.io:8080/api/v1"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoxOSwic2FsdCI6IjUwMWE4MDliLTdlNDgtNDE5NS1iNDFjLTU5OTMzNjQ2YTAxNCIsImlhdCI6MTc1OTcyOTA1NywiZXhwIjoxNzU5OTAxODU3fQ.IJLuG23oSCRSOUDh6jpreU941XMhkDMySC5ej1J6WsM"

if __name__ == "__main__":
    # ✅ Associate the current user with a tenant
    user_mgr = UserManager(BASE_URL, TOKEN)
    associate_resp = user_mgr.associate_tenant(tenant_id="4c1fc7a8-d153-40bd-a84c-8d9983edd31b")
    print("Tenant associated:", associate_resp)

    osm = ObjectStorageManager(BASE_URL, TOKEN)
    jobs = JobsManager(BASE_URL, TOKEN)
    cluster_mgr = ClusterManager(BASE_URL, TOKEN)
    workspace_mgr = WorkspaceManager(BASE_URL, TOKEN)

    cluster_resp = cluster_mgr.get_cluster(cluster_name="nabu_spark_333")
    cluster_id = cluster_resp.get("cluster_id")
    cluster_conf_id = cluster_resp.get("cluster_conf", {}).get("cluster_conf_id")
    cluster_type = cluster_resp.get("cluster_type", {})
    hive_metastore_conf_id = cluster_resp.get("cluster_conf", {}).get("hive_metastore_conf_id")
    hadoop_version = cluster_resp.get("spark_infra_version", {}).get("hadoop_version")
    python_version = cluster_resp.get("spark_infra_version", {}).get("python_version")

    workspace_resp = workspace_mgr.get_workspace(workspace_name="sample")
    print("Workspace response:", workspace_resp)

    workspace_id = workspace_resp.get("workspace_id") if workspace_resp else None
    print("Workspace ID:", workspace_id)

    print("Cluster Info:", cluster_id, cluster_conf_id, hive_metastore_conf_id, hadoop_version, python_version, cluster_type)


    def get_var(varname, NABU_SPARK_BOT_HOME):
        get_var_cmd = F'echo $(source {NABU_SPARK_BOT_HOME}/yeedu/conf/small/nabu_spark_conf.sh; echo $%s)' % varname
        result = subprocess.Popen(get_var_cmd, stdout=subprocess.PIPE, shell=True, executable='/bin/bash',
                                  universal_newlines=True)
        return result.stdout.readlines()[0].strip()


    def get_property_value(property_name, NABU_SPARK_BOT_HOME):
        configs = Properties()
        # Load configuration from nabu_common.properties file
        with open(F'{NABU_SPARK_BOT_HOME}/../common-lib/src/main/resources/nabu_common.properties',
                  'rb') as config_file:
            configs.load(config_file)
            value = configs.get(property_name).data

        return value


    def extract_key_value_pairs(extra_conf_json):
        config_map = extra_conf_json.get("extraConfigMap", {})
        entries = config_map.items()
        return [f"--conf={key}={value}" for key, value in entries]


    def AESDecription(AESString, NABU_SPARK_BOT_HOME):
        try:
            encryptedData = bytes.fromhex(AESString)
            cipher = AES.new(open(F"{NABU_SPARK_BOT_HOME}/keys/aesKey", 'rb').read(), AES.MODE_ECB)
            decrypted = cipher.decrypt(encryptedData)
            return decrypted.decode('utf-8')
        except Exception as e:
            logging.error(F"[+] Exception:  {e}")
            raise Exception(e)


    def decode_credentials(b64_encoded_json, NABU_SPARK_BOT_HOME):
        try:
            bytes_string = base64.b64decode(b64_encoded_json)
            decode_input_json = bytes_string.decode("utf-8")
            decoded_string = json.loads(decode_input_json)
            credential_id = decoded_string['credential_id']
            credential_type_id = decoded_string['credential_type_id']
            AES_enc_token = decoded_string['token']
            credential_endpoint_url = decoded_string['credential_endpoint_url']
            raw_token = AESDecription(AES_enc_token, NABU_SPARK_BOT_HOME)
            token = ''.join(filter(lambda x: x in string.printable, raw_token)).strip()
            if credential_id.strip() == "" or credential_type_id.strip() == "":
                logging.error(
                    F"[+] Exception:  Credential ID and Credential Type ID are required to fetch file details")
                raise ValueError("Insufficient details to fetch credentials")
            return credential_id, credential_type_id, token, credential_endpoint_url
        except (ValueError, Exception) as e:
            logging.error(F"[+] Exception:  Error has occurred: {e}")
            sys.exit(1)


    def decode_hive_credentials(b64_encoded_json):
        # Decode the base64 encoded string
        bytes_string = base64.b64decode(b64_encoded_json)
        decode_input_json = bytes_string.decode("utf-8")
        decoded_string = json.loads(decode_input_json)
        try:
            credential_id = decoded_string.get('credential_id', '').strip()
            credential_type_id = decoded_string.get('credential_type_id', '').strip()
        except KeyError:
            logging.error(f"unable to get the hive credential id and credntial type id")

        return credential_id, credential_type_id


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
            response = requests.request("POST", credential_endpoint_url, headers=headers, data=payload)
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
            tenant_id = credential_details['data']['tenant_id']

            return tenant_id
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
                    raise Exception(
                        f"Failed to fetch credentials, status code: {response.status_code}, response: {data}")
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
                print(f"Uploading temporary resource: {os.path.basename(resource_path)}")
                response = object_storage_mgr.upload_file(
                    file_path=resource_path,
                    osm_name=osm_name,
                    overwrite=True
                )
                logging.info(f"[+] Uploaded temporary resource {resource_path}: {response}")
            except Exception as e:
                logging.error(f"Failed to upload {resource_path}: {e}")


    upload_temp_resources(osm, osm_name="aws_nabu_osm", NABU_SPARK_BOT_HOME=NABU_SPARK_BOT_HOME)

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
            existing_files_resp = object_storage_mgr.list_files(osm_name=osm_name)
            existing_paths = [item["full_file_path"] for item in existing_files_resp.get("data", [])]
            print("Existing files in object storage:")
            print(existing_paths)
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
                    print(f"Uploading missing jar: {jar_name}")
                    response = object_storage_mgr.upload_file(
                        file_path=local_path,
                        osm_name=osm_name,
                        overwrite=True
                    )
                    print(f"Uploaded {jar_name}: {response}")
                except Exception as e:
                    logging.error(f"Failed to upload {jar_name}: {e}")
            else:
                print(f"{jar_name} already exists in object storage, skipping.")

# ✅ Upload a file
    print("Uploading file...")
    reupload_selected_jars(osm, osm_name="aws_nabu_osm")


    # ✅ List files
    files = osm.list_files(osm_name="aws_nabu_osm")
    print("################################################################")
    print(files)

    # ✅ Run a job

    def create_job_new_style(cluster_id, process_id, batch_id, b64_input_json, cluster_type, cluster_hive_enable,
                             extra_args,
                             cluster_ui, retry_num, input_args_json, NABU_FIRESHOTS_URL,
                             NABU_SPARK_BOT_REFLECTION_3_2_JAR,
                             NABU_SPARK_BOT_HOME, b64_external_jars_map, b64_file_format_json, workspace_id,
                             python_version,
                             hadoop_version, decode_hive_credentials, getHiveCredentials, jobs):

        logging.info(f"[+] Creating the Spark Job for {batch_id} {process_id}")
        time.sleep(10)

        # Decode external jars map
        external_jars_map_bytes_string = base64.b64decode(b64_external_jars_map)
        decode_json = external_jars_map_bytes_string.decode("utf-8")
        parsed_data = json.loads(decode_json)

        external_jars = parsed_data.get("external_jars", False)
        if external_jars:
            external_jars_list = parsed_data.get("external_jars_list", [])
            jars_list = [f"/yeedu/object-storage-manager/{jar}" for jar in
                         external_jars_list] if external_jars_list else []
        else:
            jars_list = []

        # Add the big list of required jars (same as old code)
        common_jars = [
            "/yeedu/object-storage-manager/delta-storage-2.0.0.jar",
            "/yeedu/object-storage-manager/azure-storage-8.6.6.jar",
            "/yeedu/object-storage-manager/jetty-util-9.4.43.v20210629.jar",
            "/yeedu/object-storage-manager/jetty-util-ajax-11.0.25.jar",
            "/yeedu/object-storage-manager/commons-pool2-2.11.1.jar",
            "/yeedu/object-storage-manager/glow-spark3_2.12-1.2.1.jar",
            "/yeedu/object-storage-manager/jdbi-2.63.1.jar",
            "/yeedu/object-storage-manager/htsjdk-3.0.1.jar",
            "/yeedu/object-storage-manager/hadoop-bam-7.10.0.jar",
            "/yeedu/object-storage-manager/wildfly-openssl-1.0.7.Final.jar",
            "/yeedu/object-storage-manager/azure-data-lake-store-sdk-2.3.8.jar",
            "/yeedu/object-storage-manager/delta-core_2.12-2.0.0.jar",
            "/yeedu/object-storage-manager/mongo-spark-connector_2.12-10.4.1.jar",
            "/yeedu/object-storage-manager/gcs-connector-hadoop3-latest.jar",
            "/yeedu/object-storage-manager/jcc-11.5.9.0.jar",
            "/yeedu/object-storage-manager/terajdbc-20.00.00.10.jar",
            f"/yeedu/object-storage-manager/hadoop-aws-{hadoop_version}.jar",
            "/yeedu/object-storage-manager/postgresql-42.7.3.jar",
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
            "/yeedu/object-storage-manager/spark-hadoop-cloud_2.12-3.5.1.jar",
            f"/yeedu/object-storage-manager/hadoop-azure-{hadoop_version}.jar",
            "/yeedu/object-storage-manager/spark-avro_2.12-3.2.4.jar",
            "/yeedu/object-storage-manager/kafka-clients-3.7.1.jar",
            "/yeedu/object-storage-manager/spark-sql-kafka-0-10_2.12-3.2.4.jar",
            "/yeedu/object-storage-manager/spark-token-provider-kafka-0-10_2.12-3.2.4.jar",
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
            "/yeedu/object-storage-manager/azure-eventhubs-spark_2.12-2.3.22.jar",
            "/yeedu/object-storage-manager/azure-eventhubs-3.3.0.jar",
            "/yeedu/object-storage-manager/scala-java8-compat_3-1.0.2.jar",
            "/yeedu/object-storage-manager/proton-j-0.34.1.jar",
            "/yeedu/object-storage-manager/iceberg-spark-runtime_3_2.jar"
        ]

        # Combine all jars
        jars_list.extend(common_jars)

        # Get spark config if cluster_ui is STANDALONE or CLUSTER
        total_executor_cores = ''
        driver_core = ''
        driver_memory = ''
        executor_memory = ''
        executor_cores = ''
        num_executors = ''
        principal = None
        keytab = None

        if cluster_ui in ['STANDALONE', 'CLUSTER']:
            b64_spark_configs = input_args_json.get('spark_configs')
            if b64_spark_configs:
                spark_config_bytes = base64.b64decode(b64_spark_configs)
                decoded_spark_config = spark_config_bytes.decode("utf-8")
                spark_config = json.loads(decoded_spark_config)
                total_executor_cores = spark_config.get('total_executor_cores')
                driver_core = spark_config.get('driver_cores')
                driver_memory = spark_config.get('driver_memory')
                executor_memory = spark_config.get('executor_memory')
                executor_cores = spark_config.get('executor_cores')
                num_executors = spark_config.get('num_executors')

        elif cluster_ui == "YEEDU" and cluster_hive_enable != "None":
            b64_hivecreds_json = input_args_json.get('b64_hive_creds_json')
            if b64_hivecreds_json:
                credential_id, credential_type_id = decode_hive_credentials(b64_hivecreds_json)
                if credential_id and credential_type_id:
                    principal, keytab = getHiveCredentials(b64_hivecreds_json, NABU_SPARK_BOT_HOME)
                else:
                    logging.warning(
                        "credential_id or credential_type_id is null provide the hive credentials if needed.")

        # File format json decoding for formatRelatedConfigs
        file_format_json_bytes_string = base64.b64decode(b64_file_format_json)
        decoded_file_format_json = file_format_json_bytes_string.decode("utf-8")
        file_format_json = json.loads(decoded_file_format_json)

        isIcebergeEnabled = file_format_json.get('isIcebergeEnabled', False)
        isDeltaEnabled = file_format_json.get('isDeltaEnabled', False)

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
            f"spark.driver.nabu_privateKey=/yeedu/object-storage-manager/privateKey",
            f"spark.driver.extraClassPath=/yeedu/object-storage-manager/",
            f"spark.driver.nabu_fireshots_url={NABU_FIRESHOTS_URL}"
        ]

        # Add format-related configs
        if isIcebergeEnabled:
            conf_list.append("spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            conf_list.append("spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog")
        elif isDeltaEnabled:
            conf_list.append("spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension")
            conf_list.append("spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Driver java options (include library path like old)
        driver_java_options = f"-Djava.library.path=/usr/local/lib/python{python_version}/dist-packages/jep/"

        # Generate timestamp for derby system home dir
        timestamp = str(int(datetime.datetime.now().timestamp()))

        # Files list (application.conf always included)
        files_list = ["/yeedu/object-storage-manager/application.conf"]

        # Append keytab file to files if hive is enabled
        if cluster_ui == "YEEDU" and cluster_hive_enable != "None" and keytab:
            files_list.append(keytab)

        # Job arguments - base64 input json from old code
        job_arguments = b64_input_json

        # Construct job data dictionary for new style
        job_data_dict = {
            "name": f"spark_curation_job_{batch_id}_{retry_num}_{process_id}",
            "cluster_id": cluster_id,
            "max_concurrency": 100,
            "job_class_name": "com.modak.BootstrapCuration",
            "job_command": f"file:///yeedu/object-storage-manager/{NABU_SPARK_BOT_REFLECTION_3_2_JAR}",
            "job_arguments": job_arguments,
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
            "driver_library_path": f"/usr/local/lib/python{python_version}/dist-packages/jep/",
            "driver_class_path": "/yeedu/object-storage-manager/",
            "executor_memory": executor_memory,
            "driver_cores": driver_core,
            "total_executor_cores": total_executor_cores,
            "executor_cores": executor_cores,
            "num_executors": num_executors,
            "principal": principal,
            "keytab": keytab,
            "queue": None,
            "should_append_params": False
        }

        # Create job using new API
        job = jobs.create_job(job_data_dict)
        print("Created Job:", job)
        job_id = int(job.get("job_id"))

        return job_id


    job_data_dict = {
        "name": "spark_job_examples_v18",
        "cluster_id": cluster_id,
        "max_concurrency": 100,
        "job_class_name": "Main",
        "job_command": "file:///yeedu/object-storage-manager/TestSBTProject-assembly-0.1.0-SNAPSHOT.jar",
        "job_arguments": "500",
        "job_rawScalaCode": None,
        "job_type": "Jar",
        "job_timeout_min": None,
        "files": [],
        "properties_file": [],
        "conf": [],
        "packages": [],
        "repositories": [],
        "jars": [],
        "archives": [],
        "driver_memory": None,
        "driver_java_options": None,
        "driver_library_path": None,
        "driver_class_path": None,
        "executor_memory": None,
        "driver_cores": None,
        "total_executor_cores": None,
        "executor_cores": None,
        "num_executors": None,
        "principal": None,
        "keytab": None,
        "queue": None,
        "should_append_params": False
    }

    job = jobs.create_job(job_data_dict)
    print("Created Job:", job)
    job_id = int(job["job_id"])
    run_data_dict = {
        "job_id": job_id,
        "conf": [
            "spark.executor.memory=4g"
        ]
    }

    #run = jobs.run_job(job["id"])
    #run = jobs.run_job(job["job_id"])
    #run_data_dict = json.loads(run_data)
    run = jobs.run_job(run_data_dict)
    print("Run started:", run)

    run_id = int(run.get("run_id"))

    # ✅ Monitor job

    while True:
        status_list = jobs.get_job_status(run_id)
        print("Job status:", status_list)

        # Find the most recent active stage (end_time = infinity)
        active_status = next((s for s in status_list if s.get("end_time") == "infinity"), None)

        if not active_status:
            print("No active job stage found  job completed.")
            break

        current_state = active_status.get("run_status")
        print("Current state:", current_state)

        if current_state in ("DONE","SUCCESS", "FAILED", "ERROR"):
            print("Job finished with state:", current_state)
            break

        time.sleep(5)



