import sys
import psycopg2
from jproperties import Properties
import logging


def logging_bootstrap():
    logging.basicConfig(format='[%(asctime)s] \t %(levelname)s:%(message)s', level=logging.DEBUG)

def insert_job_details():
    configs = Properties()
    with open(F'{NABU_SPARK_BOT_HOME}/../common-lib/src/main/resources/nabu_common.properties', 'rb') as config_file:
        configs.load(config_file)
    conn = psycopg2.connect(database=configs.get("NABU_KOSH_SERVICE_DATABASE").data, user=configs.get("NABU_KOSH_SERVICE_ACCOUNT").data, password=configs.get("NABU_KOSH_SERVICE_ACCOUNT_PASS").data, host=configs.get("NABU_KOSH_SERVICE_ENDPOINT").data, port= configs.get("NABU_KOSH_SERVICE_PORT").data)
    cursor = conn.cursor()
    sql="UPDATE nabu.spark_application_details set application_id = '{}', mod_ts = now() where process_id = {} and batch_id = {} and retry_num = {};".format(application_id, process_id ,batch_id,retry_num)
    cursor.execute(sql)
    conn.commit()
    logging.info("[+] Updated Application_id in job details into kosh")
    conn.close()

# main classs
if __name__ == "__main__":
    logging_bootstrap()
    NABU_SPARK_BOT_HOME = sys.argv[1]
    process_id = sys.argv[2]
    batch_id = sys.argv[3]
    retry_num = sys.argv[4]
    application_id = sys.argv[5]
    insert_job_details()

