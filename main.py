import json
import os
import logging
import time
import re

from client.workspace_manager import WorkspaceManager
from client.cluster_manager import ClusterManager
from client.object_storage import ObjectStorageManager
from client.jobs import JobsManager
from client.user import UserManager


BASE_URL = "https://url.yeedu.io:8080/api/v1"
TOKEN = ""

if __name__ == "__main__":
    # ✅ Associate the current user with a tenant
    user_mgr = UserManager(BASE_URL, TOKEN)
    associate_resp = user_mgr.associate_tenant(tenant_id="4c1fc7a8-d153-40bd-a84c-8d9983edd31b")
    print("Tenant associated:", associate_resp)

    osm = ObjectStorageManager(BASE_URL, TOKEN)
    jobs = JobsManager(BASE_URL, TOKEN)
    cluster_mgr = ClusterManager(BASE_URL, TOKEN)

    workspace_mgr = WorkspaceManager(BASE_URL, TOKEN)
    workspace_resp = workspace_mgr.get_workspace(workspace_name="nabu_v291_workspace")

    print("Workspace response:", workspace_resp)

    response = workspace_mgr.upload_file(
        file_path="/home/rv1701/nabu_lite/yeedu_api/yeedu_client/requirements.txt",
        workspace_name="nabu_v291_workspace",
        overwrite=True
    )

    print("Upload Response:", response)


    workspace_id = workspace_resp.get("workspace_id") if workspace_resp else None
    print("Workspace ID:", workspace_id)

    cluster_resp = cluster_mgr.get_cluster(cluster_name="nabu_spark_333")
    print("Cluster Response:", cluster_resp)

    # Extract fields safely
    if "cluster_id" in cluster_resp:
        cluster_id = cluster_resp["cluster_id"]
    else:
        cluster_id = None


    cluster_conf_id = None
    if "cluster_conf" in cluster_resp:
        cluster_conf_id = cluster_resp["cluster_conf"].get("cluster_conf_id")

    hive_metastore_conf_id = None
    if "cluster_conf" in cluster_resp:
        # This depends if the API returns hive_metastore_conf_id in cluster_conf — adjust as needed
        hive_metastore_conf_id = cluster_resp["cluster_conf"].get("hive_metastore_conf_id")


    cluster_type = cluster_resp.get("cluster_type")
    hadoop_version = None
    python_version = None

    if "spark_infra_version" in cluster_resp:
        hadoop_version = cluster_resp["spark_infra_version"].get("hadoop_version")
        python_version = cluster_resp["spark_infra_version"].get("python_version")


    print("cluster_id:", cluster_id)
    print("cluster_conf_id:", cluster_conf_id)
    print("hive_metastore_conf_id:", hive_metastore_conf_id)
    print("cluster_type:", cluster_type)
    print("hadoop_version:", hadoop_version)
    print("python_version:", python_version)


    def reupload_selected_jars(object_storage_mgr, osm_name):
        expected_jars = [
            "file:///yeedu/object-storage-manager/mongo-spark-connector_2.12-10.5.1.jar",
            "file:///yeedu/object-storage-manager/spark-examples_2.12-3.5.0.jar",
            "file:///yeedu/object-storage-manager/postgresql-42.7.3.jar",
            "file:///yeedu/object-storage-manager/TestSBTProject-assembly-0.1.0-SNAPSHOT.jar"
        ]

        local_jar_map = {
            "mongo-spark-connector_2.12-10.5.1.jar": "/home/rv1701/nabu_lite/yeedu_api/yeedu_client/mongo-spark-connector_2.12-10.5.1.jar",
            "postgresql-42.7.3.jar": "/home/rv1701/nabu_lite/yeedu_api/yeedu_client/postgresql-42.7.3.jar",
            "spark-examples_2.12-3.5.0.jar": "/home/rv1701/nabu_lite/yeedu_api/yeedu_client/spark-examples_2.12-3.5.0.jar",
            "TestSBTProject-assembly-0.1.0-SNAPSHOT.jar" : "/home/rv1701/nabu_lite/yeedu_api/yeedu_client/TestSBTProject-assembly-0.1.0-SNAPSHOT.jar"
        }

        try:
            existing_files_resp = object_storage_mgr.list_files(osm_name=osm_name)
            existing_paths = [item["full_file_path"] for item in existing_files_resp.get("data", [])]
            print("Existing files in object storage:")
            print(existing_paths)
        except Exception as e:
            logging.error(f"Failed to fetch file list from OSM: {e}")
            return

        for jar_uri in expected_jars:
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

    job_data_dict = {
        "name": "spark_job_examples_v20",
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

    job = jobs.create_job(job_data_dict,workspace_id)
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
    run = jobs.run_job(run_data_dict,workspace_id)
    print("Run started:", run)

    run_id = int(run.get("run_id"))

    # ✅ Monitor job

    while True:
        status_list = jobs.get_job_status(run_id,workspace_id)
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



