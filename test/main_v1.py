
import json
import os
import time
import logging

from client.object_storage import ObjectStorageManager
from client.jobs import JobsManager
from client.user import UserManager

BASE_URL = "https://dev-onprem-004.yeedu.io:8080/api/v1"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoxOSwic2FsdCI6IjUwMWE4MDliLTdlNDgtNDE5NS1iNDFjLTU5OTMzNjQ2YTAxNCIsImlhdCI6MTc1OTcyOTA1NywiZXhwIjoxNzU5OTAxODU3fQ.IJLuG23oSCRSOUDh6jpreU941XMhkDMySC5ej1J6WsM"

if __name__ == "__main__":
    # ✅ Associate the current user with a tenant
    user_mgr = UserManager(BASE_URL, TOKEN)
    associate_resp = user_mgr.associate_tenant(tenant_id="4c1fc7a8-d153-40bd-a84c-8d9983edd31b")
    print("Tenant associated:", associate_resp)

    osm = ObjectStorageManager(BASE_URL, TOKEN)
    jobs = JobsManager(BASE_URL, TOKEN)

    def reupload_selected_jars(object_storage_mgr, osm_name):
        expected_jars = [
            "file:///yeedu/object-storage-manager/home/rv1701/nabu_lite/yeedu_api/yeedu_client/mongo-spark-connector_2.12-10.4.1.jar",
            "file:///yeedu/object-storage-manager/home/rv1701/nabu_lite/yeedu_api/yeedu_client/spark-examples_2.12-3.5.0.jar",
            "file:///yeedu/object-storage-manager/home/rv1701/nabu_lite/yeedu_api/yeedu_client/postgresql-42.7.3.jar"
        ]

        local_jar_map = {
            "mongo-spark-connector_2.12-10.4.1.jar": "/home/rv1701/nabu_lite/yeedu_api/yeedu_client/mongo-spark-connector_2.12-10.4.1.jar",
            "postgresql-42.7.3.jar": "/home/rv1701/nabu_lite/yeedu_api/yeedu_client/postgresql-42.7.3.jar",
            "spark-examples_2.12-3.5.0.jar": "/home/rv1701/nabu_lite/yeedu_api/yeedu_client/spark-examples_2.12-3.5.0.jar"
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

    # ✅ Upload a local file (e.g., Metadata or any file)
    print("Uploading local file to object storage...")

    file_to_upload = "/home/rv1701/nabu_lite/yeedu_api/yeedu_client/spark-examples_2.12-3.5.0.jar"

    if os.path.exists(file_to_upload) and os.path.isfile(file_to_upload):
        resp = osm.upload_file(
            file_path=file_to_upload,
            osm_name="aws_nabu_osm",
            overwrite=False,   # Change to True if needed
            target_dir="//"
        )
        print("Upload response:", resp)
    else:
        print(f"File not found or not a regular file: {file_to_upload}")

    # ✅ List files in object storage
    files = osm.list_files(osm_name="aws_nabu_osm")
    print("################################################################")
    print(files)

    # ✅ Define job
    job_data1 = """{
      "name": "spark_job_examples_v9",
      "cluster_id": 189,
      "max_concurrency": 100,
      "job_class_name": "org.example.Main",
      "job_command": "file:///yeedu/object-storage-manager/home/rv1701/nabu_lite/yeedu_api/yeedu_client/spark-examples_2.12-3.5.0.jar",
      "job_arguments": "500",
      "job_rawScalaCode": null,
      "job_type": "Jar",
      "job_timeout_min": null,
      "files": [],
      "properties_file": [],
      "conf": [],
      "packages": [],
      "repositories": [],
      "jars": [],
      "archives": [],
      "driver_memory": null,
      "driver_java_options": null,
      "driver_library_path": null,
      "driver_class_path": null,
      "executor_memory": null,
      "driver_cores": null,
      "total_executor_cores": null,
      "executor_cores": null,
      "num_executors": null,
      "principal": null,
      "keytab": null,
      "queue": null,
      "should_append_params": false
    }"""

    # ✅ Create and run job
    job_data_dict = json.loads(job_data1)
    job = jobs.create_job(job_data_dict)
    print("Created Job:", job)

    job_id = int(job["job_id"])
    run_data_dict = {
        "job_id": job_id,
        "arguments": "1000",
        "conf": [
            "spark.executor.memory=4g"
        ]
    }

    run = jobs.run_job(run_data_dict)
    print("Run started:", run)

    run_id = int(run.get("run_id"))

    # ✅ Monitor job
    while True:
        status_list = jobs.get_job_status(run_id)
        print("Job status:", status_list)

        active_status = next((s for s in status_list if s.get("end_time") == "infinity"), None)

        if not active_status:
            print("No active job stage found — job completed.")
            break

        current_state = active_status.get("run_status")
        print("Current state:", current_state)

        if current_state in ("SUCCESS", "FAILED", "ERROR"):
            print("Job finished with state:", current_state)
            break

        time.sleep(5)
