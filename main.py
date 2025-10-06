import json

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


# ✅ Upload a file
    print("Uploading file...")
    resp = osm.upload_file("/home/rv1701/nabu_lite/yeedu_api/yeedu_client/spark-examples_2.12-3.5.0.jar", osm_name="aws_nabu_osm", overwrite=True)

    print(resp)

    # ✅ List files
    files = osm.list_files(osm_name="aws_nabu_osm")
    print(files)

    # ✅ Run a job

    job_data1 = """{
  "name": "spark_job_examples_v1",
  "cluster_id": 188,
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
    # job = jobs.create_job(job_data1)
    # print("Created Job:", job)
    # job_data_dict = json.loads(job_data1)

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

    #run = jobs.run_job(job["id"])
    #run = jobs.run_job(job["job_id"])
    #run_data_dict = json.loads(run_data)
    run = jobs.run_job(run_data_dict)
    print("Run started:", run)