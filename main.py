from client.object_storage import ObjectStorageManager
from client.jobs import JobsManager
from client.user import UserManager


BASE_URL = "https://dev-onprem-004.yeedu.io:8080/api"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoxOSwic2FsdCI6Ijk1MTJkZjg4LTY4YzgtNGNjMC1hYzBjLWQ5NDkyZjlhNzU1ZiIsImlhdCI6MTc1OTMyNjMxMiwiZXhwIjoxNzU5NDk5MTEyfQ.xI4W6YajitDvk3Vom22cc46PVwc5tgLTsYEOgWqr7VA"

if __name__ == "__main__":
    osm = ObjectStorageManager(BASE_URL, TOKEN)
    jobs = JobsManager(BASE_URL, TOKEN)

# ✅ Associate the current user with a tenant
    user_mgr = UserManager(BASE_URL, TOKEN)
    associate_resp = user_mgr.associate_tenant(tenant_id="4c1fc7a8-d153-40bd-a84c-8d9983edd31b")
    print("Tenant associated:", associate_resp)


# ✅ Upload a file
    print("Uploading file...")
    resp = osm.upload_file("spark-examples_2.12-3.5.0.jar", osm_name="aws_health_transformation_osm", overwrite=True)
    print(resp)

    # ✅ List files
    files = osm.list_files(osm_name="aws_health_transformation_osm")
    print(files)

    # ✅ Run a job
    job_data = {
        "name": "my-spark-job",
        "task": {
            "spark_jar_task": {
                "main_class_name": "org.example.Main"
            }
        }
    }
    job = jobs.create_job(job_data)
    print("Created Job:", job)

    run = jobs.run_job(job["id"])
    print("Run started:", run)