from .base import YeeduClient

class JobsManager(YeeduClient):

    def create_job(self, job_data: dict):
        return self._request("POST", "/jobs/create", json=job_data)

    def run_job(self, job_id: int):
        return self._request("POST", f"/jobs/{job_id}/run")

    def get_job_status(self, job_id: int):
        return self._request("GET", f"/jobs/{job_id}/status")

    def kill_job(self, job_id: int):
        return self._request("POST", f"/jobs/{job_id}/kill")