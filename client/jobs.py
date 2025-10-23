from .base import YeeduClient

class JobsManager(YeeduClient):

    def create_job(self, job_data: dict,workspace_id: int):
        return self._request("POST", f"/workspace/{workspace_id}/spark/job", json=job_data)

    def run_job(self, run_data: dict,workspace_id: int):
        return self._request("POST", f"/workspace/{workspace_id}/spark/job/run", json=run_data)

    def get_job_status(self, run_id: int, workspace_id: int):
        return self._request("GET", f"/workspace/{workspace_id}/spark/job/run/{run_id}/status")

    def get_job_run_details(self, run_id: int, workspace_id: int):
        return self._request("GET", f"/workspace/{workspace_id}/spark/job/run/{run_id}")

