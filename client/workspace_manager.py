import os
import requests
from .base import YeeduClient


class WorkspaceManager(YeeduClient):
    def get_workspace(self, workspace_id: int = None, workspace_name: str = None):
        params = {}
        if workspace_id is not None:
            params["workspace_id"] = workspace_id
        if workspace_name is not None:
            params["workspace_name"] = workspace_name

        return self._request("GET", "/workspace", params=params)

    def upload_file(self, file_path, workspace_id=None, workspace_name=None, overwrite=False, target_dir="/"):
        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)

        # Prepare query parameters
        params = {
            "overwrite": str(overwrite).lower(),
            "is_dir": "false",
            "path": f"/{file_name}",  # relative path for upload
            "target_dir": target_dir
        }

        # Add workspace filters
        if workspace_id:
            params["workspace_id"] = workspace_id
        if workspace_name:
            params["workspace_name"] = workspace_name

        # Prepare headers
        headers = self.headers.copy()  # should already include Auth token
        headers["x-file-size"] = str(file_size)
        headers["Content-Type"] = "application/octet-stream"

        # Perform upload
        with open(file_path, "rb") as f:
            resp = requests.post(
                f"{self.base_url}/workspace/files",
                headers=headers,
                params=params,
                data=f
            )

        if not resp.ok:
            raise Exception(f"Upload failed: {resp.status_code} {resp.text}")
        return resp.json()
