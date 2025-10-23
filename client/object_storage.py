import os
import requests
from .base import YeeduClient

class ObjectStorageManager(YeeduClient):

    def list_files(self, osm_id=None, osm_name=None, limit=100, page=1):
        #params = {"limit": limit, "pageNumber": page}
        params = {}
        if osm_id: params["object_storage_manager_id"] = osm_id
        if osm_name: params["object_storage_manager_name"] = osm_name
        return self._request("GET", "/object_storage_manager/files", params=params)


    def upload_file(self, file_path, osm_id=None, osm_name=None, overwrite=False, target_dir="//"):
        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)

        params = {
            "overwrite": str(overwrite).lower(),
            "is_dir": "false",
            "path": f"/{file_name}",
            "target_dir": target_dir
        }

        if osm_id:
            params["object_storage_manager_id"] = osm_id
        if osm_name:
            params["object_storage_manager_name"] = osm_name

        headers = self.headers.copy()
        headers["x-file-size"] = str(file_size)
        headers["Content-Type"] = "application/octet-stream"

        with open(file_path, "rb") as f:
            resp = requests.post(
                f"{self.base_url}/object_storage_manager/files",
                headers=headers,
                params=params,
                data=f
            )

        if not resp.ok:
            raise Exception(f"Upload failed: {resp.status_code} {resp.text}")
        return resp.json()

    def delete_file(self, file_id=None, file_path=None, osm_id=None, osm_name=None):
        params = {}
        if file_id: params["file_id"] = file_id
        if file_path: params["file_path"] = file_path
        if osm_id: params["object_storage_manager_id"] = osm_id
        if osm_name: params["object_storage_manager_name"] = osm_name
        return self._request("DELETE", "/object_storage_manager/file", params=params)
