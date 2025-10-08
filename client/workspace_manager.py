from .base import YeeduClient


class WorkspaceManager(YeeduClient):
    def get_workspace(self, workspace_id: int = None, workspace_name: str = None):
        params = {}
        if workspace_id is not None:
            params["workspace_id"] = workspace_id
        if workspace_name is not None:
            params["workspace_name"] = workspace_name

        return self._request("GET", "/workspace", params=params)
