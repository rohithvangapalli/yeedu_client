from .base import YeeduClient


class ClusterManager(YeeduClient):
    def get_cluster(self, cluster_id: int = None, cluster_name: str = None):
        params = {}
        if cluster_id is not None:
            params["cluster_id"] = cluster_id
        if cluster_name is not None:
            params["cluster_name"] = cluster_name

        return self._request("GET", "/cluster", params=params)
