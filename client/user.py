from .base import YeeduClient

class UserManager(YeeduClient):
    def associate_tenant(self, tenant_id: str):
        """
        Associates the current user with a tenant.

        :param tenant_id: ID of the tenant to associate (UUID)
        :return: API response JSON
        """
        # tenant_id is passed as a path parameter
        print(tenant_id)
        endpoint = f"/user/select/{tenant_id}"
        return self._request("POST", endpoint)
