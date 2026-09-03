from pathlib import Path


def test_rls_uses_authenticated_database_roles_not_client_tenant_setting():
    sql = Path("warehouse/sql/08_rls_tenant_policies.sql").read_text(encoding="utf-8").lower()
    assert "session_user" in sql
    assert "tenant_database_role" in sql
    assert "enable row level security" in sql
    assert "with check" in sql
    assert "app.tenant_id" not in sql
    assert "set app" not in sql
