from pathlib import Path

import pytest

from validation import platform_validator


def test_dbt_executable_prefers_explicit_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DBT_EXECUTABLE", "/opt/tools/dbt")
    monkeypatch.setattr(platform_validator.shutil, "which", lambda _name: "/usr/bin/dbt")
    assert platform_validator._dbt_executable() == "/opt/tools/dbt"


def test_dbt_executable_uses_path_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DBT_EXECUTABLE", raising=False)
    monkeypatch.setattr(platform_validator.shutil, "which", lambda _name: "/usr/bin/dbt")
    assert platform_validator._dbt_executable() == "/usr/bin/dbt"


def test_dbt_executable_uses_repository_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("DBT_EXECUTABLE", raising=False)
    monkeypatch.setattr(platform_validator.shutil, "which", lambda _name: None)
    monkeypatch.setattr(platform_validator, "ROOT", tmp_path)
    local = tmp_path / ".venv/bin/dbt"
    local.parent.mkdir(parents=True)
    local.touch()
    assert platform_validator._dbt_executable() == str(local)


def test_dbt_executable_reports_missing_binary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("DBT_EXECUTABLE", raising=False)
    monkeypatch.setattr(platform_validator.shutil, "which", lambda _name: None)
    monkeypatch.setattr(platform_validator, "ROOT", tmp_path)
    with pytest.raises(RuntimeError, match="dbt executable not found"):
        platform_validator._dbt_executable()
