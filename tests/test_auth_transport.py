import json

import pytest

from geesampler.auth import validate_auth_config
from geesampler.models import AuthConfig
from geesampler.transport import RequestsTransport


def _key(path, *, project="project-x", email="worker@project-x.iam.gserviceaccount.com"):
    path.write_text(
        json.dumps(
            {
                "type": "service_account",
                "project_id": project,
                "client_email": email,
                "private_key": "not-a-real-key",
            }
        ),
        encoding="utf-8",
    )
    return path


def test_service_account_key_must_be_owner_only(tmp_path):
    path = _key(tmp_path / "key.json")
    path.chmod(0o644)
    config = AuthConfig("project-x", "worker@project-x.iam.gserviceaccount.com", path)
    with pytest.raises(PermissionError, match="0600"):
        validate_auth_config(config)
    path.chmod(0o600)
    validate_auth_config(config)


def test_service_account_key_identity_must_match(tmp_path):
    path = _key(tmp_path / "key.json")
    path.chmod(0o600)
    with pytest.raises(ValueError, match="project does not match"):
        validate_auth_config(
            AuthConfig("other-project", "worker@project-x.iam.gserviceaccount.com", path)
        )


def test_requests_transport_sizes_both_connection_pools():
    transport = RequestsTransport(pool_size=17)
    assert transport.pool_size == 17
    assert transport.session.get_adapter("https://")._pool_maxsize == 17
    assert transport.session.get_adapter("http://")._pool_block is True
    transport.close()
