# Syntax: pytest tests/test_admin_broker.py

import pytest
from unittest.mock import patch, MagicMock
import admin_broker


# Fixture functions to run before every test function


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def mock_admin():
    return MagicMock()

# Tests of create_broker_admin()

@patch("admin_broker.KafkaAdminClient")
@patch("admin_broker.cmn.get_servers_list", return_value=["localhost:9092"])
def test_create_broker_admin_ok(mock_servers, mock_client_cls, mock_logger):
    fake_admin = MagicMock()
    mock_client_cls.return_value = fake_admin
    result = admin_broker.create_broker_admin(mock_logger)
    assert result is fake_admin
    mock_logger.info.assert_called_once_with("Broker Admin created")