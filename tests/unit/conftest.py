import datetime
import shutil
from functools import wraps

import pytest
from botocore.exceptions import NoCredentialsError

from python_apps.environment import Environment

FAKE_TIME = datetime.datetime(2021, 1, 1, 0, 0, 1)


@pytest.fixture
def test_folder_structure(tmp_path):
    """Create a temporary folder structure for testing"""
    # Create main folder
    test_folder = tmp_path / "test_folder"
    test_folder.mkdir()

    # Create some nested folders and files
    (test_folder / "subfolder1").mkdir()
    (test_folder / "subfolder1" / "file1.txt").write_text("content1")
    (test_folder / "subfolder2").mkdir()
    (test_folder / "subfolder2" / "file2.txt").write_text("content2")
    (test_folder / "root_file.txt").write_text("root_content")

    yield test_folder

    # Cleanup
    if test_folder.exists():
        shutil.rmtree(test_folder)


@pytest.fixture
def _patch_datetime_now(monkeypatch):
    class MockDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ARG003
            return FAKE_TIME

        @classmethod
        def today(cls):
            return FAKE_TIME

    monkeypatch.setattr(datetime, "datetime", MockDateTime)
    monkeypatch.setattr("airflow_load_em_data.load_em_data.utils.datetime", MockDateTime)


@pytest.fixture
def _mock_dlt_transformer(monkeypatch):
    def mock_dlt_transformer(*args, **kwargs):
        """Mock transformer factory, returning a no-op decorator."""

        # This is the actual decorator
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                return f(*args, **kwargs)

            return decorated_function

        return decorator

    # Patch the `transformer` function in the `dlt` module
    monkeypatch.setattr("dlt.transformer", mock_dlt_transformer)


@pytest.fixture
def _mock_boto_clients(request, monkeypatch):
    """Fixture to mock the boto3 client calls for prod and preprod environments"""

    class MockIAMClient:
        def list_account_aliases(self):
            if request.param == "prod":
                return {"AccountAliases": ["test-name-production"]}
            if request.param == "preprod":
                msg = "No account alias found. Assuming preprod environment."
                raise IndexError(msg)
            msg = f"Invalid environment for IAM client: {request.param}"
            raise ValueError(msg)

    class MockS3Client:
        def list_buckets(self):
            if request.param == "prod":
                return {
                    "Buckets": [
                        {"Name": "emds-prod-bucket-1234"},
                        {"Name": "emds-prod-other-bucket-1234"},
                        {"Name": "emds-prod-end"},
                    ]
                }
            if request.param == "preprod":
                return {
                    "Buckets": [
                        {"Name": "emds-preprod-bucket-1234"},
                        {"Name": "emds-preprod-other-bucket-1234"},
                        {"Name": "emds-preprod-end"},
                    ]
                }
            msg = f"Invalid environment for S3 client: {request.param}"
            raise ValueError(msg)

    class MockSTSClient:
        def get_caller_identity(self):
            return {"Account": "1234"}

    class MockSession:
        def client(self, service_name):
            if service_name == "iam":
                return MockIAMClient()
            if service_name == "s3":
                return MockS3Client()
            if service_name == "sts":
                return MockSTSClient()
            msg = f"Unexpected service: {service_name}"
            raise ValueError(msg)

    def mock_boto_client(service_name):
        if service_name == "iam":
            return MockIAMClient()
        if service_name == "s3":
            return MockS3Client()
        if service_name == "sts":
            return MockSTSClient()
        msg = f"Unexpected service: {service_name}"
        raise ValueError(msg)

    monkeypatch.setattr("boto3.client", mock_boto_client)
    monkeypatch.setattr("boto3.session.Session", MockSession)


@pytest.fixture
def _mock_no_credentials(monkeypatch):
    """Fixture to simulate No AWS credentials found."""

    def mock_boto_client(service_name):  # noqa: ARG001
        raise NoCredentialsError()

    monkeypatch.setattr("boto3.client", mock_boto_client)


@pytest.fixture
def environment():
    env = Environment()
    yield env
    Environment.clear(env)
