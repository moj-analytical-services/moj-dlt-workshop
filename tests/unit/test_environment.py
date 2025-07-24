from datetime import date

import cloudpathlib
import pytest
from botocore.exceptions import NoCredentialsError

from python_apps.environment import (
    Environment,
)


@pytest.mark.parametrize(
    ("_mock_boto_clients", "expected_alias"),
    [
        ("preprod", "electronic-monitoring-data-preproduction"),
        ("prod", "test-name-production"),
    ],
    indirect=["_mock_boto_clients"],
)
@pytest.mark.usefixtures("_mock_boto_clients")
def test_fetch_account_alias(expected_alias, environment):
    assert environment.alias == expected_alias


@pytest.mark.parametrize(
    ("_mock_boto_clients", "expected_is_prod"),
    [
        ("prod", True),
        ("preprod", False),
    ],
    indirect=["_mock_boto_clients"],
)
@pytest.mark.usefixtures("_mock_boto_clients")
def test_is_prod(expected_is_prod, environment):
    assert environment.is_prod == expected_is_prod


@pytest.mark.parametrize(
    "_mock_boto_clients",
    [
        ("prod"),
        ("preprod"),
    ],
    indirect=["_mock_boto_clients"],
)
@pytest.mark.usefixtures("_mock_boto_clients")
def test_account_number(environment):
    assert environment.account_number == "1234"


@pytest.mark.parametrize(
    ("_mock_boto_clients", "expected_name"),
    [
        ("prod", "prod"),
        ("preprod", "preprod"),
    ],
    indirect=["_mock_boto_clients"],
)
@pytest.mark.usefixtures("_mock_boto_clients")
def test_environment_name(expected_name, environment):
    assert environment.environment_name == expected_name


@pytest.mark.parametrize(
    ("_mock_boto_clients", "bucket_prefix", "full_prefix", "expected_bucket"),
    [
        ("prod", "bucket", True, cloudpathlib.S3Path("s3://emds-prod-bucket-1234")),
        ("prod", "end", True, cloudpathlib.S3Path("s3://emds-prod-end")),
        (
            "prod",
            "other-bucket",
            True,
            cloudpathlib.S3Path("s3://emds-prod-other-bucket-1234"),
        ),
        ("prod", "nonexistent", True, None),
        (
            "prod",
            "other",
            False,
            cloudpathlib.S3Path("s3://emds-prod-other-bucket-1234"),
        ),
        (
            "preprod",
            "bucket",
            True,
            cloudpathlib.S3Path("s3://emds-preprod-bucket-1234"),
        ),
        ("preprod", "end", True, cloudpathlib.S3Path("s3://emds-preprod-end")),
        (
            "preprod",
            "other-bucket",
            True,
            cloudpathlib.S3Path("s3://emds-preprod-other-bucket-1234"),
        ),
        ("preprod", "nonexistent", True, None),
        (
            "preprod",
            "other",
            False,
            cloudpathlib.S3Path("s3://emds-preprod-other-bucket-1234"),
        ),
    ],
    indirect=["_mock_boto_clients"],
)
@pytest.mark.usefixtures("_mock_boto_clients")
def test_get_full_bucket_url(
    bucket_prefix,
    full_prefix,
    expected_bucket,
    environment,
):
    assert environment.get_full_bucket_url(bucket_prefix, full_prefix) == expected_bucket


@pytest.mark.usefixtures("_mock_no_credentials")
def test_no_aws_credentials():
    with pytest.raises(NoCredentialsError, match="Unable to locate credentials"):
        Environment()


@pytest.mark.parametrize(
    "_mock_boto_clients",
    [
        "prod",
        "preprod",
    ],
    indirect=["_mock_boto_clients"],
)
@pytest.mark.usefixtures("_mock_boto_clients")
def test_clear_instance_is_none_and_not_none():
    """Test clear method when _instance is None."""
    env = Environment()
    assert Environment._instance is not None
    Environment.clear(env)
    assert Environment._instance is None
    Environment.clear(env)
    assert Environment._instance is None
