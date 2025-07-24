from datetime import date

import cloudpathlib
import pytest
from botocore.exceptions import NoCredentialsError

from python_apps.environment import (
    Environment,
    extract_delivery_date_from_file,
    extract_feed_type_from_file,
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


class TestExtractDeliveryDateFromFile:
    @pytest.mark.parametrize(
        ("file_name", "date_str"),
        [
            (
                "table-name-20230303123456-nsp-100.jsonl",
                date(2023, 3, 3),
            ),
            (
                "table-name  (brackets here ) -20230303123456-nsp-100.jsonl",
                date(2023, 3, 3),
            ),
            (
                "__TABLE_WITH_MANY-NAMES-20201101111111-ctc.jsonl",
                date(2020, 11, 1),
            ),
            (
                "-_-_-19990821023000-sp-1231231241234123124.jsonl",
                date(1999, 8, 21),
            ),
        ],
    )
    def test_extract_delivery_date_from_file(
        self,
        file_name,
        date_str,
    ):
        expected = date_str
        actual = extract_delivery_date_from_file(file_name)
        assert expected == actual

    @pytest.mark.parametrize(
        "file_name",
        [
            "000-20200101010101-nsp",
            "this wont match",
            "",
        ],
    )
    def test_failure_regex(
        self,
        file_name,
    ):
        with pytest.raises(ValueError, match=f"{file_name = } is not in correct format."):
            extract_delivery_date_from_file(file_name)


class TestModel:
    def __init__(self):
        self.table_name_ = {}
        self.matts_bday_ = {}
        self.world_cup_centenary = {}


class TestExtractFeedTypeFromFile:
    @pytest.mark.parametrize(
        ("file_name", "feed_type"),
        [
            (
                "2021-01-01/table_name_-20210101111111-nsp-1.jsonl",
                "general",
            ),
            (
                "1999-08-21/matts_bday_-19990821023000-ctc.jsonl",
                "home_office",
            ),
            (
                "2030-06-08/world_cup_centenary-20300608150000-sp.jsonl",
                "specials",
            ),
            (
                "2030-06-08/world_cup_centenary-20300608150000-nsp.jsonl",
                "general",
            ),
            (
                "2030-06-08/world_cup_centenary-20300608150000-ctc.jsonl",
                "home_office",
            ),
        ],
    )
    def test_extract_feed_type_from_file(
        self,
        file_name,
        feed_type,
    ):
        actual = extract_feed_type_from_file(file_name)
        assert actual == feed_type