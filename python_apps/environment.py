import logging
import os

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3 = boto3.client("s3")

ENV_BUCKET_PREFIX = "emds"


class Environment:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            try:
                cls._instance.alias = boto3.client("iam").list_account_aliases()[
                    "AccountAliases"
                ][0]
            except (IndexError, KeyError):
                msg = "No account alias found. Assuming preprod environment."
                cls._instance.alias = "electronic-monitoring-data-preproduction"
            try:
                cls._instance.account_no = boto3.client("sts").get_caller_identity()[
                    "Account"
                ]
                cls._instance.bucket_list = boto3.client("s3").list_buckets()["Buckets"]
            except NoCredentialsError:
                msg = "AWS credentials not found."
                logger.error(msg)
                raise RuntimeError(msg) from None
            except PartialCredentialsError:
                msg = "Incomplete AWS credentials."
                logger.error(msg)
                raise RuntimeError(msg) from None
        return cls._instance

    @property
    def account_number(self):
        return self.account_no

    @property
    def is_prod(self):
        return self.environment_name == "prod"

    @property
    def environment_name(self):
        full_env_name = self.alias.split("-")[-1]
        environment_mapping = {
            "production": "prod",
            "development": "dev",
            "test": "test",
            "preproduction": "preprod",
        }
        return environment_mapping.get(full_env_name, full_env_name)

    def get_full_bucket_url(
        self,
        bucket_prefix,
    ):
        """Assuming we are in one of the EM namespaces,
        get bucket names from prefix/env

        Parameters
        ----------
        bucket_prefix : str
            prefix of the bucket, by default None
        full_prefix : bool
            whether full prefix is required,
            by default True
        Returns
        -------
        str
            Bucket name for the environment and prefix, if found.
        """
        for bucket in self.bucket_list:
            if (
                f"{ENV_BUCKET_PREFIX}-{self.environment_name}-{bucket_prefix}"
                == "-".join(bucket["Name"].split("-")[:-1])
            ) or (
                f"{ENV_BUCKET_PREFIX}-{self.environment_name}-{bucket_prefix}"
                == bucket["Name"]
            ):
                return bucket["Name"]
        return None

    def clear(cls):
        if cls._instance is None:
            pass
        else:
            Environment._instance = None

    def export_dbt_variables(self, ACTIONS=False):
        """Export dbt variables for the environment"""
        s3_data_bucket_name = self.get_full_bucket_url("cadt")
        dbt_test_profile_workgroup = f"{self.account_number}-default"

        if ACTIONS:
            export_bucket = f'echo "S3_DATA_BUCKET_NAME={s3_data_bucket_name}" \
                >> $GITHUB_ENV\n'
            export_dbt_profile = f'echo \
                "DBT_TEST_PROFILE_WORKGROUP={dbt_test_profile_workgroup}"\
                 >> $GITHUB_ENV\n'
            export_dbt_profile_location = ""
        else:
            export_bucket = f"export S3_DATA_BUCKET_NAME='{s3_data_bucket_name}'\n"
            export_dbt_profile = f"""
                export DBT_TEST_PROFILE_WORKGROUP='{dbt_test_profile_workgroup}'\n
                """
            export_dbt_profile_location = 'export DBT_PROFILES_DIR="../.dbt/"'

        with open("set_env.sh", "w") as f:
            f.write(export_bucket)
            f.write(export_dbt_profile)
            f.write(export_dbt_profile_location)


if __name__ == "__main__":
    ACTIONS = os.getenv("ACTIONS", False)
    env = Environment()
    env.export_dbt_variables(ACTIONS)
