import json
import logging

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

logger = logging.getLogger(__name__)

# Aws configuration
BUCKET_NAME = Variable.get("AWS_REGION_NAME")


def get_boto3_session():
    """
    Returns boto3 session from Airflow Connections
    """

    aws_hook = AwsBaseHook(aws_conn_id="AWS_CONN")
    my_session = aws_hook.get_session(region_name=BUCKET_NAME)
    return my_session


def push_data_to_s3(boto3_session, data: dict, bucket_name, bucket_key):
    """
    Accepts python dictionary as input and loads it into s3 bucket as json file
    """

    s3_client = boto3_session.client(service_name="s3")
    json_data = json.dumps(obj=data, default="str")

    try:
        logger.info(f"Loading data into s3://{bucket_name}/{bucket_key}")
        s3_client.put_object(
            Bucket=bucket_name, Key=bucket_key, Body=json_data
        )
        logger.info(
            f"Data is successfully loaded into s3://{bucket_name}/{bucket_key}"
        )

    except Exception as e:
        logger.error(f"Operation not successful: {e}")
        raise


def get_data_from_s3(boto3_session, bucket_name, bucket_key):
    """
    Gets Json data from s3 bucket ane returns it as a python dictionary
    """

    s3_client = boto3_session.client(service_name="s3")

    try:
        logger.info(f"Pulling data from s3://{bucket_name}/{bucket_key}")
        from_s3 = s3_client.get_object(Bucket=bucket_name, Key=bucket_key)

        data = from_s3["Body"].read()
        logger.info(
            f"Successfully pulled data from s3://{bucket_name}/{bucket_key}"
        )

        return json.loads(data)

    except Exception as e:
        logger.error(f"Operation not successful::: {e}")
        raise
