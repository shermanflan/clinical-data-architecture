import base64
from json import loads

import boto3
from botocore.exceptions import ClientError

from lib import logger


def get_secret(
    secret_name: str,
    region_name: str = "us-east-2"
) -> dict:
    """
    Standard Secrets Manager access code borrowed from AWS.

    In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            logger.error("Can't decrypt secret using the provided KMS key")
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            logger.error("An error occurred on the server side")
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("You provided an invalid value for a parameter")
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error("Invalid parameter value for the current state of the resource.")
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("Can't find the resource")
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            return loads(get_secret_value_response['SecretString'])
        else:
            raise Exception("Can't find the string key")
            # return base64.b64decode(get_secret_value_response['SecretBinary'])
