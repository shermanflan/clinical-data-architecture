from json import loads
from typing import Dict

import boto3
from botocore.exceptions import ClientError

from lib import logger


def get_secret(
    secret_name: str,
    region_name: str = "us-east-2"
) -> Dict[str, str]:
    """
    Standard Secrets Manager access code borrowed from AWS.

    This implementation only handles the specific exceptions for the
    'GetSecretValue' API.
    - https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html

    In addition, it decrypts the secret using the associated KMS CMK.
    Depending on whether the secret is a string or binary, one or the
    other of these fields will be populated. If binary, this returns
    the decoded bytes, which will need to be decoded via UTF-8.

    :param secret_name: the Secrets Manager key name
    :param region_name: the optional AWS region name
    :return: the unpacked secret key/values as a Dictionary
    """
    logger.info("Creating Secrets Manager client")

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        secret_value = client.get_secret_value(SecretId=secret_name)

        if 'SecretString' in secret_value:
            return loads(secret_value['SecretString'])
        # else:
        #     from base64 import b64decode
        #     return b64decode(secret_value['SecretBinary'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            logger.error("Can't decrypt secret using the provided KMS key")
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            logger.error("An error occurred on the server side")
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("You provided an invalid value for a parameter")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error("Invalid parameter value for the current state of the resource.")
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("Can't find the resource")
        elif e.response['Error']['Code'] == 'AccessDeniedException':
            logger.error("Unauthorized access")

        raise e
