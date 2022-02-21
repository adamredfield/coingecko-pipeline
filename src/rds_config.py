# This code is given by AWS' Secrets Manager service (other than last line)
# This service allows for secure data to be unexposed in code

import boto3
import base64
import json
import os
from botocore.exceptions import ClientError
from datetime import datetime
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger()
logging.basicConfig(filename=f'logs/data_{datetime.now()}.log', encoding='utf-8')
logger.setLevel(logging.INFO)

os.environ['AWS_PROFILE'] = "adam-aws"
os.environ['AWS_DEFAULT_REGION'] = "us-west-2"


def get_secret():

    secret_name = "local_cg_2_secrets"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these
        # fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(
                get_secret_value_response['SecretBinary'])

    return json.loads(secret)  # returns the secret as dictionary


def rds_engine(db_name):
    rds_host = get_secret()["host"]
    username = get_secret()["username"]
    password = get_secret()["password"]
    db_name = db_name
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{rds_host}/{db_name}")
    try:
        engine.connect()
        logger.info(
            "Successfully connected to RDS MySql instance via sqlalchemy!")
        return engine
    except SQLAlchemyError as err:
        logger.error(
            "Could not connect to RDS MySql instance via sqlalchemy: ",
            err.__cause__)
