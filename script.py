#!/usr/bin/python

# Import required python libraries
import os
import sys
import time
import datetime
import pipes
import base64
import json
import logging
from botocore.exceptions import ClientError
import sentry_sdk
import boto3
import requests
import base64

from datetime import date
import calendar

my_date = date.today()
WEEKDAY = calendar.day_name[my_date.weekday()]

# MySQL database details to which backup to be done. Make sure below user having enough privileges to take databases backup.
# To take multiple databases backup, create any file liken /backup/dbnames.txt and put databases names one on each line and assigned to DB_NAME variable.

# reading .env file
# environ.Env.read_env(env_file='sensehawk_project_terra/settings/.env_local')
# environ.Env.read_env(env_file='sensehawk_data_vault/settings/.env')

SERVICE_NAME = os.getenv('SERVICE_NAME')

def get_aws_backups_keys_consul(consul_key):

    consul_url = "https://consul.sensehawk.com:8501/v1/kv/%s/backups/keys" % (SERVICE_NAME.lower())
    res = requests.get(url=consul_url, headers={"X-Consul-Token": consul_key}, timeout=5, verify=False)
    try:
        if res.status_code in [200, 202]:
            value = res.json()[0].get('Value')
            return json.loads(base64.b64decode(value).decode("utf-8"))
    except Exception as e:
        return {}


s3 = get_aws_backups_keys_consul(os.getenv('CONSUL_KEY')) or {}

if not s3:
    print({"error": "aws keys are not available from consul"})
    sentry_sdk.capture_message(str("aws keys are not available from consul"))
    sys.exit()

SENTRY_KEY = os.getenv('SENTRY_KEY')
SENTRY_PROJECT = os.getenv('SENTRY_PROJECT')

sentry_sdk.init("https://" + SENTRY_KEY + "@sentry.io/" + SENTRY_PROJECT)


def put_object(dest_bucket_name, dest_object_name, src_data):
    """Add an object to an Amazon S3 bucket

    The src_data argument must be of type bytes or a string that references
    a file specification.

    :param dest_bucket_name: string
    :param dest_object_name: string
    :param src_data: bytes of data or string reference to file spec
    :return: True if src_data was added to dest_bucket/dest_object, otherwise
    False
    """

    # Construct Body= parameter
    if isinstance(src_data, bytes):
        object_data = src_data
    elif isinstance(src_data, str):
        try:
            object_data = open(src_data, 'rb')
            # possible FileNotFoundError/IOError exception
        except Exception as e:
            logging.error(e)
            return False
    else:
        logging.error('Type of ' + str(type(src_data)) +
                      ' for the argument \'src_data\' is not supported.')
        return False

    # Put the object
    client = boto3.client(
        's3',
        aws_access_key_id=s3['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=s3['AWS_SECRET_ACCESS_KEY']
    )

    try:
        client.put_object(Bucket=dest_bucket_name, Key=dest_object_name, Body=object_data)
    except ClientError as e:
        message = {
            'app': 'vault',
            'script': 'backup_db',
            'error': str(e)
        }
        sentry_sdk.capture_message(str(message))
        # AllAccessDisabled error == bucket not found
        # NoSuchKey or InvalidRequest error == (dest bucket/obj == src bucket/obj)
        logging.error(e)
        return False
    finally:
        if isinstance(src_data, str):
            object_data.close()
    return True


DB_HOST = os.getenv('DBHOST')
DB_USER = os.getenv('DBUSER')
DB_PASS = os.getenv('DBPASSWORD')
DB_PORT = os.getenv('DBPORT')
DB_NAME = os.getenv('DBNAME')
BACKUP_PATH = os.getenv('BACKUP_PATH')

try:

    # Getting current DateTime to create the separate backup folder like "20180817-123433".
    DATETIME = time.strftime('%Y_%m_%d')
    TODAYBACKUPPATH = BACKUP_PATH + '/' + WEEKDAY

    # Checking if backup folder already exists or not. If not exists will create it.
    try:
        os.stat(TODAYBACKUPPATH)
    except Exception as e:
        os.mkdir(TODAYBACKUPPATH)

    # Code for checking if you want to take single database backup or assinged multiple backups in DB_NAME.
    print("checking for databases names file.")
    DB_PATH = TODAYBACKUPPATH + '/' + DB_NAME + '.pgsql.gz'

    if os.path.exists(DB_PATH):
        print("Databases file found...")
        print("first deleting the existing folder DB")
        deletecmd = "rm -rf " + DB_PATH
        os.system(deletecmd)
        print("Starting backup of all dbs listed in file " + DB_NAME)
    else:
        print("Databases file not found...")
        print("Starting backup of database " + DB_NAME)

    # Starting actual database backup process.
    db = DB_NAME
    dumpcmd = "PGPASSWORD=" + DB_PASS + " pg_dump -h " + DB_HOST + " -U " + DB_USER + " -p" + DB_PORT + " " + db + " > " + pipes.quote(
        TODAYBACKUPPATH) + "/" + db + ".pgsql"
    os.system(dumpcmd)
    gzipcmd = "gzip " + pipes.quote(TODAYBACKUPPATH) + "/" + db + ".pgsql"
    os.system(gzipcmd)

    INSTANCE_STAGE = os.getenv('INSTANCE_STAGE')
    ISO_FORMAT_DATETIME = datetime.datetime.now().isoformat()
    src_data = pipes.quote(TODAYBACKUPPATH) + "/" + db + ".pgsql.gz"
    dest_object_name = "%s/%s/%s.gz" % (s3['prefix'], INSTANCE_STAGE, ISO_FORMAT_DATETIME)
    put_object(s3['bucket'], dest_object_name, src_data)
    print("Backup script completed")
    print("Your backups have been created in '" + TODAYBACKUPPATH + "' directory")

except Exception as e:
    print('something went wrong')
    print(str(e))
    message = {
        'app': 'vault',
        'script': 'backup_db',
        'error': str(e)
    }
    # sentry_sdk.capture_message(str(message))

