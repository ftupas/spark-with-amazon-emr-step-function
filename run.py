from botocore.exceptions import ClientError
import boto3
import configparser
import os

# Setup up of variables
config = configparser.ConfigParser()
with open("dl.cfg", "r") as f:
    config.read_file(f)

region_name = config["AWS"]["region_name"]
aws_access_key_id = config["AWS"]["aws_access_key_id"]
aws_secret_access_key = config["AWS"]["aws_secret_access_key"]
aws_session_token = config["AWS"]["aws_session_token"]
LogUri = config["AWS"]["LogUri"]
s3_bucket = config["AWS"]["s3_bucket"]
files = ["etl.py", "dl.cfg"]
file_path = "s3://" + s3_bucket + "/" + files[0]
config_path = "s3://" + s3_bucket + "/" + files[1]

def upload_files(files, bucket):
    """
    Upload etl script and config file to an S3 bucket.

    Params: 
        files(list): List of files to be uploaded
        bucket(str): Name of the S3 bucket
    """
    # Create S3 client
    s3 = boto3.client(
        "s3",
        region_name = region_name,
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        aws_session_token  = aws_session_token
    )

    # Upload files to S3 bucket
    for file_name in files:
        object_name = file_name
        try:
            print(f"UPLOADING {file_name}...")
            response = s3.upload_file(file_name, bucket, object_name)
            print("DONE!")
        except ClientError as e:
            print(e)

    return response

def create_emr_cluster():
    """
    Creates an EMR cluster that auto-terminates after performing a step (Spark job).
    """
    # Create EMR client
    emr = boto3.client(
        "emr",
        region_name = region_name,
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        aws_session_token  = aws_session_token
    )
    print("CREATING emr cluster...")

    # Create EMR cluster
    cluster_id = emr.run_job_flow(
        Name = "Sparkify_Data_Lake_Job",
        LogUri = LogUri,
        ReleaseLabel = "emr-5.30.0",
        Applications = [
            {
                "Name": "Spark"
            }
        ],
        Instances = {
            "InstanceGroups": [
                {
                    "Name": "Master nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1
                },
                {
                    "Name": "Slave nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 3
                }
            ],
            "Ec2KeyName": "spark-pem",
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False
        },
        VisibleToAllUsers = True,
        JobFlowRole = "EMR_EC2_DefaultRole",
        ServiceRole = "EMR_DefaultRole",
        Steps = [
            {
                "Name" : "spark-transform-step",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep" : {
                    "Jar": "s3n://elasticmapreduce/libs/script-runner/script-runner.jar",
                    "Args": [
                        "/usr/bin/spark-submit", "--deploy-mode", "cluster",
                        "--driver-memory", "10g", "--num-executors", "5",
                        "--executor-cores", "2", "--executor-memory", "10g",
                        "--py-files", config_path, file_path
                    ]
                }
            }
        ]
    )
    print(f"STARTED cluster {cluster_id.get('ClusterArn')}.")

    return True

def main():
    """
    Function used to upload etl script and config file, creating an EMR cluster and submitting
    the etl script as a Spark job.
    Usage: python.exe run.py Windows
           python     run.py Linux/Mac 
    """
    upload_files(files, s3_bucket)
    create_emr_cluster()

if __name__ == "__main__":
    main()