import boto3
import os
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()
    AWS_ACCESS_KEY = os.getenv('ACCESS_KEY')
    AWS_SECRET_KEY = os.getenv('SECRET_KEY')
    AWS_REGION = "ap-southeast-1"

    s3 = boto3.client('s3',
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    bucket_name = 'test-bucket-bigdataisnotreal101'

    # create bucket
    # try:
    #    s3.create_bucket(Bucket = bucket_name, CreateBucketConfiguration = {'LocationConstraint': AWS_REGION})
    #    print('Bucket has created!')
    # except:
    #    print("Failed to create bucket!")

    key = "data.csv"
    file_path = './data.csv'

    # upload csv weather file
    try:
        s3.put_object(Body=file_path, Bucket=bucket_name, Key=key)
        print("Successful upload file!")
    except:
        print("Fail to upload, try again!")

    # read all files/messages in bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        for obj in response['Contents']:
            print(obj['Key'])