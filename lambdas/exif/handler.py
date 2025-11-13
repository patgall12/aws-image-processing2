import json
from PIL import Image
import io
import boto3
from pathlib import Path

def download_from_s3(bucket, key):
    s3 = boto3.client('s3')
    buffer = io.BytesIO()
    s3.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    return Image.open(buffer)

def upload_to_s3(bucket, key, data, content_type='application/json'):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)

def exif_handler(event, context):
    print("EXIF Lambda triggered")
    print(f"Event received with {len(event.get('Records', []))} SNS records")

    processed_count = 0
    failed_count = 0

    for sns_record in event.get('Records', []):
        try:
            sns_message = json.loads(sns_record['Sns']['Message'])
            for s3_event in sns_message.get('Records', []):
                s3_record = s3_event['s3']
                bucket_name = s3_record['bucket']['name']
                object_key = s3_record['object']['key']

                print(f"Processing: s3://{bucket_name}/{object_key}")
                image = download_from_s3(bucket_name, object_key)

                exif_data = image.getexif()
                json_data = json.dumps({k: str(v) for k, v in exif_data.items()}, indent=2)

                filename = Path(object_key).stem + ".json"
                output_key = f"processed/exif/{filename}"
                upload_to_s3(bucket_name, output_key, json_data)

                processed_count += 1
                print(f"Uploaded EXIF data to {output_key}")

        except Exception as e:
            failed_count += 1
            print(f"Error: {str(e)}")

    print(f"Processing complete: {processed_count} succeeded, {failed_count} failed")
    return {"statusCode": 200, "processed": processed_count, "failed": failed_count}

handler = exif_handler


                       
