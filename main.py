import streamlit as st
import boto3
import time
import pandas as pd
from io import StringIO

# Retrieve AWS credentials from Streamlit secrets
AWS_ACCESS_KEY_ID = st.secrets['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']
AWS_REGION_NAME = st.secrets['AWS_REGION_NAME']

# Initialize boto3 clients
textract_client = boto3.client(
    'textract',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION_NAME
)

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION_NAME
)

def upload_to_s3(file, bucket, key):
    try:
        s3_client.upload_fileobj(file, bucket, key)
        return f's3://{bucket}/{key}'
    except Exception as e:
        st.error(f"Error occurred while uploading file to S3: {str(e)}")
        return None

def start_job(s3_object):
    feature_types = ['TABLES', 'FORMS']
    try:
        bucket_name = s3_object.split('/')[2]
        object_name = '/'.join(s3_object.split('/')[3:])
        response = textract_client.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': object_name}},
            FeatureTypes=feature_types
        )
        return response['JobId']
    except Exception as e:
        st.error(f"Error occurred while starting Textract job: {str(e)}")
        return None

def get_job_results(job_id):
    try:
        time.sleep(5)  # Small delay to ensure Textract has begun processing
        response = textract_client.get_document_analysis(JobId=job_id)
        while response['JobStatus'] not in ['SUCCEEDED', 'FAILED']:
            time.sleep(5)
            response = textract_client.get_document_analysis(JobId=job_id)
        return response
    except Exception as e:
        st.error(f"Error occurred while getting Textract job results: {str(e)}")
        return None

def process_document(response):
    document_text = ""
    tables = []
    form_fields = {}
    key = ""

    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            document_text += item['Text'] + '\n'
        elif item['BlockType'] == 'TABLE':
            table_csv = extract_table(item, response['Blocks'])
            tables.append(table_csv)
        elif item['BlockType'] == 'KEY_VALUE_SET':
            if 'KEY' in item['EntityTypes']:
                key = get_text(item, response['Blocks'])
            elif 'VALUE' in item['EntityTypes']:
                value = get_text(item, response['Blocks'])
                form_fields[key] = value
    
    return document_text, tables, form_fields

# The `extract_table` and `get_text` functions from your original code remain unchanged

def main():
    st.title('Amazon Textract Document Processing')
    
    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg'])
    bucket_name = 'streamlit-bucket-1'  # Replace with your actual bucket name
    
    if uploaded_file is not None:
        s3_object = upload_to_s3(uploaded_file, bucket_name, uploaded_file.name)
        if s3_object:
            job_id = start_job(s3_object)
            if job_id:
                response = get_job_results(job_id)
                if response and response.get('JobStatus') == 'SUCCEEDED':
                    document_text, tables, form_fields = process_document(response)
                    
                    st.subheader("Extracted Text")
                    st.text(document_text)
                    
                    st.subheader("Tables")
                    for i, table_csv in enumerate(tables, start=1):
                        st.write(f"Table {i}:")
                        df = pd.read_csv(StringIO(table_csv), header=None)
                        st.dataframe(df)
                    
                    st.subheader("Form Fields")
                    for key, value in form_fields.items():
                        st.write(f"{key}: {value}")
                else:
                    st.error("Document processing failed or did not complete successfully.")

if __name__ == '__main__':
    main()
