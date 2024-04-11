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
    time.sleep(5)  # Small delay to ensure Textract has begun processing
    pages = []
    response = textract_client.get_document_analysis(JobId=job_id)
    
    pages.append(response)
    while response.get('NextToken', None):
        time.sleep(5)
        response = textract_client.get_document_analysis(JobId=job_id, NextToken=response['NextToken'])
        pages.append(response)
        
    return pages

def process_document(pages):
    document_text = ""
    tables = []
    form_fields = {}
    table_block_ids = set()  # Track block IDs associated with tables

    for page in pages:
        for item in page['Blocks']:
            if item['BlockType'] == 'TABLE':
                table_csv, block_ids = extract_table(item, page['Blocks'])
                tables.append(table_csv)
                table_block_ids.update(block_ids)

            elif item['BlockType'] == 'KEY_VALUE_SET':
                if 'KEY' in item['EntityTypes']:
                    key = get_text(item, page['Blocks'])
                elif 'VALUE' in item['EntityTypes']:
                    value = get_text(item, page['Blocks'])
                    form_fields[key] = value

    for page in pages:
        for item in page['Blocks']:
            if item['BlockType'] == 'LINE' and item['Id'] not in table_block_ids:
                document_text += item['Text'] + '\n'

    return document_text, tables, form_fields

def extract_table(table_block, blocks):
    table_dict = {}
    block_ids = set()
    
    for relationship in table_block.get('Relationships', []):
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                block_ids.add(child_id)
                cell_block = next((block for block in blocks if block['Id'] == child_id), None)
                if cell_block:
                    row_index = cell_block.get('RowIndex', 0) - 1
                    col_index = cell_block.get('ColumnIndex', 0) - 1
                    if row_index not in table_dict:
                        table_dict[row_index] = {}
                    table_dict[row_index][col_index] = get_text(cell_block, blocks)

    df = pd.DataFrame.from_dict(table_dict, orient='index').sort_index().fillna('')
    return df.to_csv(index=False, header=False), block_ids

def get_text(block, blocks):
    text = ""
    for relationship in block.get('Relationships', []):
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                child_block = next((b for b in blocks if b['Id'] == child_id), None)
                if child_block and 'Text' in child_block:
                    text += child_block['Text'] + ' '
    return text.strip()

def main():
    st.title('Amazon Textract Document Processing')
    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg'])
    bucket_name = 'streamlit-bucket-1'

    if uploaded_file is not None:
        s3_object = upload_to_s3(uploaded_file, bucket_name, uploaded_file.name)
        if s3_object:
            job_id = start_job(s3_object)
            if job_id:
                results_pages = get_job_results(job_id)
                if results_pages and all(result.get('JobStatus') == 'SUCCEEDED' for result in results_pages):
                    document_text, tables, form_fields = process_document(results_pages)
                    st.subheader("Extracted Text")
                    st.text(document_text)
                    
                    st.subheader("Tables")
                    for i, table_csv in enumerate(tables, start=1):
                        st.write(f"Table {i}:")
                        df = pd.read_csv(StringIO(table_csv))
                        st.dataframe(df)
                    
                    st.subheader("Form Fields")
                    for key, value in form_fields.items():
                        st.write(f"{key}: {value}")
                else:
                    st.error("Document processing failed or did not complete successfully.")

if __name__ == '__main__':
    main()
