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
textract_client = boto3.client('textract', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION_NAME)
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION_NAME)

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
        response = textract_client.start_document_analysis(DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': object_name}}, FeatureTypes=feature_types)
        return response['JobId']
    except Exception as e:
        st.error(f"Error occurred while starting Textract job: {str(e)}")
        return None

def get_job_results(job_id):
    next_token = None
    pages = []
    while True:
        kwargs = {'JobId': job_id}
        if next_token:
            kwargs['NextToken'] = next_token
        response = textract_client.get_document_analysis(**kwargs)
        pages.extend(response['Blocks'])
        next_token = response.get('NextToken', None)
        if not next_token:
            break
    return pages

def process_document(blocks):
    document_text = ""
    tables = []
    form_fields = {}
    key = ""
    table_block_ids = set()

    for item in blocks:
        if item['BlockType'] == 'TABLE':
            table_csv = extract_table(item, blocks)
            tables.append(table_csv)
            for rel in item.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    table_block_ids.update(rel['Ids'])

        elif item['BlockType'] == 'KEY_VALUE_SET':
            if 'KEY' in item['EntityTypes']:
                key = get_text(item, blocks)
            elif 'VALUE' in item['EntityTypes']:
                value = get_text(item, blocks)
                form_fields[key] = value

    for item in blocks:
        if item['BlockType'] == 'LINE' and item['Id'] not in table_block_ids:
            document_text += item['Text'] + '\n'

    return document_text, tables, form_fields

def extract_table(table_block, blocks):
    table_dict = {}
    for relationship in table_block.get('Relationships', []):
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell_block = next((block for block in blocks if block['Id'] == child_id), None)
                if cell_block:
                    row_index = cell_block.get('RowIndex', 0) - 1
                    col_index = cell_block.get('ColumnIndex', 0) - 1
                    if row_index not in table_dict:
                        table_dict[row_index] = {}
                    table_dict[row_index][col_index] = get_text(cell_block, blocks)
    df = pd.DataFrame.from_dict(table_dict, orient='index').sort_index().fillna('')
    return df.to_csv(index=False, header=False)

def get_text(block, blocks):
    text = ""
    for relationship in block.get('Relationships', []):
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                child_block = next((b for b in blocks if b['Id'] == child_id), None)
                if child_block:
                    if 'Text' in child_block:
                        text += child_block['Text'] + ' '
    return text.strip()

def main():
    st.title('Amazon Textract Document Processing')

    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg'])
    bucket_name = 'streamlit-bucket-1'  # Replace with your actual bucket name

    if uploaded_file is not None:
        s3_object = upload_to_s3(uploaded_file, bucket_name, uploaded_file.name)
        if s3_object:
            job_id = start_job(s3_object)
            if job_id:
                blocks = get_job_results(job_id)
                document_text, tables, form_fields = process_document(blocks)

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

if __name__ == '__main__':
    main()
