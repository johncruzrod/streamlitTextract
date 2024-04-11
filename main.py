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
textract_client = boto3.client('textract',
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                               region_name=AWS_REGION_NAME)

s3_client = boto3.client('s3',
                         aws_access_key_id=AWS_ACCESS_KEY_ID,
                         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                         region_name=AWS_REGION_NAME)

def upload_to_s3(file, bucket, key):
    try:
        s3_client.upload_fileobj(file, bucket, key)
        return f's3://{bucket}/{key}'
    except Exception as e:
        st.error(f"Error occurred while uploading file to S3: {str(e)}")
        return None

def start_job(s3_object):
    feature_types = ['TABLES', 'FORMS']  # Add 'QUERIES' if needed for checkbox detection
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
        response = textract_client.get_document_analysis(JobId=job_id)
        return response
    except Exception as e:
        st.error(f"Error occurred while getting Textract job results: {str(e)}")
        return None

def process_document(response):
    document_text = ""
    tables = []
    forms = []
    
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            document_text += item['Text'] + '\n'
        elif item['BlockType'] == 'TABLE':
            table_csv = extract_table(item, response['Blocks'])
            tables.append(table_csv)
        elif item['BlockType'] == 'KEY_VALUE_SET':
            if 'KEY' in item['EntityTypes']:
                key = get_text(item, response['Blocks'])
            else:
                value = get_text(item, response['Blocks'])
                forms.append((key, value))
    
    return document_text, tables, forms

def extract_table(table_block, blocks):
    rows = {}
    for relationship in table_block.get('Relationships', []):
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = next((b for b in blocks if b['Id'] == child_id), None)
                if cell and 'RowIndex' in cell and 'ColumnIndex' in cell:
                    row_index = cell['RowIndex'] - 1
                    col_index = cell['ColumnIndex'] - 1
                    if row_index not in rows:
                        rows[row_index] = {}
                    rows[row_index][col_index] = get_text(cell, blocks)
    
    table_df = pd.DataFrame(rows).T.fillna('')
    table_csv = table_df.to_csv(index=False, header=False)
    return table_csv

def get_text(block, blocks):
    text = ""
    if 'Relationships' in block:
        for relationship in block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = next((b for b in blocks if b['Id'] == child_id), None)
                    if child_block:
                        text += get_text(child_block, blocks)
    if 'Text' in block:
        text += block['Text']
    return text

def main():
    st.title('Amazon Textract File Processing')
    
    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg'])
    
    if uploaded_file is not None:
        s3_object = upload_to_s3(uploaded_file, 'streamlit-bucket-1', uploaded_file.name)
        
        if s3_object:
            job_id = start_job(s3_object)
            
            if job_id:
                with st.spinner('Processing...'):
                    response = None
                    while True:
                        response = get_job_results(job_id)
                        if response['JobStatus'] == 'SUCCEEDED':
                            break
                        elif response['JobStatus'] == 'FAILED':
                            st.error('The document analysis failed.')
                            return
                        time.sleep(5)
                
                document_text, tables, forms = process_document(response)
                
                st.subheader("Extracted Text")
                st.write(document_text)
                
                for i, table_csv in enumerate(tables, start=1):
                    st.subheader(f"Table {i}")
                    df = pd.read_csv(StringIO(table_csv), header=None)
                    st.dataframe(df)
                
                st.subheader("Form Fields")
                for key, value in forms:
                    st.write(f"{key}: {value}")

if __name__ == '__main__':
    main()
