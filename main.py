import streamlit as st
import boto3
import time
import pandas as pd
from io import StringIO

# Initialize session state for 'logged_in' flag if it doesn't exist
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

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
    feature_types = ['TABLES']
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
    while True:
        response = textract_client.get_document_analysis(JobId=job_id)
        status = response['JobStatus']
        
        if status == 'SUCCEEDED':
            pages = [response]
            while 'NextToken' in response:
                response = textract_client.get_document_analysis(JobId=job_id, NextToken=response['NextToken'])
                pages.append(response)
            return pages
        elif status == 'FAILED':
            st.error(f"Document analysis has failed with status: {status}")
            return None
        
        time.sleep(5)

def process_document(pages):
    document_text = ""
    tables = []
    table_block_ids = set()
    for page in pages:
        for item in page['Blocks']:
            if item['BlockType'] == 'TABLE':
                table_csv, block_ids = extract_table(item, page['Blocks'])
                tables.append(table_csv)
                table_block_ids.update(block_ids)
    for page in pages:
        for item in page['Blocks']:
            if item['BlockType'] == 'LINE' and item['Id'] not in table_block_ids:
                document_text += item['Text'] + '\n'
    return document_text, tables

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

def delete_file_from_s3(bucket_name, file_name):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)
        st.success(f"File {file_name} has been deleted from S3.")
    except Exception as e:
        st.error(f"Error occurred while deleting file from S3: {str(e)}")

def main():
    st.title('Amazon Textract Document Processing')
    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg'])
    bucket_name = 'streamlit-bucket-1'
    
    if uploaded_file is not None:
        s3_object = upload_to_s3(uploaded_file, bucket_name, uploaded_file.name)
        if s3_object:
            job_id = start_job(s3_object)
            if job_id:
                with st.spinner("Processing document..."):
                    results_pages = get_job_results(job_id)
                    if results_pages:
                        document_text, tables = process_document(results_pages)
                        st.subheader("Extracted Text")
                        st.text_area('Extracted Text', document_text, height=150)
                        if tables:
                            st.subheader("Extracted Tables")
                            for i, table_csv in enumerate(tables, start=1):
                                st.write(f"Table {i}:")
                                df = pd.read_csv(StringIO(table_csv))
                                st.dataframe(df)
                        
                        delete_file_from_s3(bucket_name, uploaded_file.name)
                    else:
                        st.error("Document processing failed or did not complete successfully.")

# Password form
def password_form():
    st.sidebar.title("Access")
    password = st.sidebar.text_input("Enter the password", type="password")
    if st.sidebar.button("Enter"):
        if check_password(password):
            st.session_state.logged_in = True
        else:
            st.sidebar.error("Incorrect password, please try again.")

# Check password function
def check_password(password):
    correct_password = st.secrets["PASSWORD"]
    return password == correct_password

# Check if logged in, if not show password form, else show the main app
if not st.session_state.logged_in:
    password_form()
else:
    main()
