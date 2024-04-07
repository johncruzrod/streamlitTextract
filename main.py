import streamlit as st
import boto3
import time

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

def start_job(s3_object, feature_types):
    try:
        bucket_name = s3_object.split('/')[2]
        object_name = '/'.join(s3_object.split('/')[3:])
        st.write(f"Starting Textract job for object: s3://{bucket_name}/{object_name}")
        st.write(f"Bucket Name: {bucket_name}")
        st.write(f"Object Key: {object_name}")
        st.write(f"Region Name: {AWS_REGION_NAME}")
        
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

def extract_text(response):
    text = ""
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            text += item['Text'] + '\n'
    return text

def extract_tables(response):
    tables = []
    # Create a dictionary to hold block Ids and their corresponding text
    block_map = {block['Id']: block for block in response['Blocks'] if 'BlockType' in block}
    
    for block in response['Blocks']:
        if block['BlockType'] == 'TABLE':
            table_data = []
            if 'Relationships' in block:
                for relationship in block['Relationships']:
                    if relationship['Type'] == 'CHILD':
                        for child_id in relationship['Ids']:
                            row_data = []
                            cell = block_map[child_id]
                            if 'Relationships' in cell and cell['Relationships']:
                                for rel in cell['Relationships']:
                                    if rel['Type'] == 'CHILD':
                                        for cid in rel['Ids']:
                                            cell_block = block_map[cid]
                                            row_data.append(cell_block.get('Text', ''))
                            table_data.append(row_data)
            tables.append(table_data)
    return tables

def main():
    st.title('Amazon Textract File Processing')
    
    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg'])
    
    if uploaded_file is not None:
        # Upload the file to S3
        s3_object = upload_to_s3(uploaded_file, 'streamlit-bucket-1', uploaded_file.name)
        
        if s3_object:
            option = st.radio('Select processing option', ('Extract Text', 'Extract Tables'))
            
            if option == 'Extract Text':
                job_id = start_job(s3_object, ['TABLES', 'FORMS'])
            else:
                job_id = start_job(s3_object, ['TABLES'])
            
            if job_id:
                with st.spinner('Processing...'):
                    while True:
                        response = get_job_results(job_id)
                        if response['JobStatus'] == 'SUCCEEDED':
                            break
                        time.sleep(1)
                
                if option == 'Extract Text':
                    text = extract_text(response)
                    if text:
                        st.write(text)
                else:
                    tables = extract_tables(response)
                    if tables:
                        for i, table in enumerate(tables, start=1):
                            st.write(f"Table {i}:")
                            st.table(table)

if __name__ == '__main__':
    main()
