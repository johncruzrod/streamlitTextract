import streamlit as st
import boto3
import time
import pandas as pd


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
    # Create a dictionary to hold block Ids and their corresponding text
    block_map = {block['Id']: block for block in response['Blocks'] if 'BlockType' in block}
    tables = []

    for block in response['Blocks']:
        if block['BlockType'] == 'TABLE':
            # Get all cells for this table
            cells = [block_map[cell_id] for r in block.get('Relationships', [])
                     for cell_id in r.get('Ids', []) if block_map[cell_id]['BlockType'] == 'CELL']
            # Group cells by their row and column
            cells_by_row = {}
            for cell in cells:
                row_index = cell['RowIndex']
                col_index = cell['ColumnIndex']
                cells_by_row.setdefault(row_index, {})[col_index] = cell.get('Text', '')

            # Convert the dictionary to a DataFrame
            table_df = pd.DataFrame(cells_by_row).sort_index().sort_index(axis=1)
            tables.append(table_df)
    return tables

def main():
    st.title('Amazon Textract File Processing')
    
    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg'])
    
    if uploaded_file is not None:
        option = st.radio('Select processing option', ('Extract Text', 'Extract Tables'))
        
        if st.button('Extract'):
            s3_object = upload_to_s3(uploaded_file, 'streamlit-bucket-1', uploaded_file.name)
            
            if s3_object:
                # Determine feature types based on user selection
                if option == 'Extract Text':
                    feature_types = ['TABLES', 'FORMS']
                else:  # 'Extract Tables'
                    feature_types = ['TABLES']
                
                job_id = start_job(s3_object, feature_types)
                
                if job_id:
                    with st.spinner('Processing...'):
                        # Check job completion
                        while True:
                            response = get_job_results(job_id)
                            if response['JobStatus'] == 'SUCCEEDED':
                                break
                            elif response['JobStatus'] == 'FAILED':
                                st.error('The document analysis failed.')
                                return
                            time.sleep(5)  # Adjust time as needed
                    
                    # Process results based on user option
                    if option == 'Extract Text':
                        text = extract_text(response)
                        if text:
                            st.write(text)
                    elif option == 'Extract Tables':  # Correctly handle 'Extract Tables' as a separate case
                        tables = extract_tables(response)
                        if tables:
                            for i, table_df in enumerate(tables, start=1):
                                st.write(f"Table {i}:")
                                st.dataframe(table_df)

if __name__ == '__main__':
    main()
