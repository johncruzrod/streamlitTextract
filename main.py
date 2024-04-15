import streamlit as st
import boto3
import time
import pandas as pd
from io import StringIO
import anthropic

# Initialize session state for 'logged_in' flag if it doesn't exist
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

# Retrieve AWS credentials from Streamlit secrets
AWS_ACCESS_KEY_ID = st.secrets['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']
AWS_REGION_NAME = st.secrets['AWS_REGION_NAME']

# Use Streamlit's secret management to safely store and access your API key and the correct password
api_key = st.secrets["ANTHROPIC_API_KEY"]
client = anthropic.Anthropic(api_key=api_key)

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
    # Specify only 'TABLES' if you want tables, remove 'FORMS' to avoid form fields results
    feature_types = ['TABLES']  # Removed 'FORMS' from this list
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

def get_job_results(job_id, timeout=3600):  # Timeout in seconds
    # Initial delay settings
    delay = 5
    max_delay = 60  # Maximum delay between retries
    start_time = time.time()
    
    # Polling loop
    while True:
        response = textract_client.get_document_analysis(JobId=job_id)
        status = response['JobStatus']
        
        if status in ['SUCCEEDED', 'FAILED']:
            if status == 'SUCCEEDED':
                # Collect all pages if paginated results exist
                pages = [response]
                while 'NextToken' in response:
                    response = textract_client.get_document_analysis(JobId=job_id, NextToken=response['NextToken'])
                    pages.append(response)
                return pages
            elif status == 'FAILED':
                st.error(f"Document analysis has failed with status: {status}")
                return None
        
        # Check for timeout to avoid infinite waiting
        if time.time() - start_time > timeout:
            st.error(f'Timeout reached while waiting for document analysis to complete.')
            return None
        
        # Sleep before the next check, with exponential backoff
        time.sleep(delay)
        delay = min(delay * 2, max_delay)  # Exponential backoff, cap at max_delay

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

def summarize_with_anthropic(document_text, tables):
    full_text = document_text + "\n" + "\n".join([pd.read_csv(StringIO(table)).to_string(index=False, header=False) for table in tables])
    try:
        message = client.messages.create(
            model="claude-3-opus-20240229",
            max_tokens=350,
            temperature=1,
            system=f"Data contents: {full_text}. You will summarize this content in a detailed report. Be precise, and avoid errors.",
            messages=[
                {
                    "role": "user",
                    "content": "Please summarize the provided text and tables thoroughly."
                }
            ]
        )
        if hasattr(message, 'content') and isinstance(message.content, list):
            response_text = '\n'.join(block.text for block in message.content if block.type == 'text')
        else:
            response_text = "Unexpected response format or no match found."
        return response_text
    except Exception as e:
        return f"Error in summarization: {str(e)}"

def main():
    st.title('Amazon Textract Document Processing')
    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg'])
    bucket_name = 'streamlit-bucket-1'
    
    # Reset session state for a new file
    if uploaded_file is not None and ('uploaded_file_name' not in st.session_state or st.session_state.uploaded_file_name != uploaded_file.name):
        # Clear previous states
        for key in ['document_text', 'tables', 'summary', 'uploaded_file_name']:
            if key in st.session_state:
                del st.session_state[key]
        
    process_button_pressed = st.button('Process Document')
    
    if process_button_pressed or (uploaded_file is not None and 'document_text' in st.session_state and 'tables' in st.session_state):
        if 'document_text' not in st.session_state:
            s3_object = upload_to_s3(uploaded_file, bucket_name, uploaded_file.name)
            if s3_object:
                job_id = start_job(s3_object)
                if job_id:
                    results_pages = get_job_results(job_id)
                    if results_pages:
                        document_text, tables, form_fields = process_document(results_pages)
                        # Store in session state to avoid reprocessing on refresh or after summarizing
                        st.session_state.document_text = document_text
                        st.session_state.tables = tables
                        st.session_state.uploaded_file_name = uploaded_file.name
                    else:
                        st.error("Document processing failed or did not complete successfully.")
                        return  # Exit early if processing failed
        # At this point, we have the document processed, either from this run or a previous one
        if 'document_text' in st.session_state:
            st.subheader("Extracted Text")
            st.text_area('Extracted Text', st.session_state.document_text, height=150)
            if st.session_state.tables:
                st.subheader("Extracted Tables")
                for i, table_csv in enumerate(st.session_state.tables, start=1):
                    st.write(f"Table {i}:")
                    df = pd.read_csv(StringIO(table_csv))
                    st.dataframe(df)
        if 'summary' not in st.session_state:
            summarize_button = st.button('Summarize')
            if summarize_button and 'document_text' in st.session_state:
                with st.spinner("Summarizing..."):
                    summary = summarize_with_anthropic(st.session_state.document_text, st.session_state.tables)
                    st.session_state.summary = summary  # Store the summary for accessing after rerun
    if 'summary' in st.session_state:
        st.subheader("Summary")
        st.markdown(st.session_state.summary)

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
