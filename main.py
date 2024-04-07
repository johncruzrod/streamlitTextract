import time
import boto3
import streamlit as st
from botocore.exceptions import NoCredentialsError

# Initialise the boto3 client for Textract in an async way
def initialise_textract_client():
    return boto3.client(
        'textract',
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
        region_name=st.secrets["AWS_REGION_NAME"]
    )

# Start an asynchronous job to process the document
def start_textract_job(textract_client, file_content, feature_type):
    if feature_type == 'text':
        response = textract_client.start_document_text_detection(Document={'Bytes': file_content})
    elif feature_type == 'tables':
        response = textract_client.start_document_analysis(Document={'Bytes': file_content}, FeatureTypes=['TABLES'])
    return response['JobId']

# Check the status of the Textract job
def is_job_complete(textract_client, job_id):
    response = textract_client.get_document_analysis(JobId=job_id)
    status = response['JobStatus']
    return status == 'SUCCEEDED'

# Get the results of the Textract job
def get_job_results(textract_client, job_id):
    response = textract_client.get_document_analysis(JobId=job_id)
    return response

# Streamlit app layout
def main():
    st.title('AWS Textract Document Processing (Asynchronous)')
    
    textract_client = initialise_textract_client()
    
    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg', 'tiff'])
    
    if uploaded_file is not None:
        feature_type = st.selectbox("Choose the feature", ('text', 'tables'))
        if st.button('Process Document'):
            try:
                # Start the Textract job
                job_id = start_textract_job(textract_client, uploaded_file.read(), feature_type)
                st.write(f"Started job {job_id}, waiting for the results...")
                
                # Wait for the Textract job to complete
                while not is_job_complete(textract_client, job_id):
                    time.sleep(5)
                
                # Retrieve the results
                result = get_job_results(textract_client, job_id)
                st.write(result) # Here, you would format and display the result as needed
                
            except NoCredentialsError:
                st.error("Could not authenticate with AWS. Check your credentials.")
            except Exception as e:
                st.error(f"Error processing file: {e}")

if __name__ == "__main__":
    main()
