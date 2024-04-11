import streamlit as st
import boto3
import time
import pandas as pd
from io import BytesIO

# Retrieve AWS credentials from Streamlit secrets (ensure these are set up in your Streamlit app settings)
AWS_ACCESS_KEY_ID = st.secrets['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']
AWS_REGION_NAME = st.secrets['AWS_REGION_NAME']

# Initialize the boto3 client for Amazon Textract
textract_client = boto3.client('textract',
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                               region_name=AWS_REGION_NAME)

# Function to process uploaded file and start the Textract job
def process_file(uploaded_file):
    try:
        # Convert uploaded file to bytes
        content = uploaded_file.getvalue()
        response = textract_client.analyze_document(
            Document={'Bytes': content},
            FeatureTypes=['FORMS', 'TABLES']
        )
        return response
    except Exception as e:
        st.error(f"Error processing file with Textract: {str(e)}")
        return None

# Function to extract text from the Textract response
def extract_text(response):
    text = ""
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            text += item['Text'] + '\n'
    return text

# Main app function
def main():
    st.title('Amazon Textract OCR Application')
    
    uploaded_file = st.file_uploader("Upload a PDF file", type=['pdf'])
    
    if uploaded_file is not None:
        with st.spinner('Processing...'):
            response = process_file(uploaded_file)
            if response:
                extracted_text = extract_text(response)
                st.subheader("Extracted Text")
                st.text_area("Extracted Content", extracted_text, height=300)

if __name__ == '__main__':
    main()
