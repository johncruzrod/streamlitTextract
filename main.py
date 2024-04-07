import streamlit as st
import boto3

# Retrieve AWS credentials from Streamlit secrets
AWS_ACCESS_KEY_ID = st.secrets['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = st.secrets['AWS_SECRET_ACCESS_KEY']
AWS_REGION_NAME = st.secrets['AWS_REGION_NAME']

# Initialize boto3 clients
textract_client = boto3.client('textract', 
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                               region_name=AWS_REGION_NAME)

def extract_text(file):
    response = textract_client.detect_document_text(Document={'Bytes': file.read()})
    text = ''
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            text += item['Text'] + '\n'
    return text

def extract_tables(file):
    response = textract_client.analyze_document(Document={'Bytes': file.read()}, FeatureTypes=['TABLES'])
    tables = []
    for table in response['Blocks']:
        if table['BlockType'] == 'TABLE':
            tables.append(table)
    return tables

def main():
    st.title('Amazon Textract File Processing')
    
    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg'])
    
    if uploaded_file is not None:
        option = st.radio('Select processing option', ('Extract Text', 'Extract Tables'))
        
        if option == 'Extract Text':
            text = extract_text(uploaded_file)
            st.write(text)
        else:
            tables = extract_tables(uploaded_file)
            for table in tables:
                st.write(table)

if __name__ == '__main__':
    main()
