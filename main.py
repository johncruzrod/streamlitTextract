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

def extract_text(file_content):
    try:
        response = textract_client.detect_document_text(Document={'Bytes': file_content})
        text = ''
        for item in response['Blocks']:
            if item['BlockType'] == 'LINE':
                text += item['Text'] + '\n'
        return text
    except boto3.exceptions.Boto3Error as e:
        st.error(f"An error occurred: {e}")
        return None

def extract_tables(file_content):
    try:
        response = textract_client.analyze_document(Document={'Bytes': file_content}, FeatureTypes=['TABLES'])
        tables = []
        for table in response['Blocks']:
            if table['BlockType'] == 'TABLE':
                tables.append(table)
        return tables
    except boto3.exceptions.Boto3Error as e:
        st.error(f"An error occurred: {e}")
        return None

def main():
    st.title('Amazon Textract File Processing')
    
    uploaded_file = st.file_uploader("Choose a file", type=['pdf', 'png', 'jpg', 'jpeg'])
    
    if uploaded_file is not None:
        # Read file content once
        file_content = uploaded_file.getvalue()
        
        option = st.radio('Select processing option', ('Extract Text', 'Extract Tables'))
        
        if st.button('Process File'):
            if option == 'Extract Text':
                text = extract_text(file_content)
                if text is not None:
                    st.write(text)
            else:
                tables = extract_tables(file_content)
                if tables is not None:
                    for table in tables:
                        st.write(table)

if __name__ == '__main__':
    main()
