import streamlit as st
import boto3
import pandas as pd

# Function to initialise the boto3 Textract client using Streamlit secrets
def initialise_textract_client():
    return boto3.client(
        'textract',
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
        region_name=st.secrets["AWS_REGION"]
    )

# Process file and extract text
def extract_text(textract_client, file_content):
    response = textract_client.detect_document_text(Document={'Bytes': file_content})
    lines = [item['Text'] for item in response['Blocks'] if item['BlockType'] == 'LINE']
    return '\n'.join(lines)

# Process file and extract tables
def extract_tables(textract_client, file_content):
    response = textract_client.analyze_document(Document={'Bytes': file_content}, FeatureTypes=['TABLES'])
    
    # Collecting the information about the tables
    blocks = response['Blocks']
    tables = {}
    for block in blocks:
        if block['BlockType'] == 'TABLE':
            table_id = block['Id']
            tables[table_id] = []
        if block['BlockType'] == 'CELL':
            table_id = block['Page']['Table']['Id']
            row_index = block['RowIndex'] - 1
            col_index = block['ColumnIndex'] - 1
            text = block.get('Text', '')
            
            # Make sure the row has enough columns
            while len(tables[table_id]) <= row_index:
                tables[table_id].append([])
            while len(tables[table_id][row_index]) <= col_index:
                tables[table_id][row_index].append('')
                
            tables[table_id][row_index][col_index] = text
            
    # Convert the dictionary of tables to a list of DataFrame objects
    dataframes = []
    for table_id, rows in tables.items():
        df = pd.DataFrame(rows)
        dataframes.append(df)
    
    return dataframes

# Main Streamlit app
def main():
    st.title('AWS Textract Document Processor')

    textract_client = initialise_textract_client()
    uploaded_file = st.file_uploader("Choose a file to process", type=['pdf', 'png', 'jpg', 'jpeg', 'tiff'])
    
    if uploaded_file:
        feature_type = st.radio("Feature type", ['Text', 'Tables'])
        if st.button('Process'):
            file_content = uploaded_file.read()
            if feature_type == 'Text':
                text = extract_text(textract_client, file_content)
                st.subheader('Extracted Text')
                st.write(text)
            else:
                tables = extract_tables(textract_client, file_content)
                st.subheader('Extracted Tables')
                for i, df in enumerate(tables, start=1):
                    st.write(f"Table {i}")
                    st.dataframe(df)

if __name__ == "__main__":
    main()
