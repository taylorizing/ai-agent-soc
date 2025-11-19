import pandas as pd
import streamlit as st
import os
from pathlib import Path

# Unity Catalog volume path
UPLOAD_FOLDER = '/Volumes/users/jason_taylor/agent_app_uploads'

# Ensure upload directory exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Page configuration
st.set_page_config(
    page_title="File Upload App",
    page_icon="📁",
    layout="wide"
)

# Title
st.title("Hello, World! 👋")

# File Upload Section
st.header("📁 Upload File to Unity Catalog")
st.info(f"**Upload Location:** `{UPLOAD_FOLDER}`")

uploaded_file = st.file_uploader(
    "Choose a file to upload",
    type=['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'csv', 'xlsx', 'doc', 'docx'],
    help="Allowed file types: txt, pdf, png, jpg, jpeg, gif, csv, xlsx, doc, docx"
)

if uploaded_file is not None:
    # Save the uploaded file
    file_path = os.path.join(UPLOAD_FOLDER, uploaded_file.name)
    
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    st.success(f'✅ File "{uploaded_file.name}" uploaded successfully to Unity Catalog!')
    
    # Show file details
    st.write(f"**Filename:** {uploaded_file.name}")
    st.write(f"**File size:** {uploaded_file.size} bytes")
    st.write(f"**File type:** {uploaded_file.type}")

# Display uploaded files
st.header("📂 Uploaded Files")

if os.path.exists(UPLOAD_FOLDER):
    uploaded_files = [f for f in os.listdir(UPLOAD_FOLDER) if os.path.isfile(os.path.join(UPLOAD_FOLDER, f))]
    
    if uploaded_files:
        st.write(f"Total files: **{len(uploaded_files)}**")
        
        # Display as a dataframe for better presentation
        file_info = []
        for filename in uploaded_files:
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            file_size = os.path.getsize(filepath)
            file_info.append({
                'Filename': filename,
                'Size (KB)': f"{file_size / 1024:.2f}"
            })
        
        files_df = pd.DataFrame(file_info)
        st.dataframe(files_df, use_container_width=True)
    else:
        st.info("No files uploaded yet.")
else:
    st.warning("Upload folder does not exist.")

# Chart Data Section
st.header("📊 Chart Data")

chart_data = pd.DataFrame({
    'Apps': [x for x in range(30)],
    'Fun with data': [2 ** x for x in range(30)]
})

# Display the dataframe
st.dataframe(chart_data, use_container_width=True)

# Add a line chart visualization
st.subheader("📈 Data Visualization")
st.line_chart(chart_data.set_index('Apps'))
