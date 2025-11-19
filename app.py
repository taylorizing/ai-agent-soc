import streamlit as st
import pandas as pd
import io
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError

# Page configuration
st.set_page_config(
    page_title="Unity Catalog File Upload",
    page_icon="📁",
    layout="wide"
)

# Initialize Databricks Workspace Client
@st.cache_resource
def get_workspace_client():
    """Initialize and cache the Databricks Workspace Client"""
    try:
        return WorkspaceClient()
    except Exception as e:
        st.error(f"Failed to initialize Databricks Workspace Client: {e}")
        return None

w = get_workspace_client()

# Title and Description
st.title("🚀 Databricks Unity Catalog File Upload")
st.markdown("""
Upload files from your local machine to Unity Catalog volumes using the Databricks SDK.
This app uses your current Databricks credentials to securely upload files.
""")

# Main Upload Section
st.header("📁 Upload File to Unity Catalog Volume")

# Create two columns for better layout
col1, col2 = st.columns([2, 1])

with col1:
    uploaded_file = st.file_uploader(
        "Select file to upload",
        help="Choose any file from your local machine"
    )

with col2:
    if uploaded_file:
        st.metric("File Size", f"{uploaded_file.size / 1024:.2f} KB")
        st.metric("File Type", uploaded_file.type or "unknown")

# Unity Catalog Path Input
st.subheader("🎯 Specify Destination Path")

# Provide example and help text
st.markdown("""
Enter the Unity Catalog volume path in the format: `catalog.schema.volume_name`

**Examples:**
- `main.default.my_volume`
- `users.jason_taylor.agent_app_uploads`
- `prod.data.raw_files`
""")

upload_volume_path = st.text_input(
    "Unity Catalog Volume Path",
    value="users.jason_taylor.agent_app_uploads",
    placeholder="catalog.schema.volume_name",
    help="Format: catalog.schema.volume_name"
)

# Optional subfolder path
subfolder = st.text_input(
    "Subfolder (optional)",
    value="",
    placeholder="e.g., uploads/2024",
    help="Optional subfolder within the volume"
)

# Upload Button
st.divider()

if uploaded_file and upload_volume_path:
    # Construct full file path
    if subfolder:
        file_path = f"/Volumes/{upload_volume_path}/{subfolder}/{uploaded_file.name}"
    else:
        file_path = f"/Volumes/{upload_volume_path}/{uploaded_file.name}"
    
    # Display where file will be uploaded
    st.info(f"📍 **Destination:** `{file_path}`")
    
    # Upload button
    if st.button("🔼 Upload File", type="primary", use_container_width=True):
        if w is None:
            st.error("❌ Workspace client not initialized. Please check your Databricks credentials.")
        else:
            with st.spinner(f"Uploading {uploaded_file.name}..."):
                try:
                    # Read file content into BytesIO buffer
                    file_content = uploaded_file.read()
                    buffer = io.BytesIO(file_content)
                    
                    # Upload to Unity Catalog volume using Databricks SDK
                    w.files.upload(path=file_path, contents=buffer, overwrite=True)
                    
                    # Success message
                    st.success(f"✅ File successfully uploaded to Unity Catalog!")
                    st.balloons()
                    
                    # Display upload details
                    with st.expander("📋 Upload Details", expanded=True):
                        st.write(f"**File Name:** {uploaded_file.name}")
                        st.write(f"**File Size:** {len(file_content):,} bytes ({len(file_content) / 1024:.2f} KB)")
                        st.write(f"**Destination Path:** `{file_path}`")
                        st.write(f"**Volume:** {upload_volume_path}")
                        
                except DatabricksError as e:
                    st.error(f"❌ Databricks Error: {e}")
                    with st.expander("🔍 Error Details"):
                        st.write(f"**Error Type:** DatabricksError")
                        st.write(f"**Message:** {str(e)}")
                        st.write("**Common Issues:**")
                        st.write("- Check that the Unity Catalog volume exists")
                        st.write("- Verify you have WRITE permissions on the volume")
                        st.write("- Ensure the path format is correct: catalog.schema.volume_name")
                        
                except Exception as e:
                    st.error(f"❌ Upload Failed: {e}")
                    with st.expander("🔍 Error Details"):
                        st.write(f"**Error Type:** {type(e).__name__}")
                        st.write(f"**Message:** {str(e)}")

elif not uploaded_file:
    st.warning("⚠️ Please select a file to upload")
elif not upload_volume_path:
    st.warning("⚠️ Please specify a Unity Catalog volume path")

# Instructions Section
st.divider()
st.header("📖 Instructions")

with st.expander("How to use this app", expanded=False):
    st.markdown("""
    ### Step-by-Step Guide:
    
    1. **Select a File**: Click "Browse files" to choose a file from your local machine
    2. **Enter Volume Path**: Specify the Unity Catalog volume path in the format `catalog.schema.volume_name`
    3. **Optional Subfolder**: Add a subfolder path if you want to organize files
    4. **Upload**: Click the "Upload File" button to start the upload
    5. **Confirmation**: You'll see a success message when the upload completes
    
    ### Permissions Required:
    
    - READ permission on the specified Unity Catalog catalog and schema
    - WRITE permission on the target volume
    - USE CATALOG permission on the catalog
    - USE SCHEMA permission on the schema
    
    ### Troubleshooting:
    
    - **Authentication Error**: Ensure you're logged into Databricks and have valid credentials
    - **Permission Error**: Contact your Databricks administrator to grant volume write access
    - **Path Not Found**: Verify the catalog, schema, and volume exist and the path is correct
    """)

# Sample Data Visualization Section
st.divider()
st.header("📊 Sample Data Visualization")

# Create sample data
chart_data = pd.DataFrame({
    'Day': [f'Day {x}' for x in range(1, 31)],
    'Uploads': [2 ** (x * 0.3) for x in range(30)]
})

# Display chart
st.line_chart(chart_data.set_index('Day'))

# Display data table
with st.expander("View Data Table"):
    st.dataframe(chart_data, use_container_width=True)
