import streamlit as st
import io
import os
import pandas as pd
import json

# Try to import Databricks SDK
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import DatabricksError
    DATABRICKS_SDK_AVAILABLE = True
except ImportError as e:
    DATABRICKS_SDK_AVAILABLE = False
    IMPORT_ERROR = str(e)

# No longer need PySpark - using Databricks SDK SQL execution instead

# Page configuration
st.set_page_config(
    page_title="Unity Catalog File Upload",
    page_icon="📁",
    layout="wide"
)

# Load and inject custom CSS matching CLA Connect style guide
def load_css():
    """Load custom CSS matching CLA Connect design"""
    cla_css = """
    <style>
        /* Import Roboto font */
        @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');
        
        /* Global styles matching CLA Connect */
        .stApp {
            background-color: #ffffff;
            font-family: 'Roboto', 'Helvetica', 'Arial', sans-serif;
        }
        
        /* Main content area */
        .main .block-container {
            padding: 2rem 3rem;
            max-width: 1200px;
        }
        
        /* Headers - CLA teal color */
        h1, h2, h3 {
            color: #004d40 !important;
            font-family: 'Roboto', sans-serif !important;
            font-weight: 700 !important;
        }
        
        h1 {
            font-size: 2.5rem !important;
            margin-bottom: 1rem !important;
            padding-bottom: 0.5rem !important;
            border-bottom: 3px solid #004d40 !important;
        }
        
        h2 {
            font-size: 2rem !important;
            margin-top: 2rem !important;
            margin-bottom: 1rem !important;
        }
        
        h3 {
            font-size: 1.5rem !important;
            margin-bottom: 0.75rem !important;
        }
        
        /* Paragraph text */
        p, .stMarkdown {
            color: #333333;
            font-size: 1rem;
            line-height: 1.6;
        }
        
        /* Buttons - CLA orange action button (matching insights page) */
        .stButton > button {
            background-color: #FF6B35 !important;
            color: #ffffff !important;
            border: none !important;
            padding: 0.75rem 2rem !important;
            font-size: 1rem !important;
            font-weight: 500 !important;
            border-radius: 4px !important;
            transition: all 0.3s ease !important;
            font-family: 'Roboto', sans-serif !important;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .stButton > button:hover {
            background-color: #E55A28 !important;
            border: none !important;
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(255, 107, 53, 0.3) !important;
        }
        
        .stButton > button:focus {
            background-color: #E55A28 !important;
            box-shadow: 0 0 0 0.2rem rgba(255, 107, 53, 0.3) !important;
        }
        
        .stButton > button:active {
            transform: translateY(0);
            box-shadow: 0 2px 4px rgba(255, 107, 53, 0.3) !important;
        }
        
        /* File uploader */
        .stFileUploader {
            background-color: #f5f5f5;
            border: 2px dashed #004d40;
            border-radius: 8px;
            padding: 1.5rem;
        }
        
        .stFileUploader label {
            color: #004d40 !important;
            font-weight: 500 !important;
        }
        
        /* Text inputs */
        .stTextInput > div > div > input {
            border: 2px solid #e0e0e0 !important;
            border-radius: 4px !important;
            padding: 0.75rem !important;
            font-family: 'Roboto', sans-serif !important;
            transition: border-color 0.3s ease !important;
        }
        
        .stTextInput > div > div > input:focus {
            border-color: #004d40 !important;
            box-shadow: 0 0 0 0.2rem rgba(0, 77, 64, 0.1) !important;
        }
        
        .stTextInput > label {
            color: #004d40 !important;
            font-weight: 500 !important;
        }
        
        /* Metrics */
        .stMetric {
            background-color: #f5f5f5;
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #004d40;
        }
        
        .stMetric label {
            color: #757575 !important;
            font-size: 0.875rem !important;
            font-weight: 500 !important;
        }
        
        .stMetric [data-testid="stMetricValue"] {
            color: #004d40 !important;
            font-size: 1.5rem !important;
            font-weight: 700 !important;
        }
        
        /* Info, Warning, Success, Error boxes */
        .stAlert {
            border-radius: 8px !important;
            border-left: 4px solid;
        }
        
        /* Info boxes */
        div[data-baseweb="notification"] {
            border-radius: 8px !important;
        }
        
        .stInfo {
            background-color: #e8f4f8 !important;
            border-left-color: #004d40 !important;
        }
        
        .stSuccess {
            background-color: #e8f5e9 !important;
            border-left-color: #2e7d32 !important;
        }
        
        .stWarning {
            background-color: #fff3e0 !important;
            border-left-color: #f57c00 !important;
        }
        
        .stError {
            background-color: #ffebee !important;
            border-left-color: #c62828 !important;
        }
        
        /* Divider */
        hr {
            border: none;
            border-top: 2px solid #e0e0e0;
            margin: 2rem 0;
        }
        
        /* Expander */
        .streamlit-expanderHeader {
            background-color: #f5f5f5 !important;
            border-radius: 4px !important;
            color: #004d40 !important;
            font-weight: 500 !important;
        }
        
        .streamlit-expanderHeader:hover {
            background-color: #eeeeee !important;
        }
        
        /* Columns */
        [data-testid="column"] {
            padding: 0.5rem;
        }
        
        /* Spinner */
        .stSpinner > div {
            border-top-color: #004d40 !important;
        }
        
        /* Hide Streamlit branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        
        /* Responsive design */
        @media (max-width: 768px) {
            .main .block-container {
                padding: 1rem;
            }
            
            h1 {
                font-size: 2rem !important;
            }
            
            h2 {
                font-size: 1.5rem !important;
            }
            
            .stButton > button {
                padding: 0.5rem 1rem !important;
                font-size: 0.875rem !important;
            }
        }
    </style>
    """
    st.markdown(cla_css, unsafe_allow_html=True)

# Load custom styles
load_css()

def parse_document_with_ai(file_path, workspace_client):
    """Parse document using Databricks AI parse_document function via SQL Execution API"""
    try:
        if not DATABRICKS_SDK_AVAILABLE:
            return None, "Databricks SDK not available"
        
        # Get warehouse ID from environment or use default
        # You can set this in your app configuration
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
        
        if not warehouse_id:
            return None, "SQL Warehouse ID not configured. Please set DATABRICKS_WAREHOUSE_ID environment variable."
        
        # Use Databricks AI parse_document function via SQL execution
        query = f"""
        SELECT ai_parse_document('{file_path}') as parsed_content
        """
        
        # Execute SQL query using the workspace client
        from databricks.sdk.service.sql import StatementState
        
        statement = workspace_client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="30s"
        )
        
        # Wait for completion
        if statement.status.state == StatementState.SUCCEEDED:
            # Get the result
            if statement.result and statement.result.data_array:
                data = statement.result.data_array
                
                if data and len(data) > 0:
                    # Extract parsed content from first row
                    parsed_content = data[0][0] if data[0] else None
                    
                    if parsed_content:
                        # Convert parsed content to a structured format for display
                        try:
                            if isinstance(parsed_content, str):
                                # Try to parse as JSON first
                                try:
                                    parsed_json = json.loads(parsed_content)
                                    # If it's a list of dicts, create dataframe
                                    if isinstance(parsed_json, list):
                                        df = pd.DataFrame(parsed_json)
                                    elif isinstance(parsed_json, dict):
                                        df = pd.DataFrame([parsed_json])
                                    else:
                                        # Plain text - split by lines
                                        lines = parsed_content.split('\n')
                                        df = pd.DataFrame({'Content': lines})
                                except json.JSONDecodeError:
                                    # Not JSON, treat as plain text
                                    lines = parsed_content.split('\n')
                                    df = pd.DataFrame({'Content': lines})
                            else:
                                # If it's already structured, convert to dataframe
                                df = pd.DataFrame([parsed_content])
                            
                            return df, None
                        except Exception as e:
                            # Fallback: return as single column dataframe
                            df = pd.DataFrame({'Parsed_Text': [str(parsed_content)]})
                            return df, None
                    else:
                        return None, "No content returned from ai_parse_document"
                else:
                    return None, "No data returned from query"
            else:
                return None, "Query returned no results"
        else:
            error_msg = statement.status.error.message if statement.status.error else "Unknown error"
            return None, f"SQL execution failed: {error_msg}"
        
    except Exception as e:
        return None, str(e)

# Title and Description
st.markdown("""
    <div style="margin-bottom: 2rem;">
        <h1 style="color: #004d40; font-size: 2.5rem; font-weight: 700; margin-bottom: 0.5rem; border-bottom: 3px solid #004d40; padding-bottom: 0.5rem;">
            Databricks Unity Catalog File Upload
        </h1>
        <p style="color: #757575; font-size: 1.1rem; margin-top: 1rem; line-height: 1.6;">
            Upload files from your local machine to Unity Catalog volumes using the Databricks SDK.
            This app uses your current Databricks credentials to securely upload files.
        </p>
    </div>
""", unsafe_allow_html=True)

# Display SDK availability status
warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")

col_status1, col_status2 = st.columns(2)

with col_status1:
    if not DATABRICKS_SDK_AVAILABLE:
        st.warning(f"⚠️ Databricks SDK not available")
    else:
        st.success("✅ Databricks SDK is available")

with col_status2:
    if warehouse_id and warehouse_id != "YOUR_WAREHOUSE_ID_HERE":
        st.success("✅ SQL Warehouse configured")
    else:
        st.warning("⚠️ SQL Warehouse not configured")
        with st.expander("Configure SQL Warehouse"):
            st.markdown("""
            <div style="font-family: 'Roboto', sans-serif; color: #333333;">
                <p>AI document parsing requires a SQL Warehouse ID to be configured.</p>
                <p style="margin-top: 1rem;"><strong style="color: #004d40;">Setup:</strong></p>
                <ol>
                    <li>Set <code style="background-color: #e0e0e0; padding: 0.2rem 0.5rem; border-radius: 3px;">DATABRICKS_WAREHOUSE_ID</code> in your <code>app.yaml</code></li>
                    <li>Find your Warehouse ID in Databricks SQL > SQL Warehouses</li>
                    <li>Redeploy the app after updating the configuration</li>
                </ol>
            </div>
            """, unsafe_allow_html=True)

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
<div style="background-color: #f5f5f5; padding: 1rem; border-radius: 8px; border-left: 4px solid #004d40; margin-bottom: 1rem;">
    <p style="margin: 0; color: #333333;">
        Enter the Unity Catalog volume path in the format: <code style="background-color: #e0e0e0; padding: 0.2rem 0.5rem; border-radius: 3px;">catalog.schema.volume_name</code>
    </p>
    <p style="margin-top: 0.75rem; margin-bottom: 0.5rem; color: #004d40; font-weight: 500;">Examples:</p>
    <ul style="margin: 0; color: #757575;">
        <li><code style="background-color: #e0e0e0; padding: 0.2rem 0.5rem; border-radius: 3px;">main.default.my_volume</code></li>
        <li><code style="background-color: #e0e0e0; padding: 0.2rem 0.5rem; border-radius: 3px;">users.jason_taylor.agent_app_uploads</code></li>
        <li><code style="background-color: #e0e0e0; padding: 0.2rem 0.5rem; border-radius: 3px;">prod.data.raw_files</code></li>
    </ul>
</div>
""", unsafe_allow_html=True)

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
    # Parse the volume path (catalog.schema.volume_name)
    try:
        parts = upload_volume_path.split('.')
        if len(parts) != 3:
            st.error("❌ Invalid volume path format. Please use: catalog.schema.volume_name")
            st.stop()
        
        catalog, schema, volume_name = parts
        
        # Construct full file path with proper Unity Catalog format
        if subfolder:
            file_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{subfolder}/{uploaded_file.name}"
        else:
            file_path = f"/Volumes/{catalog}/{schema}/{volume_name}/{uploaded_file.name}"
    except Exception as e:
        st.error(f"❌ Error parsing volume path: {e}")
        st.stop()
    
    # Display where file will be uploaded
    st.info(f"📍 **Destination:** `{file_path}`")
    
    # Upload button
    if st.button("🔼 Upload File", type="primary", use_container_width=True):
        if not DATABRICKS_SDK_AVAILABLE:
            st.error("❌ Databricks SDK not available. Please install databricks-sdk.")
        else:
            with st.spinner(f"Uploading {uploaded_file.name}..."):
                try:
                    # Initialize workspace client only when needed
                    w = WorkspaceClient()
                    
                    # Read file content into BytesIO buffer
                    file_content = uploaded_file.read()
                    buffer = io.BytesIO(file_content)
                    
                    # Upload to Unity Catalog volume using Databricks SDK
                    w.files.upload(file_path=file_path, contents=buffer, overwrite=True)
                    
                    # Success message
                    st.success(f"✅ File successfully uploaded to Unity Catalog!")
                    st.balloons()
                    
                    # Display upload details
                    with st.expander("📋 Upload Details", expanded=True):
                        st.write(f"**File Name:** {uploaded_file.name}")
                        st.write(f"**File Size:** {len(file_content):,} bytes ({len(file_content) / 1024:.2f} KB)")
                        st.write(f"**Destination Path:** `{file_path}`")
                        st.write(f"**Volume:** {upload_volume_path}")
                    
                    # AI Document Parsing Section
                    st.divider()
                    st.subheader("🤖 AI Document Parsing")
                    
                    # Check if file is a PDF or image
                    file_extension = uploaded_file.name.lower().split('.')[-1]
                    supported_formats = ['pdf', 'png', 'jpg', 'jpeg', 'tiff', 'bmp']
                    
                    if file_extension in supported_formats:
                        with st.spinner("Parsing document with Databricks AI..."):
                            try:
                                parsed_result, parse_error = parse_document_with_ai(file_path, w)
                                
                                if parse_error:
                                    st.warning(f"⚠️ Document parsing encountered an issue: {parse_error}")
                                    with st.expander("ℹ️ Setup Instructions"):
                                        st.markdown("""
                                        <div style="font-family: 'Roboto', sans-serif; color: #333333;">
                                            <p>This app uses Databricks' built-in <code style="background-color: #e0e0e0; padding: 0.2rem 0.5rem; border-radius: 3px;">ai_parse_document</code> function to extract text and structure from documents.</p>
                                            <p style="margin-top: 1rem;"><strong style="color: #004d40;">Requirements:</strong></p>
                                            <ul>
                                                <li>A Databricks SQL Warehouse for query execution</li>
                                                <li>Databricks Runtime with AI functions enabled</li>
                                                <li>Access to Databricks AI/Foundation Model APIs</li>
                                                <li>Environment variable <code style="background-color: #e0e0e0; padding: 0.2rem 0.5rem; border-radius: 3px;">DATABRICKS_WAREHOUSE_ID</code> must be set</li>
                                            </ul>
                                            <p style="margin-top: 1rem;"><strong style="color: #004d40;">How to set the Warehouse ID:</strong></p>
                                            <ol>
                                                <li>Go to your Databricks SQL Warehouses</li>
                                                <li>Copy the Warehouse ID from your SQL Warehouse</li>
                                                <li>Set it as an environment variable in your app configuration</li>
                                            </ol>
                                        </div>
                                        """, unsafe_allow_html=True)
                                elif parsed_result is not None and not parsed_result.empty:
                                    st.success("✅ Document parsed successfully with Databricks AI!")
                                    
                                    # Display parsed results in a table
                                    st.markdown("""
                                    <div style="margin-top: 1rem;">
                                        <h3 style="color: #004d40; font-weight: 600;">📊 Extracted Content</h3>
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                                    # Show dataframe with custom styling
                                    st.dataframe(
                                        parsed_result,
                                        use_container_width=True,
                                        height=400
                                    )
                                    
                                    # Option to download results
                                    csv = parsed_result.to_csv(index=False).encode('utf-8')
                                    st.download_button(
                                        label="📥 Download Parsed Results as CSV",
                                        data=csv,
                                        file_name=f"parsed_{uploaded_file.name}.csv",
                                        mime="text/csv",
                                    )
                                    
                                    # Display summary statistics
                                    with st.expander("📈 Data Summary"):
                                        col1, col2, col3 = st.columns(3)
                                        with col1:
                                            st.metric("Rows", parsed_result.shape[0])
                                        with col2:
                                            st.metric("Columns", parsed_result.shape[1])
                                        with col3:
                                            st.metric("Data Points", parsed_result.shape[0] * parsed_result.shape[1])
                                else:
                                    st.info("ℹ️ No content extracted from the document. The file may be empty or contain no readable text.")
                                    
                            except Exception as e:
                                st.error(f"❌ Document parsing failed: {e}")
                                with st.expander("🔍 Error Details"):
                                    st.write(f"**Error Type:** {type(e).__name__}")
                                    st.write(f"**Message:** {str(e)}")
                                    st.write("**Tip:** Ensure you have access to Databricks AI functions and the workspace is properly configured.")
                    else:
                        st.info(f"ℹ️ AI document parsing is available for PDF and image files. Uploaded file type: `.{file_extension}`")
                        
                except Exception as e:
                    # Check if it's a DatabricksError
                    if DATABRICKS_SDK_AVAILABLE and type(e).__name__ == 'DatabricksError':
                        st.error(f"❌ Databricks Error: {e}")
                        with st.expander("🔍 Error Details"):
                            st.write(f"**Error Type:** DatabricksError")
                            st.write(f"**Message:** {str(e)}")
                            st.write("**Common Issues:**")
                            st.write("- Check that the Unity Catalog volume exists")
                            st.write("- Verify you have WRITE permissions on the volume")
                            st.write("- Ensure the path format is correct: catalog.schema.volume_name")
                    else:
                        # General error handling
                        st.error(f"❌ Upload Failed: {e}")
                        with st.expander("🔍 Error Details"):
                            st.write(f"**Error Type:** {type(e).__name__}")
                            st.write(f"**Message:** {str(e)}")

elif not uploaded_file:
    st.warning("⚠️ Please select a file to upload")
elif not upload_volume_path:
    st.warning("⚠️ Please specify a Unity Catalog volume path")
