import streamlit as st
import io
import os

# Try to import Databricks SDK
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import DatabricksError
    DATABRICKS_SDK_AVAILABLE = True
except ImportError as e:
    DATABRICKS_SDK_AVAILABLE = False
    IMPORT_ERROR = str(e)

# Try to import PySpark for OCR processing
try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

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

# Initialize Spark Session for OCR
@st.cache_resource
def get_spark_session():
    """Initialize and cache Spark session for OCR processing"""
    if not PYSPARK_AVAILABLE:
        return None
    try:
        spark = SparkSession.builder \
            .appName("PDF_OCR_App") \
            .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7") \
            .getOrCreate()
        return spark
    except Exception as e:
        st.error(f"Failed to initialize Spark session: {e}")
        return None

def perform_ocr(file_path):
    """Perform OCR on uploaded file using com.stabrise.spark.pdf"""
    try:
        spark = get_spark_session()
        if spark is None:
            return None, "Spark session not available"
        
        # Read PDF file with OCR using com.stabrise.spark.pdf
        df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file_path)
        
        # For PDF files, use the pdf format
        if file_path.lower().endswith('.pdf'):
            df = spark.read \
                .format("pdf") \
                .option("ocr", "true") \
                .load(file_path)
        
        # Convert to Pandas for display
        pandas_df = df.toPandas()
        return pandas_df, None
        
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
if not DATABRICKS_SDK_AVAILABLE:
    st.warning(f"⚠️ Databricks SDK not available: {IMPORT_ERROR}")
    st.info("Please ensure databricks-sdk is installed: `pip install databricks-sdk`")
else:
    st.success("✅ Databricks SDK is available")

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
                    
                    # OCR Processing Section
                    st.divider()
                    st.subheader("🔍 OCR Processing")
                    
                    # Check if file is a PDF or image
                    file_extension = uploaded_file.name.lower().split('.')[-1]
                    supported_ocr_formats = ['pdf', 'png', 'jpg', 'jpeg', 'tiff', 'bmp']
                    
                    if file_extension in supported_ocr_formats:
                        with st.spinner("Performing OCR on uploaded file..."):
                            try:
                                ocr_result, ocr_error = perform_ocr(file_path)
                                
                                if ocr_error:
                                    st.warning(f"⚠️ OCR processing encountered an issue: {ocr_error}")
                                elif ocr_result is not None and not ocr_result.empty:
                                    st.success("✅ OCR processing completed successfully!")
                                    
                                    # Display OCR results in a table
                                    st.markdown("""
                                    <div style="margin-top: 1rem;">
                                        <h3 style="color: #004d40; font-weight: 600;">📊 Extracted Data</h3>
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                                    # Show dataframe with custom styling
                                    st.dataframe(
                                        ocr_result,
                                        use_container_width=True,
                                        height=400
                                    )
                                    
                                    # Option to download results
                                    csv = ocr_result.to_csv(index=False).encode('utf-8')
                                    st.download_button(
                                        label="📥 Download OCR Results as CSV",
                                        data=csv,
                                        file_name=f"ocr_results_{uploaded_file.name}.csv",
                                        mime="text/csv",
                                    )
                                    
                                    # Display summary statistics
                                    with st.expander("📈 Data Summary"):
                                        col1, col2, col3 = st.columns(3)
                                        with col1:
                                            st.metric("Rows", ocr_result.shape[0])
                                        with col2:
                                            st.metric("Columns", ocr_result.shape[1])
                                        with col3:
                                            st.metric("Data Points", ocr_result.shape[0] * ocr_result.shape[1])
                                else:
                                    st.info("ℹ️ No data extracted from the file. The file may be empty or contain no readable text.")
                                    
                            except Exception as e:
                                st.error(f"❌ OCR processing failed: {e}")
                                with st.expander("🔍 Error Details"):
                                    st.write(f"**Error Type:** {type(e).__name__}")
                                    st.write(f"**Message:** {str(e)}")
                                    st.write("**Tip:** Ensure the com.stabrise.spark.pdf library is properly configured in your Databricks cluster.")
                    else:
                        st.info(f"ℹ️ OCR is only available for PDF and image files. Uploaded file type: `.{file_extension}`")
                        
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
