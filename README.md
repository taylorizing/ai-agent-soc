# Databricks Unity Catalog File Upload & AI Parse App

A professional Streamlit application for uploading files to Databricks Unity Catalog volumes with built-in AI document parsing capabilities using Databricks AI functions.

## Features

- 📁 **File Upload**: Upload files from your local machine to Unity Catalog volumes
- 🤖 **AI Document Parsing**: Automatic document parsing using Databricks `ai_parse_document` function
- 📊 **Data Export**: View and download extracted content as CSV
- 🎨 **Professional UI**: Styled to match CLA Connect branding
- 🔒 **Secure**: Uses Databricks SDK with your credentials

## Prerequisites

1. **Databricks Workspace** with:
   - Unity Catalog enabled
   - SQL Warehouse configured
   - AI/Foundation Model APIs enabled

2. **Unity Catalog Permissions**:
   - READ permission on catalog and schema
   - WRITE permission on target volume
   - USE CATALOG and USE SCHEMA permissions

## Setup Instructions

### 1. Configure SQL Warehouse

For AI document parsing to work, you need to configure a SQL Warehouse ID:

1. Go to **Databricks SQL** > **SQL Warehouses**
2. Select or create a SQL Warehouse
3. Copy the **Warehouse ID** from the warehouse details
4. Update `app.yaml` with your Warehouse ID:

```yaml
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: "your-warehouse-id-here"
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Application Locally

```bash
streamlit run app.py
```

### 4. Deploy to Databricks Apps

Use the Databricks CLI or UI to deploy the app using the included `app.yaml` configuration.

## Usage

1. **Select a File**: Choose a file from your local machine
2. **Enter Volume Path**: Specify the Unity Catalog path (format: `catalog.schema.volume_name`)
3. **Optional Subfolder**: Add a subfolder path for organization
4. **Upload**: Click the upload button
5. **AI Parsing**: If the file is a PDF or image, Databricks AI will automatically parse the document
6. **Download Results**: Export parsed content as CSV

## Supported File Types for AI Parsing

- PDF (`.pdf`)
- PNG (`.png`)
- JPEG (`.jpg`, `.jpeg`)
- TIFF (`.tiff`)
- BMP (`.bmp`)

## Configuration

### Environment Variables

- `DATABRICKS_WAREHOUSE_ID`: (Required for AI parsing) The ID of your SQL Warehouse

### app.yaml Structure

```yaml
command: ["streamlit", "run", "app.py"]

env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: "YOUR_WAREHOUSE_ID_HERE"
```

## How It Works

### File Upload
1. Files are uploaded to Unity Catalog volumes using the Databricks SDK
2. The `WorkspaceClient` handles authentication with your Databricks credentials
3. Files are saved to the specified volume path

### AI Document Parsing
1. After upload, the app calls `ai_parse_document()` via SQL Execution API
2. The function uses Databricks AI to extract text and structure
3. Results are converted to a pandas DataFrame for display
4. Users can view and download the parsed content

## Troubleshooting

### Upload Errors

- **Authentication Error**: Ensure valid Databricks credentials
- **Permission Error**: Contact your admin for volume write access
- **Path Not Found**: Verify the catalog, schema, and volume exist

### AI Parsing Errors

- **SQL Warehouse not configured**: Set `DATABRICKS_WAREHOUSE_ID` in `app.yaml`
- **AI function not available**: Ensure your workspace has AI functions enabled
- **Permission Error**: Verify access to SQL Warehouse and AI APIs

## Technical Stack

- **Streamlit**: Web application framework
- **Databricks SDK**: For Unity Catalog and SQL Execution API
- **Pandas**: Data manipulation and display
- **Databricks AI**: Document parsing with `ai_parse_document`

## License

MIT License