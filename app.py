import pandas as pd
from flask import Flask, request, redirect, url_for, flash, render_template_string
import logging
import os
from werkzeug.utils import secure_filename

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

flask_app = Flask(__name__)
flask_app.secret_key = 'your-secret-key-here'  # Required for flash messages

# Unity Catalog volume path
UPLOAD_FOLDER = '/Volumes/users/jason_taylor/agent_app_uploads'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'csv', 'xlsx', 'doc', 'docx'}

# Ensure upload directory exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@flask_app.route('/')
def hello_world():
    chart_data = pd.DataFrame({'Apps': [x for x in range(30)],
                               'Fun with data': [2 ** x for x in range(30)]})
    
    # Get list of uploaded files
    uploaded_files = []
    if os.path.exists(UPLOAD_FOLDER):
        uploaded_files = [f for f in os.listdir(UPLOAD_FOLDER) if os.path.isfile(os.path.join(UPLOAD_FOLDER, f))]
    
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>File Upload App</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f5f5;
            }
            .container {
                background-color: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }
            h1 {
                color: #333;
                border-bottom: 3px solid #007bff;
                padding-bottom: 10px;
            }
            h2 {
                color: #555;
                margin-top: 30px;
            }
            .upload-form {
                margin: 20px 0;
                padding: 20px;
                background-color: #f8f9fa;
                border-radius: 5px;
                border: 2px dashed #007bff;
            }
            .upload-btn {
                background-color: #007bff;
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 16px;
                margin-top: 10px;
            }
            .upload-btn:hover {
                background-color: #0056b3;
            }
            input[type="file"] {
                padding: 10px;
                margin: 10px 0;
            }
            .file-list {
                margin-top: 20px;
            }
            .file-item {
                padding: 10px;
                background-color: #e9ecef;
                margin: 5px 0;
                border-radius: 4px;
            }
            .flash-message {
                padding: 15px;
                margin: 10px 0;
                border-radius: 4px;
            }
            .flash-success {
                background-color: #d4edda;
                color: #155724;
                border: 1px solid #c3e6cb;
            }
            .flash-error {
                background-color: #f8d7da;
                color: #721c24;
                border: 1px solid #f5c6cb;
            }
            table {
                margin-top: 20px;
                background-color: white;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Hello, World!</h1>
            
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, message in messages %}
                        <div class="flash-message flash-{{ category }}">{{ message }}</div>
                    {% endfor %}
                {% endif %}
            {% endwith %}
            
            <div class="upload-form">
                <h2>📁 Upload File to Unity Catalog</h2>
                <p><strong>Upload Location:</strong> {{ upload_path }}</p>
                <form method="POST" action="{{ url_for('upload_file') }}" enctype="multipart/form-data">
                    <input type="file" name="file" required>
                    <br>
                    <button type="submit" class="upload-btn">Upload File</button>
                </form>
                <p style="margin-top: 10px; color: #666; font-size: 14px;">
                    Allowed file types: txt, pdf, png, jpg, jpeg, gif, csv, xlsx, doc, docx
                </p>
            </div>
            
            {% if uploaded_files %}
            <div class="file-list">
                <h2>📂 Uploaded Files ({{ uploaded_files|length }})</h2>
                {% for file in uploaded_files %}
                <div class="file-item">{{ file }}</div>
                {% endfor %}
            </div>
            {% endif %}
            
            <h2>📊 Chart Data</h2>
            {{ chart_html|safe }}
        </div>
    </body>
    </html>
    """
    
    return render_template_string(html_template, 
                                 chart_html=chart_data.to_html(index=False),
                                 uploaded_files=uploaded_files,
                                 upload_path=UPLOAD_FOLDER)

@flask_app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file selected', 'error')
        return redirect(url_for('hello_world'))
    
    file = request.files['file']
    
    if file.filename == '':
        flash('No file selected', 'error')
        return redirect(url_for('hello_world'))
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        flash(f'File "{filename}" uploaded successfully to Unity Catalog!', 'success')
        return redirect(url_for('hello_world'))
    else:
        flash('Invalid file type. Please upload an allowed file type.', 'error')
        return redirect(url_for('hello_world'))

if __name__ == '__main__':
    flask_app.run(debug=True)
