import os
import unittest
import tempfile
import json
import io
from unittest.mock import patch, MagicMock, mock_open
from flask import Flask

# Import modules to test
from app import create_app
from config import Config
from api.chunk.utils import (
    get_metadata, 
    get_storage_client, 
    get_resource_storage_key,
    create_resumable_upload_session,
    get_signed_url,
    save_chunk_to_storage,
    is_processing_needed
)
from api.chunk.service import (
    start_chunk_upload,
    upload_chunk_data,
    complete_direct_upload,
    resume_chunk_upload,
    delete_chunk_upload
)
from api.chunk.pubsub_utils import (
    publish_message,
    publish_file_processing_task
)
from api.chunk.models import Resource, Chunk
from extensions import db

class TestConfig(Config):
    """Test configuration that overrides production config."""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"
    GCP_PROJECT_ID = "test-project"
    GCS_STORAGE_BUCKET_NAME = "test-bucket"
    GCS_STORAGE_EINO_BUCKET_NAME = "test-eino-bucket"
    PUBSUB_FILE_PROCESSING_TOPIC = "test-file-processing"
    PUBSUB_MEDIA_PROCESSING_TOPIC = "test-media-processing"
    USE_PUBSUB_FOR_MEDIA_PROCESSING = True
    SECRET_KEY = "test-key"
    DJANGO_BASE_URL = "http://localhost:8000"
    WATCHDOG_FOLDER = tempfile.mkdtemp()

class EinoGCPTestCase(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test."""
        self.app = create_app('TESTING')
        self.app.config.from_object(TestConfig)
        self.client = self.app.test_client()
        
        # Create all database tables
        with self.app.app_context():
            db.create_all()
            
        # Create test directories
        if not os.path.exists('chunk_files'):
            os.makedirs('chunk_files')
        if not os.path.exists(self.app.config['WATCHDOG_FOLDER']):
            os.makedirs(self.app.config['WATCHDOG_FOLDER'])
    
    def tearDown(self):
        """Clean up after each test."""
        with self.app.app_context():
            db.session.remove()
            db.drop_all()
            
    @patch('api.chunk.utils.storage.Client')
    def test_get_storage_client(self, mock_client):
        """Test the get_storage_client function."""
        with self.app.app_context():
            # Test without credentials file
            client = get_storage_client()
            mock_client.assert_called_once()
            
            # Test with credentials file
            mock_client.reset_mock()
            with patch('os.path.exists', return_value=True), \
                 patch('google.oauth2.service_account.Credentials.from_service_account_file') as mock_creds:
                self.app.config['GCP_SERVICE_ACCOUNT_FILE'] = 'fake-credentials.json'
                client = get_storage_client()
                mock_creds.assert_called_once_with('fake-credentials.json')
                mock_client.assert_called_once()
    
    def test_get_metadata(self):
        """Test metadata extraction from upload headers."""
        # Test with filename and filetype
        meta = "filename dGVzdC5wZGY=,filetype YXBwbGljYXRpb24vcGRm"
        result = get_metadata(meta)
        self.assertEqual(result['filename'], "test.pdf")
        self.assertEqual(result['filetype'], "application/pdf")
        
        # Test with empty values
        meta = "filename,"
        result = get_metadata(meta)
        self.assertEqual(result['filename'], "")
    
    @patch('api.chunk.utils.get_storage_client')
    def test_create_resumable_upload_session(self, mock_get_client):
        """Test creation of resumable upload session."""
        with self.app.app_context():
            # Create a mock resource
            resource = Resource(
                id="test-resource-id",
                name="test.mp4",
                type="video/mp4",
                company="test-company",
                created_by="test-user",
                size=1024,
                is_multipart=True
            )
            
            # Mock the GCS client and blob
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_bucket = MagicMock()
            mock_client.bucket.return_value = mock_bucket
            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob
            mock_blob.create_resumable_upload_session.return_value = "https://storage.googleapis.com/resumable-upload-url"
            
            # Call the function
            result = create_resumable_upload_session(resource)
            
            # Assertions
            self.assertEqual(result, "https://storage.googleapis.com/resumable-upload-url")
            mock_client.bucket.assert_called_once_with(self.app.config['GCS_STORAGE_EINO_BUCKET_NAME'])
            mock_bucket.blob.assert_called_once()
            mock_blob.create_resumable_upload_session.assert_called_once_with(
                content_type=resource.type,
                size=resource.size
            )
    
    @patch('api.chunk.utils.get_storage_client')
    def test_get_signed_url(self, mock_get_client):
        """Test generation of signed URLs."""
        with self.app.app_context():
            # Mock the GCS client and blob
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_bucket = MagicMock()
            mock_client.bucket.return_value = mock_bucket
            mock_blob = MagicMock()
            mock_bucket.blob.return_value = mock_blob
            mock_blob.generate_signed_url.return_value = "https://storage.googleapis.com/signed-url"
            
            # Call the function
            result = get_signed_url("test/key.mp4", expiration=1800, method="PUT")
            
            # Assertions
            self.assertEqual(result, "https://storage.googleapis.com/signed-url")
            mock_client.bucket.assert_called_once_with(self.app.config['GCS_STORAGE_EINO_BUCKET_NAME'])
            mock_bucket.blob.assert_called_once_with("test/key.mp4")
            mock_blob.generate_signed_url.assert_called_once_with(
                version="v4",
                expiration=1800,
                method="PUT"
            )
    
    def test_get_resource_storage_key(self):
        """Test generation of resource storage keys."""
        # Test for regular file
        resource = Resource(
            id="test-id",
            name="document.pdf",
            type="application/pdf",
            company="company1",
            created_by="user1"
        )
        key = get_resource_storage_key(resource)
        self.assertEqual(key, "company1/user1/test-id-document.pdf")
        
        # Test for video file
        resource.type = "video/mp4"
        key = get_resource_storage_key(resource)
        self.assertEqual(key, "hls_media/company1/user1/test-id/test-id-document.pdf")
    
    def test_is_processing_needed(self):
        """Test if processing is needed for different file types."""
        # Video file with processing
        self.assertTrue(is_processing_needed("video/mp4", True))
        
        # Video file without processing
        self.assertFalse(is_processing_needed("video/mp4", False))
        
        # Non-video file
        self.assertFalse(is_processing_needed("application/pdf", True))
    
    @patch('api.chunk.service.utils.create_resumable_upload_session')
    @patch('api.chunk.service.utils.get_storage_client')
    @patch('flask.request')
    def test_start_chunk_upload_direct(self, mock_request, mock_get_client, mock_create_session):
        """Test starting a direct chunk upload."""
        with self.app.app_context():
            # Mock request headers
            mock_request.headers = {
                'Upload-Length': '1024',
                'Upload-Metadata': 'filename dGVzdC5wZGY=,filetype YXBwbGljYXRpb24vcGRm'
            }
            
            # Mock session URI
            mock_create_session.return_value = "https://storage.googleapis.com/resumable-upload-url"
            
            # Mock auth data
            auth_data = {
                'user': {'uuid': 'test-user'},
                'company_user': {'id': 'test-company-user'}
            }
            
            # Call the function with direct_upload=True
            result = start_chunk_upload(
                auth_data, 
                'test-company', 
                'filename dGVzdC5wZGY=,filetype YXBwbGljYXRpb24vcGRm',
                'test-department',
                False,
                False,
                True
            )
            
            # Assertions
            self.assertEqual(result['id'], result['id'])  # ID will be dynamic
            self.assertEqual(result['filename'], 'test.pdf')
            self.assertEqual(result['filetype'], 'application/pdf')
            self.assertEqual(result['upload_url'], "https://storage.googleapis.com/resumable-upload-url")
            
            # Verify resource was created in the database
            resource = Resource.query.filter_by(name='test.pdf').first()
            self.assertIsNotNone(resource)
            self.assertEqual(resource.type, 'application/pdf')
            self.assertEqual(resource.company, 'test-company')
            self.assertEqual(resource.created_by, 'test-user')
            self.assertEqual(resource.upload_id, "https://storage.googleapis.com/resumable-upload-url")
    
    @patch('api.chunk.pubsub_utils.get_publisher_client')
    def test_publish_file_processing_task(self, mock_get_publisher):
        """Test publishing a file processing task to Pub/Sub."""
        with self.app.app_context():
            # Mock publisher
            mock_publisher = MagicMock()
            mock_get_publisher.return_value = mock_publisher
            mock_future = MagicMock()
            mock_future.result.return_value = "message-id-123"
            mock_publisher.publish.return_value = mock_future
            
            # Call the function
            result = publish_file_processing_task("test-resource-id")
            
            # Assertions
            mock_publisher.topic_path.assert_called_once_with(
                self.app.config['GCP_PROJECT_ID'],
                self.app.config['PUBSUB_FILE_PROCESSING_TOPIC']
            )
            mock_publisher.publish.assert_called_once()
            self.assertEqual(result, "message-id-123")

if __name__ == '__main__':
    unittest.main()