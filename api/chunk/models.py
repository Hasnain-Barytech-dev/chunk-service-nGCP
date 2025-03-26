from extensions import db
from sqlalchemy.orm import Mapped, mapped_column
import uuid
from flask import Response, current_app

from . import utils

class Resource(db.Model):
    __tablename__ = 'resource'

    id = db.Column(db.String(100), unique=True, primary_key=True, default=utils.get_random_uuid)
    name = db.Column(db.String(500), nullable=True, default='')
    type = db.Column(db.String(100), nullable=True, default='')
    directory = db.Column(db.String(1000), nullable=True, default='')
    size = db.Column(db.BigInteger, nullable=False)
    offset = db.Column(db.BigInteger, nullable=True, default=0)
    paused = db.Column(db.Boolean, default=False)
    status = db.Column(db.String(100), default='CHUNK_UPLOADING')
    is_completed = db.Column(db.Boolean, default=False)
    chunks_uploaded = db.Column(db.BigInteger, default=0)
    preview_image = db.Column(db.String(250), nullable=True)

    created_by = db.Column(db.String(250), nullable=True)
    company = db.Column(db.String(250), nullable=True)
    company_user = db.Column(db.String(250), nullable=True)
    department = db.Column(db.String(250), nullable=True)

    # Video streaming quality flags
    is_360p_done = db.Column(db.Boolean, default=False)
    is_480p_done = db.Column(db.Boolean, default=False)
    is_720p_done = db.Column(db.Boolean, default=False)
    is_1080p_done = db.Column(db.Boolean, default=False)
    
    # Upload and processing flags
    upload_id = db.Column(db.String(250), nullable=True)
    is_multipart = db.Column(db.Boolean, default=False)
    need_processing = db.Column(db.Boolean, default=False)
    is_deleted = db.Column(db.Boolean, default=False)
    file_upload_from_chat = db.Column(db.Boolean, default=False)
    
    # Streaming URLs and related fields
    hls_url = db.Column(db.String(500), nullable=True)
    dash_url = db.Column(db.String(500), nullable=True)
    stream_key = db.Column(db.String(250), nullable=True)
    
    # Streaming metadata
    video_duration = db.Column(db.Float, nullable=True)
    video_width = db.Column(db.Integer, nullable=True)
    video_height = db.Column(db.Integer, nullable=True)
    video_bitrate = db.Column(db.Integer, nullable=True)
    video_codec = db.Column(db.String(50), nullable=True)
    audio_codec = db.Column(db.String(50), nullable=True)
    
    # Processing tracking
    processing_started_at = db.Column(db.DateTime, nullable=True)
    processing_completed_at = db.Column(db.DateTime, nullable=True)
    processing_error = db.Column(db.Text, nullable=True)
    processing_progress = db.Column(db.Float, default=0)  # 0-100%
    
    # Relationship to chunks
    chunks = db.relationship('Chunk', backref='resource', lazy='dynamic')
    
    def get_hls_master_url(self):
        """Returns the HLS master playlist URL."""
        if self.hls_url:
            return self.hls_url
            
        # If explicit HLS URL is not set, construct one using standard pattern
        if not is_video_file(self.type):
            return None
            
        bucket_name = current_app.config.get('GCS_STORAGE_EINO_BUCKET_NAME')
        return f"https://storage.googleapis.com/{bucket_name}/hls_media/{self.company}/{self.created_by}/{self.id}/output.m3u8"
    
    def get_dash_url(self):
        """Returns the MPEG-DASH manifest URL."""
        if self.dash_url:
            return self.dash_url
            
        # If explicit DASH URL is not set, construct one using standard pattern
        if not is_video_file(self.type):
            return None
            
        bucket_name = current_app.config.get('GCS_STORAGE_EINO_BUCKET_NAME')
        return f"https://storage.googleapis.com/{bucket_name}/dash_media/{self.company}/{self.created_by}/{self.id}/manifest.mpd"
    
    def is_streaming_ready(self):
        """Checks if the resource is ready for streaming."""
        if not is_video_file(self.type):
            return False
            
        # Resource is ready for streaming if at least the 720p version is done
        return self.is_720p_done


class Chunk(db.Model):
    __tablename__ = 'resource_chunks'

    id = db.Column(db.String(120), unique=True, primary_key=True, default=utils.get_random_uuid)
    chunk_index = db.Column(db.Integer, nullable=True)
    data_key = db.Column(db.String(1000), nullable=False)
    tag = db.Column(db.String(1000), nullable=True)
    is_deleted = db.Column(db.Boolean, default=False)
    
    # Upload metadata
    chunk_size = db.Column(db.BigInteger, nullable=True)
    upload_started_at = db.Column(db.DateTime, nullable=True)
    upload_completed_at = db.Column(db.DateTime, nullable=True)

    resource_id = db.Column(db.String(120), db.ForeignKey('resource.id'), nullable=False)
    
    def __repr__(self):
        return f"<Chunk {self.id} (index: {self.chunk_index}, resource: {self.resource_id})>"


# Helper function to properly import inside the model methods
def is_video_file(file_type):
    """Checks if a file type is a video format."""
    return file_type and file_type.startswith('video/')
