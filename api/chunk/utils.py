import os
import requests
import uuid
import base64
import logging
import subprocess
import time
import io
import fitz
import threading
from pathlib import Path
from flask import Response, current_app, request
from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from extensions import db
from sqlalchemy.orm import Session


CHUNK_FOLDER_PATH = 'chunk_files'

def get_random_uuid():
    return str(uuid.uuid4())

def get_metadata(meta: str):
    metadata = {}
    for md in meta.split(','):
        md_split = md.split(' ')
        if len(md_split) == 2:
            key, value = md_split
            value = base64.b64decode(value)
            if isinstance(value, bytes):
                value = value.decode()
            metadata[key] = value
        else:
            metadata[md_split[0]] = ''
    return metadata

def get_upload_response(response = {}, status=200, extra_headers: dict = {}):
    headers = {
        'Access-Control-Allow-Origin': "*",
        'Access-Control-Allow-Methods': "PATCH,HEAD,GET,POST,OPTIONS",
        'Access-Control-Expose-Headers': "Tus-Resumable,upload-length,upload-metadata,Location,Upload-Offset",
        'Access-Control-Allow-Headers': "Tus-Resumable,upload-length,upload-metadata,Location,Upload-Offset,content-type",
        'Cache-Control': 'no-store',
        **extra_headers
    }
    return Response(response, status, headers=headers)

def check_chunk_folder():
    if not os.path.exists(CHUNK_FOLDER_PATH):
        os.mkdir(CHUNK_FOLDER_PATH)

def create_chunk_file(filename: str, data: str):
    check_chunk_folder()

    with open(f"{CHUNK_FOLDER_PATH}/{filename}", 'wb') as f:
        f.write(data)

def get_storage_client():
    """Returns an authenticated GCS client using the service account credentials."""
    credentials_path = current_app.config.get('GCP_SERVICE_ACCOUNT_FILE')
    if credentials_path and os.path.exists(credentials_path):
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        return storage.Client(credentials=credentials, project=current_app.config.get('GCP_PROJECT_ID'))
    return storage.Client()

def get_storage_bucket_name():
    """Returns the name of the GCS bucket for temporary chunk storage."""
    bucket_name = current_app.config.get('GCS_STORAGE_BUCKET_NAME')
    return bucket_name

def get_eino_storage_bucket_name():
    """Returns the name of the GCS bucket for final resource storage."""
    bucket_name = current_app.config.get('GCS_STORAGE_EINO_BUCKET_NAME')
    return bucket_name

def save_chunk_to_storage(resource_id, chunk_id):
    """Uploads a chunk file to GCS bucket."""
    storage_client = get_storage_client()
    bucket_name = get_storage_bucket_name()
    file_key = f"{resource_id}/{chunk_id}"
    local_file_path = f"{CHUNK_FOLDER_PATH}/{chunk_id}"
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_key)
    blob.upload_from_filename(local_file_path)
    
    return file_key

def get_chunk_file_size(chunk_id):
    path = f"{CHUNK_FOLDER_PATH}/{chunk_id}"
    if os.path.exists(path):
        return os.stat(path).st_size
    else:
        return current_app.config['MULTIPART_FILESIZE']

def delete_chunk_file(chunk_id):
    path = f"{CHUNK_FOLDER_PATH}/{chunk_id}"
    if os.path.exists(path):
        os.remove(path)

def delete_chunks(resource):
    chunks = resource.chunks
    storage_client = get_storage_client()
    bucket_name = get_storage_bucket_name()
    bucket = storage_client.bucket(bucket_name)

    for chunk in chunks:
        chunk.is_deleted = True
        db.session.add(chunk)
    resource.is_deleted = True

    db.session.add(resource)
    db.session.commit()

    # For GCP, we could implement proper deletion if needed
    # for chunk in chunks:
    #   if chunk and chunk.data_key:
    #     blob = bucket.blob(chunk.data_key)
    #     blob.delete()

def get_document_type(type: str):
    if type.startswith('image'):
        return 'image'
    elif type.startswith('video'):
        return 'video'
    elif type.startswith('audio'):
        return 'audio'
    return 'document'

def is_video_file(type: str):
    return type.startswith('video/')

def is_audio_file(type: str):
    return type.startswith('audio/')

def is_image_file(type: str):
    return type.startswith('image/')

def get_extension(resource):
    extension = resource.name.split('.')[-1] if len(resource.name.split('.')) > 1 else ''
    return extension

def get_resource_storage_key(resource):
    resource_key = f"{resource.company}/{resource.created_by}/{resource.id}-{resource.name}"
    if is_video_file(resource.type):
        resource_key = f"hls_media/{resource.company}/{resource.created_by}/{resource.id}/{resource.id}-{resource.name}"
    return resource_key

def create_resumable_upload_session(resource):
    """Creates a resumable upload session to GCS."""
    storage_client = get_storage_client()
    bucket_name = get_eino_storage_bucket_name()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(get_resource_storage_key(resource))
    
    # Create a resumable upload session
    session_uri = blob.create_resumable_upload_session(
        content_type=resource.type,
        size=resource.size
    )
    
    return session_uri

def get_signed_url(resource_key, expiration=3600, method='GET'):
    """Generate a signed URL for the given resource key."""
    storage_client = get_storage_client()
    bucket_name = get_eino_storage_bucket_name()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(resource_key)
    
    url = blob.generate_signed_url(
        version="v4",
        expiration=expiration,
        method=method
    )
    
    return url


def get_extension(resource):
    """Gets the file extension from the resource."""
    # Implementation depends on your resource object structure
    if hasattr(resource, 'name') and '.' in resource.name:
        return resource.name.split('.')[-1].lower()
    return ''

def get_resource_storage_key(resource):
    """Gets the storage key for the resource."""
    # Implementation depends on your storage strategy
    return f"{resource.company}/{resource.created_by}/{resource.id}"

def get_document_type(content_type):
    """Determines document type based on content type."""
    # Implementation depends on your content type classification
    if 'image' in content_type:
        return 'image'
    elif 'video' in content_type:
        return 'video'
    elif 'pdf' in content_type:
        return 'document'
    # Add more document types as needed
    return 'other'

def is_video_file(content_type):
    """Checks if the content type is for video."""
    return content_type and 'video' in content_type

def get_auth_token_from_company_user(company_user, company, user):
    """Gets authentication token for a company user."""
    data = {
        'company_user': company_user,
        'user': user,
        'company': company
    }
    res = requests.post(f"{current_app.config['DJANGO_BASE_URL']}/api/v2/company/get_company_user_token/", json=data)
    
    if res.ok:
        return res.json()
    else:
        logging.error("Auth Token error during resource save")
        return {'access_token': None}

def save_resource_to_db(resource, need_auth=False, fileUploadFromChat=False):
    """
    Saves a resource to the database.
    
    Args:
        resource: The resource object to save
        need_auth: Whether authentication token is needed
        fileUploadFromChat: Whether file was uploaded from chat
    
    Returns:
        Response data if successful, None otherwise
    """
    try:
        extension = get_extension(resource)
        resource_key = get_resource_storage_key(resource)
        
        if fileUploadFromChat:
            document_type = 'chat'
        else:
            document_type = get_document_type(resource.type)
        
        data = {
            'id': resource.id,
            'document_type': document_type,
            'type': resource.type,
            'directory': resource.directory,
            'extension': extension,
            'size': resource.size,
            'link_url': None,
            'title': resource.name,
            'preview_image': resource.preview_image,
            'document': resource_key,
            'document_size': resource.size,
            'content_type': resource.type,
        }
        
        # If video has HLS streaming available, use that URL
        is_video = is_video_file(resource.type)
        if is_video and resource.is_720p_done:
            hls_folder = f"hls_media/{resource.company}/{resource.created_by}/{resource.id}"
            gcs_url = f"https://storage.googleapis.com/{current_app.config['GCS_STORAGE_EINO_BUCKET_NAME']}/{hls_folder}/output.m3u8"
            data['link_url'] = gcs_url
            data['document'] = None
        
        # Set up headers based on authentication need
        if need_auth:
            user_data = get_auth_token_from_company_user(resource.company_user, resource.company, resource.created_by)
            headers = {
                'Authorization': f"Bearer {user_data.get('access_token')}",
                'X-Tenant-ID': resource.company,
                'Department-Id': resource.department
            }
        else:
            headers = {
                'Authorization': request.headers.get('Authorization'),
                'X-Tenant-ID': request.headers.get('X-Tenant-ID'),
                'Department-Id': resource.department
            }
        
        # Make API request to save resource
        res = requests.post(
            f"{current_app.config['DJANGO_BASE_URL']}/api/v2/resource/save_chunk_resource/",
            json=data,
            headers=headers
        )
        
        if res.ok:
            return res.json()
        else:
            logging.error(f"Failed to save resource {resource.name} to DB. Status: {res.status_code}")
            return None
            
    except Exception as ex:
        logging.error(f"Exception in save_resource_to_db: {ex}")
        return None

def generate_hls_streams(resource, db, bucket):
    """
    Generates HLS streams for video resources.
    
    Args:
        resource: The video resource object
        db: Database session
        bucket: GCS bucket object
    """
    try:
        # Create output folder for HLS streams
        output_folder = f"hls_media/{resource.company}/{resource.created_by}/{resource.id}"
        master_playlist = "#EXTM3U\n"
        
        # Generate streams for different qualities
        # This would contain the code for generating various quality streams
        # ...
        
        # For demonstration, assuming a quality_name variable was defined in the loop
        quality_name = "720p"  # This would normally be set in the loop
        
        # Upload master playlist
        master_playlist_path = f"{output_folder}/output.m3u8"
        with open(master_playlist_path, 'w') as f:
            f.write(master_playlist)
        
        master_blob = bucket.blob(f"{output_folder}/output.m3u8")
        master_blob.upload_from_filename(master_playlist_path)
        master_blob.make_public()
        
        # Clean up master playlist file
        if os.path.exists(master_playlist_path):
            os.remove(master_playlist_path)
        
        # Update resource link URL to point to the master playlist
        hls_url = f"https://storage.googleapis.com/{current_app.config['GCS_STORAGE_EINO_BUCKET_NAME']}/{output_folder}/output.m3u8"
        resource.link_url = hls_url
        db.session.commit()
        
        # Save resource to DB with updated streaming URL
        save_resource_to_db(resource, need_auth=True)
        
    except Exception as ex:
        logging.error(f"Error generating {quality_name} stream: {ex}")
        return False
        
    return True


def update_resource_quality_status(resource, quality):
    """Updates resource status flags for completed qualities."""
    if quality == '360p':
        resource.is_360p_done = True
    elif quality == '480p':
        resource.is_480p_done = True
    elif quality == '720p':
        resource.is_720p_done = True
    elif quality == '1080p':
        resource.is_1080p_done = True
    
    db.session.add(resource)
    db.session.commit()

def save_hls_file(event):
    """Handles file events for HLS segments and playlists."""
    from main import app
    from .service import delete_chunk_upload

    with app.app_context():
        storage_client = get_storage_client()
        bucket_name = get_eino_storage_bucket_name()
        bucket = storage_client.bucket(bucket_name)
    
        path = event.src_path
        file_path = Path(path)
        gcs_key = str(file_path.relative_to(os.getcwd()))
        logging.info(f"Saving HLS file: {path}")
        
        if os.path.exists(path) and path.endswith('.m3u8'):
            if path.endswith('output_360p.m3u8'):
                # Generate and upload master playlist if 360p variant is ready
                output_key = gcs_key.replace('output_360p.m3u8', 'output.m3u8')
                output_data = f"""#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=640x360,NAME="360"
output_360p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=854x480,NAME="480"
output_480p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1280x720,NAME="720"
output_720p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=8000000,RESOLUTION=1920x1080,NAME="1080"
output_1080p.m3u8"""
                output_blob = bucket.blob(output_key)
                output_blob.upload_from_string(output_data, content_type='application/vnd.apple.mpegurl')
                output_blob.make_public()

            # Upload the variant playlist
            blob = bucket.blob(gcs_key)
            blob.upload_from_filename(path)
            blob.make_public()
            logging.info(f"m3u8 saved: {path}")
            
            if os.path.exists(path):
                os.remove(path)
            
            # When the 1080p variant is done, clean up
            if path.endswith('output_1080p.m3u8'):
                resource_id = path.split(os.path.sep)[-2]
                # Update resource status and perform cleanup
                delete_chunk_upload(resource_id)
    
def save_stream_file(event):
    """Saves streaming video files to GCS and updates resource status."""
    from main import app
    from api.chunk.models import Resource
    from .service import delete_chunk_upload

    try:
        with app.app_context():
            should_commit = False
            path = event.src_path
            file_path = Path(path)
            
            if os.path.exists(path) and file_path.is_file() and not path.endswith('.tmp'):
                storage_client = get_storage_client()
                bucket_name = get_eino_storage_bucket_name()
                bucket = storage_client.bucket(bucket_name)

                # Get relative path for GCS key
                gcs_key = str(file_path.relative_to(os.getcwd()))
                blob = bucket.blob(gcs_key)
                blob.upload_from_filename(path)
                blob.make_public()
                
                # Clean up local file after upload
                if os.path.exists(path):
                    os.remove(path)
                
                # Update resource status based on segment file
                if 'output_360p0.ts' in path:
                    resource_id = path.split(os.path.sep)[-2]
                    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
                    if resource:
                        should_commit = True
                        resource.is_360p_done = True
                
                if 'output_480p0.ts' in path:
                    resource_id = path.split(os.path.sep)[-2]
                    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
                    if resource:
                        should_commit = True
                        resource.is_480p_done = True
                
                if 'output_720p0.ts' in path:
                    resource_id = path.split(os.path.sep)[-2]
                    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
                    if resource:
                        should_commit = True
                        resource.is_720p_done = True
                        
                if 'output_1080p0.ts' in path:
                    resource_id = path.split(os.path.sep)[-2]
                    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
                    if resource:
                        should_commit = True
                        resource.is_1080p_done = True
                
                # Commit any status changes
                if should_commit and resource:
                    current_db_session = db.session.object_session(resource)
                    if current_db_session:
                        current_db_session.add(resource)
                        current_db_session.commit()
                    else:
                        db.session.add(resource)
                        db.session.commit()
                
                # Check if all segments are uploaded and clean up
                if 'output_1080p' in path:
                    res_path = os.path.split(path)[0]
                    files = next(os.walk(res_path))[2]
                    if len(files) == 0:
                        resource_id = path.split(os.path.sep)[-2]
                        delete_chunk_upload(resource_id)

    except Exception as ex:
        logging.error(f"Exception in save_stream_file: {ex}")

def is_processing_needed(type, need_processing=False):
    """Determines if a file needs video processing."""
    is_video = is_video_file(type)
    return is_video and need_processing

def convert_to_mp3_file(file, resource):
    """Converts an audio file to MP3 format."""
    audio_command = ['ffmpeg', '-i', '-', '-vn', '-ar', '44100', '-ac', '2', '-b:a', '192k', f'{resource.id}.mp3']
    file_memoryview = memoryview(file.getbuffer())

    process = subprocess.Popen(
        audio_command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    process.communicate(input=file_memoryview)

    mp3_file = open(f'{resource.id}.mp3', 'rb')
    audio_bytes = io.BytesIO(mp3_file.read())

    return audio_bytes

def convert_to_mp4(resource):
    """
    Converts a video file to MP4 format and handles HLS generation.
    This function has been enhanced to better integrate with adaptive streaming.
    """
    from main import app
    from .service import delete_chunk_upload
    from . import pubsub_utils
   
    with app.app_context():
        try:
            app.config['MP4_CONVERT_LOCK'].acquire()
            
            # For Cloud environment, use Pub/Sub for async processing
            if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
                # Publish a message to Pub/Sub for mp4 conversion
                pubsub_utils.publish_mp4_conversion_task(resource.id)
                return
                
            storage_client = get_storage_client()
            bucket_name = get_eino_storage_bucket_name()
            bucket = storage_client.bucket(bucket_name)
            resource_key = get_resource_storage_key(resource)
            
            signed_url = get_signed_url(resource_key, expiration=3600, method='GET')
            output_filename = f"{resource.name.split('.')[0]}.mp4"
            output_name = f"{resource.id}-{output_filename}"
            
            # Update key for the new MP4 file
            res_key = resource_key.split('/')
            res_key[-1] = output_name
            new_resource_key = '/'.join(res_key)
            
            # Create a clean MP4 that's optimized for streaming
            command = [
                'ffmpeg', '-i', signed_url, 
                '-c:v', 'libx264', '-profile:v', 'main', '-level', '4.0',
                '-preset', 'medium', '-crf', '22', 
                '-c:a', 'aac', '-b:a', '192k', '-ac', '2',
                '-movflags', '+faststart',  # Important for streaming
                output_name
            ]
            
            process = subprocess.run(
                command, 
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            is_valid_file = os.path.exists(output_name) and os.stat(output_name).st_size > 0
            
            if is_valid_file:
                # Upload the MP4 to GCS
                new_blob = bucket.blob(new_resource_key)
                new_blob.upload_from_filename(output_name)
                os.remove(output_name)

                # Update resource name to reflect MP4 conversion
                resource.name = output_filename
                db.session.commit()
                
                # If video processing is needed, generate HLS streams
                if resource.need_processing:
                    create_stream(signed_url, resource)
                else:
                    # Otherwise just save the MP4 resource
                    save_resource_to_db(resource, True)
                    
                # Clean up if using multipart upload
                if resource.is_multipart:
                    delete_chunk_upload(resource.id)
                    
        except storage.exceptions.NotFound as e:
            logging.error(f"GCS object not found: {e}")
            # Re-raise to allow calling code to handle it
            raise
        except subprocess.SubprocessError as e:
            logging.error(f"FFmpeg conversion error: {e}")
            # Could handle specific ffmpeg errors here if needed
        except Exception as ex:
            logging.error(f"Exception in conversion to mp4: {ex}")
        finally:
            if app.config['MP4_CONVERT_LOCK'].locked():
                app.config['MP4_CONVERT_LOCK'].release()

def get_default_filepreview_by_content_type(content_type):
    if content_type in [
        'application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'application/vnd.ms-fontobject', 'application/epub+zip', 'application/vnd.oasis.opendocument.text',
        'application/x-abiword', 'application/x-freearc', 'application/epub'
    ]:
        return f"{current_app.config['TEMPLATE_IMAGES_PATH']}/templates/images/ppt-preview.jpeg"
    elif content_type in ['text/plain', 'font/ttf', 'application/xml', 'text/vcard']:
        return f"{current_app.config['TEMPLATE_IMAGES_PATH']}/templates/images/txt-preview.jpeg"
    elif content_type in ['text/vcard', 'text/x-vcard', 'application/vcf']:
        return f"{current_app.config['TEMPLATE_IMAGES_PATH']}/templates/images/vcf-preview.svg"
    elif is_audio_file(content_type):
      return None
    elif is_image_file(content_type):
      return None
    elif is_video_file(content_type):
      return None
    else:
        return f"{current_app.config['TEMPLATE_IMAGES_PATH']}/templates/images/no-preview.jpeg"

def save_preview_image(resource, file, preview_video = True):
    """Generates and saves a preview image for the resource."""
    from main import app

    with app.app_context():
        is_video = is_video_file(resource.type)
        if resource.type in ['application/pdf']:
            resource = save_pdf_preview(resource, file)
        elif resource.type in ['application/epub+zip', 'application/epub']:
            resource = save_epub_preview(resource, file)
        elif is_video and preview_video:
            resource = save_video_preview(resource, file)
        elif is_image_file(resource.type):
            # No need to save the preview image as this is handled in frontend
            pass
        elif is_audio_file(resource.type):
            # No need to save the preview image as this is handled in frontend
            pass
        else:
            preview_image = get_default_filepreview_by_content_type(resource.type)

            if preview_image:
                storage_client = get_storage_client()
                bucket_name = get_eino_storage_bucket_name()
                bucket = storage_client.bucket(bucket_name)
                
                extension = preview_image.split('.')[-1]
                key_path = f"{resource.company}/{resource.created_by}/preview-{resource.id}.{extension}"
                blob = bucket.blob(key_path)
                blob.upload_from_filename(preview_image)

                resource.preview_image = key_path
                db.session.commit()
    return resource
    
def save_epub_preview(resource, file):
    """Generates a preview image for EPUB files."""
    file_name = f"epub-content-{resource.id}.epub"
    output = f"epub-preview-{resource.id}.png"

    # String file parameter means the file URL is sent
    if type(file) == str:
        download_file(file, file_name)
    else:
        # Save the EPUB in local to open it with fitz library
        with open(file_name, "wb") as f:
            f.write(file.getbuffer())

    doc = fitz.open(file_name)
    page = doc.load_page(0)
    pix = page.get_pixmap()
    pix.save(output)
    doc.close()

    with open(output, 'rb') as img_file:
        img_bytes = img_file.read()

    storage_client = get_storage_client()
    bucket_name = get_eino_storage_bucket_name()
    bucket = storage_client.bucket(bucket_name)
    
    key_path = f"{resource.company}/{resource.created_by}/epub-preview-{resource.id}.png"
    blob = bucket.blob(key_path)
    blob.upload_from_string(img_bytes, content_type='image/png')

    resource.preview_image = key_path
    db.session.commit()

    try:
        if os.path.exists(output):
            os.remove(output)
        if os.path.exists(file_name):
            os.remove(file_name)
    except:
        pass

    return resource

def download_file(url, filename):
    """Downloads a file from a URL to a local file."""
    with requests.get(url, stream=True) as r:
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    
def save_pdf_preview(resource, file):
    """Generates a preview image for PDF files."""
    filename = f'pdf-file__{resource.id}-{resource.name}'
    # String file parameter means the file URL is sent
    if type(file) == str:
        download_file(file, filename)
        file = open(filename, 'rb')
        doc = fitz.open(stream=file.read())
    else:
        doc = fitz.open(stream=file.read()) 
    page = doc.load_page(0)
    pix = page.get_pixmap()
    img_bytes = pix.tobytes('png')

    storage_client = get_storage_client()
    bucket_name = get_eino_storage_bucket_name()
    bucket = storage_client.bucket(bucket_name)
    
    key_path = f"{resource.company}/{resource.created_by}/pdf-preview-{resource.id}.png"
    blob = bucket.blob(key_path)
    blob.upload_from_string(img_bytes, content_type='image/png')

    resource.preview_image = key_path
    db.session.commit()
    doc.close()

    file.seek(0)

    if os.path.exists(filename):
        os.remove(filename)
    return resource

def save_video_preview(resource, file):
    """Generates a thumbnail preview image for video files."""
    try:
        unique_filename = str(uuid.uuid4())
        output_image = os.path.join(f'{unique_filename}.png')

        if not resource.is_multipart:
            combined_file_name = f"{resource.id}-{resource.name}"

            ffmpeg_command = [
                'ffmpeg', '-i', combined_file_name, '-ss', '00:00:01', '-frames:v', '1', 
                '-vf', 'scale=640:-1', output_image
            ]
            subprocess.run(
                ffmpeg_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        else:
            ffmpeg_command = [
                'ffmpeg', '-i', file, '-ss', '00:00:01', '-frames:v', '1', 
                '-vf', 'scale=640:-1', output_image
            ]
            subprocess.run(
                ffmpeg_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

        if os.path.exists(output_image):
            storage_client = get_storage_client()
            bucket_name = get_eino_storage_bucket_name()
            bucket = storage_client.bucket(bucket_name)
            
            key_path = f"{resource.company}/{resource.created_by}/video-preview-{resource.id}.jpg"
            blob = bucket.blob(key_path)
            blob.upload_from_filename(output_image)

            if os.path.exists(output_image):
                os.remove(output_image)

            resource.preview_image = key_path
            db.session.commit()

    except Exception as ex:
        logging.error(f"Exception in saving Video preview: {ex}")

    return resource



def get_auth_token_from_company_user(company_user: str, company: str, user: str):
    """Gets an authentication token for a company user."""
    data = {
        'company_user': company_user,
        'user': user,
        'company': company
    }
    res = requests.post(f"{current_app.config['DJANGO_BASE_URL']}/api/v2/company/get_company_user_token/", json=data)
    
    if res.ok:
        res = res.json()
        return res.get('data')
    else:
        logging.error("Auth Token error during restart")
        return None

def get_preview_image_by_content_type(content_type):
    """Returns preview image path based on content type."""
    if content_type in [
        'application/pdf', 'image/tiff', 'application/postscript'
    ]:
        return f"{current_app.config['TEMPLATE_IMAGES_PATH']}/templates/images/doc-preview.jpeg"
    elif content_type in [
        'application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.oasis.opendocument.spreadsheet', 'text/csv', 'application/csv'
    ]:
        return f"{current_app.config['TEMPLATE_IMAGES_PATH']}/templates/images/excel-preview.jpeg"
    elif content_type in [
        'application/vnd.ms-powerpoint', 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'application/vnd.oasis.opendocument.presentation'
    ]:
        return f"{current_app.config['TEMPLATE_IMAGES_PATH']}/templates/images/ppt-preview.jpeg"
    elif content_type in ['text/plain', 'font/ttf', 'application/xml', 'text/vcard']:
        return f"{current_app.config['TEMPLATE_IMAGES_PATH']}/templates/images/text-preview.jpeg"
    else:
        return f"{current_app.config['TEMPLATE_IMAGES_PATH']}/templates/images/default-preview.jpeg"

def save_resource_to_db(resource):
    """Saves a resource to the database."""
    try:
        # Database saving logic would go here
        return True
    except Exception as ex:
        logging.error(f"Exception in save_resource_to_db: {ex}")
        return False

def create_stream(file, resource):
    """
    Creates adaptive streaming formats (HLS) for video resources.
    
    Args:
        file: The source file path or BytesIO object
        resource: The Resource object from the database
    """
    from main import app
    from .service import delete_chunk_upload, combine_chunks
    from .models import Resource
    from . import pubsub_utils

    combined_file_name = f"{uuid.uuid4()}-{resource.name}"
    try:
        with app.app_context():
            # Need this because of different app context
            resource = Resource.query.filter_by(id=resource.id, is_deleted=False).first()
            combined_file = combine_chunks(resource) if not isinstance(file, str) else None
            
            # Save preview image first
            if combined_file:
                save_preview_image(resource, combined_file, True)
                with open(combined_file_name, 'wb') as f:
                    f.write(combined_file.getbuffer())
            else:
                # If file is a string (URL), download it first
                download_file(file, combined_file_name)
                save_preview_image(resource, combined_file_name, True)
            
            resource_key = get_resource_storage_key(resource)

            logging.info(f"Creating adaptive streams for: {resource.name}")
            
            # Define streaming quality presets
            qualities = [
                {'name': '360p', 'resolution': '640x360', 'bitrate': '1M', 'crf': '28', 'bandwidth': '1000000'},
                {'name': '480p', 'resolution': '854x480', 'bitrate': '2M', 'crf': '26', 'bandwidth': '2000000'},
                {'name': '720p', 'resolution': '1280x720', 'bitrate': '4M', 'crf': '24', 'bandwidth': '4000000'},
                {'name': '1080p', 'resolution': '1920x1080', 'bitrate': '8M', 'crf': '22', 'bandwidth': '8000000'}
            ]

            storage_client = get_storage_client()
            eino_bucket_name = get_eino_storage_bucket_name()
            bucket = storage_client.bucket(eino_bucket_name)

            # Update the database with preview image if available
            if resource.preview_image:
                save_resource_to_db(resource, need_auth=True)

            # Create the HLS directory structure
            hls_folder = f"hls_media/{resource.company}/{resource.created_by}/{resource.id}"
            os.makedirs(hls_folder, exist_ok=True)

            if combined_file:
                combined_file.close()

            # For Cloud environment, use Pub/Sub for async processing
            if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
                # Publish a message to Pub/Sub for media processing
                pubsub_utils.publish_media_processing_task(
                    resource.id, 
                    combined_file_name, 
                    hls_folder, 
                    qualities
                )
                return
            
            # Process locally if not using Pub/Sub
            generate_hls_streams(combined_file_name, hls_folder, resource, qualities, bucket)
            
    except Exception as ex:
        logging.error(f"Error creating stream: {ex}")
    finally:
        if os.path.exists(combined_file_name):
            os.remove(combined_file_name)

def generate_hls_streams(source_file, output_folder, resource, qualities, bucket):
    """
    Generates HLS streams at different quality levels using FFmpeg.
    
    Args:
        source_file: Path to the source video file
        output_folder: Directory to store HLS segments
        resource: The Resource database object
        qualities: List of quality presets (resolution, bitrate)
        bucket: GCS bucket object for uploads
    """
    # Create master playlist content
    master_playlist = "#EXTM3U\n#EXT-X-VERSION:3\n"
    
    for quality in qualities:
        quality_name = quality['name']
        output_name = f"output_{quality_name}"
        
        # Skip already processed qualities
        if quality_name == '360p' and resource.is_360p_done:
            continue
        if quality_name == '480p' and resource.is_480p_done:
            continue
        if quality_name == '720p' and resource.is_720p_done:
            continue
        if quality_name == '1080p' and resource.is_1080p_done:
            continue
        
        segment_path = f"{output_folder}/{output_name}.m3u8"
        os.makedirs(os.path.dirname(segment_path), exist_ok=True)
        
        # Improved FFmpeg command with better encoding parameters
        command = [
            'ffmpeg', '-i', source_file,
            '-c:v', 'libx264', '-profile:v', 'main', '-level', '4.0',
            '-preset', 'medium', '-crf', quality['crf'], 
            '-sc_threshold', '0', '-g', '48', '-keyint_min', '48',
            '-hls_time', '4', '-hls_playlist_type', 'vod',
            '-b:v', quality['bitrate'], '-maxrate', quality['bitrate'], 
            '-bufsize', str(int(quality['bitrate'].replace('M', '')) * 2) + 'M',
            '-c:a', 'aac', '-b:a', '128k', '-ac', '2',
            '-s', quality['resolution'], 
            '-hls_segment_filename', f"{output_folder}/{output_name}_%03d.ts",
            segment_path
        ]
        
        try:
            logging.info(f"Generating {quality_name} HLS stream")
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                logging.error(f"FFmpeg error for {quality_name}: {stderr.decode()}")
                continue
                
            # Add entry to master playlist
            master_playlist += f"#EXT-X-STREAM-INF:BANDWIDTH={quality['bandwidth']},RESOLUTION={quality['resolution']},NAME=\"{quality_name}\"\n"
            master_playlist += f"{output_name}.m3u8\n"
            
            # Upload segment playlist to GCS
            blob = bucket.blob(f"{output_folder}/{output_name}.m3u8")
            blob.upload_from_filename(segment_path)
            blob.make_public()
            
            # Upload all segment files
            segment_files = [f for f in os.listdir(output_folder) if f.startswith(f"{output_name}_") and f.endswith(".ts")]
            for segment in segment_files:
                segment_path = f"{output_folder}/{segment}"
                blob = bucket.blob(f"{output_folder}/{segment}")
                blob.upload_from_filename(segment_path)
                blob.make_public()
                
                # Clean up local segment file after upload
                if os.path.exists(segment_path):
                    os.remove(segment_path)
            
            # Update resource status based on quality
            update_resource_quality_status(resource, quality_name)
            
        except Exception as ex:
            logging.error(f"Error generating {quality_name} stream: {ex}")
    
            
            
       
    
    # Upload master playlist
        master_playlist_path = f"{output_folder}/output.m3u8"
        with open(master_playlist_path, 'w') as f:
            
            
            f.write(master_playlist)
    
        master_blob = bucket.blob(f"{output_folder}/output.m3u8")
        master_blob.upload_from_filename(master_playlist_path)
        master_blob.make_public()
    
    # Clean up master playlist file
        if os.path.exists(master_playlist_path):
            
           os.remove(master_playlist_path)
    
    # Update resource link URL to point to the master playlist
        hls_url = f"https://storage.googleapis.com/{current_app.config['GCS_STORAGE_EINO_BUCKET_NAME']}/{output_folder}/output.m3u8"
        resource.link_url = hls_url
        db.session.commit()
    
    # Save resource to DB with updated streaming URL
        save_resource_to_db(resource, need_auth=True)     
