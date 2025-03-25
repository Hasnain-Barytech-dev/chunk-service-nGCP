import os
import requests
import uuid
import base64
from flask import Response, current_app, request
from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from extensions import db
import subprocess
import time
from pathlib import Path
import threading
import time
import io
import fitz
import logging

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

def chunks_uploaded():
  pass

def save_resource(resource):
  print("Chunk upload Completed! Saving Resource...")

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

def save_resource_to_db(resource, need_auth=False, fileUploadFromChat=False):
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

    is_video = is_video_file(resource.type)
    if is_video and resource.is_720p_done:
      hls_folder = f"hls_media/{resource.company}/{resource.created_by}/{resource.id}"
      gcs_url = f"https://storage.googleapis.com/{current_app.config['GCS_STORAGE_EINO_BUCKET_NAME']}/{hls_folder}/output.m3u8"
      data['link_url'] = gcs_url
      data['document'] = None

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
    res = requests.post(f"{current_app.config['DJANGO_BASE_URL']}/api/v2/resource/save_chunk_resource/", json=data, headers=headers)

    if res.ok:
      res = res.json()
    else:
      print(f"Failed to save: {resource.name} to DB")
    
  except Exception as ex:
    print(f"Exception in save_resource_to_db: {ex}")

def create_steam(file, resource):
  from main import app
  from .service import delete_chunk_upload, combine_chunks
  from .models import Resource
  from . import pubsub_utils

  combined_file_name = f"{uuid.uuid4()}-{resource.name}"
  try:
    with app.app_context():
      # Need this because of different app context.
      resource = Resource.query.filter_by(id=resource.id, is_deleted=False).first()
      combined_file = combine_chunks(resource)
      save_preview_image(resource, combined_file, True)
      resource_key = get_resource_storage_key(resource)

      with open(combined_file_name, 'wb') as f:
        f.write(combined_file.getbuffer())

      print("Creating Stream: ", resource.name)
      qualities = [
        { 'output': 'output_360p.m3u8', 'bitrate': '1M', 'crf': 40 },
        { 'output': 'output_480p.m3u8', 'bitrate': '2M', 'crf': 30 },
        { 'output': 'output_720p.m3u8', 'bitrate': '4M', 'crf': 25 },
        { 'output': 'output_1080p.m3u8', 'bitrate': '8M', 'crf': 20 },
      ]

      storage_client = get_storage_client()
      eino_bucket_name = get_eino_storage_bucket_name()
      bucket = storage_client.bucket(eino_bucket_name)

      if resource.preview_image:
        save_resource_to_db(resource, need_auth=True)

      hls_folder = f"hls_media/{resource.company}/{resource.created_by}/{resource.id}"

      if not os.path.exists(f"{hls_folder}"):
        os.makedirs(hls_folder, exist_ok=True)

      combined_file.close()

      # For Cloud environment, we should use Pub/Sub for async processing
      if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
        # Publish a message to Pub/Sub for media processing
        pubsub_utils.publish_media_processing_task(
          resource.id, 
          combined_file_name, 
          hls_folder, 
          qualities
        )
        return
      
      # Local processing logic (when not using Pub/Sub)
      save_output = False
      for quality in qualities:
        if quality['output'] == 'output_360p.m3u8' and resource.is_360p_done:
          continue
        if quality['output'] == 'output_480p.m3u8' and resource.is_480p_done:
          continue
        if quality['output'] == 'output_720p.m3u8' and resource.is_720p_done:
          continue
        if quality['output'] == 'output_1080p.m3u8' and resource.is_1080p_done:
          continue

        command = ['ffmpeg', '-i', combined_file_name, '-profile:v', 'baseline', '-level', '5.2', '-b:v', quality['bitrate'], '-crf', f'{quality["crf"]}', '-start_number', '0', '-movflags', 'faststart', '-hls_time', '2', '-hls_list_size', '0', '-f', 'hls', f"{hls_folder}/{quality['output']}"]

        process = subprocess.Popen(
          command,
          stdin=subprocess.PIPE,
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE
        )
        
        process.communicate()
        logging.info(f"Process done for: {quality['output']}", os.path.exists(f"{hls_folder}/{quality['output']}"))

        if not save_output:
          output_data = f"""#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-STREAM-INF:BANDWIDTH=413696,RESOLUTION=640x360,NAME="360"\noutput_360p.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=964608,RESOLUTION=854x480,NAME="480"\noutput_480p.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=2424832,RESOLUTION=1280x720,NAME="720"\noutput_720p.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=4521984,RESOLUTION=1920x1080,NAME="1080"\noutput_1080p.m3u8"""
          output_file = io.BytesIO(output_data.encode())
          
          output_blob = bucket.blob(f"{hls_folder}/output.m3u8")
          output_blob.upload_from_string(output_data, content_type='application/vnd.apple.mpegurl')
          output_blob.make_public()
          save_output = True

        s3_key = f"{hls_folder}/{quality['output']}"
        path = f"{app.config['WATCHDOG_FOLDER']}/{resource.company}/{resource.created_by}/{resource.id}/{quality['output']}"
        
        # Upload to GCS
        blob = bucket.blob(s3_key)
        blob.upload_from_filename(path)
        blob.make_public()

        if os.path.exists(path):
          os.remove(path)

  except Exception as ex:
    print("Exception: ", ex)
  finally:
    if os.path.exists(combined_file_name):
      os.remove(combined_file_name)
    if file and os.path.exists(file):
      os.remove(file)

def save_hls_file(event):
  from main import app
  from .service import delete_chunk_upload

  with app.app_context():
    storage_client = get_storage_client()
    bucket_name = get_eino_storage_bucket_name()
    bucket = storage_client.bucket(bucket_name)
  
    path = event.src_path
    file_path = Path(path)
    gcs_key = str(file_path.relative_to(os.getcwd()))
    logging.info(f"Saving chunk HLS: {path}")
    if os.path.exists(path) and path.endswith('.m3u8'):
      if path.endswith('output_360p.m3u8'):
        output_key = gcs_key.replace('output_360p.m3u8', 'output.m3u8')
        output_data = f"""#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-STREAM-INF:BANDWIDTH=413696,RESOLUTION=640x360,NAME="360"\noutput_360p.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=964608,RESOLUTION=854x480,NAME="480"\noutput_480p.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=2424832,RESOLUTION=1280x720,NAME="720"\noutput_720p.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=4521984,RESOLUTION=1920x1080,NAME="1080"\noutput_1080p.m3u8"""
        output_blob = bucket.blob(output_key)
        output_blob.upload_from_string(output_data, content_type='application/vnd.apple.mpegurl')
        output_blob.make_public()

      blob = bucket.blob(gcs_key)
      blob.upload_from_filename(path)
      blob.make_public()
      print(f"m3u8 saved: {path}")
      if os.path.exists(path):
        os.remove(path)
      
      if path.endswith('output_1080p.m3u8'):
        resource_id = path.split(os.path.sep)[-2]
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

          gcs_key = str(file_path.relative_to(os.getcwd()))
          blob = bucket.blob(gcs_key)
          blob.upload_from_filename(path)
          blob.make_public()
        
        if os.path.exists(path):
          os.remove(path)
        
        if 'output_480p0.ts' in path:
          should_commit = True
          resource_id = path.split(os.path.sep)[-2]
          resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
          if resource:
            should_commit = True
            resource.is_360p_done = True
        
        if 'output_720p0.ts' in path:
          should_commit = True
          resource_id = path.split(os.path.sep)[-2]
          resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
          if resource:
            should_commit = True
            resource.is_480p_done = True
        
        if 'output_1080p0.ts' in path:
          resource_id = path.split(os.path.sep)[-2]
          resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
          if resource:
            should_commit = True
            resource.is_720p_done = True
          
        if should_commit:
          current_db_session = db.session.object_session(resource)
          if current_db_session:
            current_db_session.add(resource)
            current_db_session.commit()
          else:
            db.session.add(resource)
          db.session.commit()
        
        res_path = os.path.split(path)[0]
        files = next(os.walk(res_path))[2]
        if len(files) == 0 and 'output_1080p' in path:
          resource_id = path.split(os.path.sep)[-2]
          delete_chunk_upload(resource_id)

      except Exception as ex:
        
        print("Exception in save_stream_file: ", ex)

def is_processing_needed(type, need_processing=False):
      
      
      """Determines if a file needs video processing."""
      is_video = is_video_file(type)
      return is_video and need_processing 

      gcs_key = str(file_path.relative_to(os.getcwd()))
      blob = bucket.blob(gcs_key)
      blob.upload_from_filename(path)
      blob.make_public()
        
      if os.path.exists(path):
          os.remove(path)
        
      if 'output_480p0.ts' in path:
          should_commit = True
          resource_id = path.split(os.path.sep)[-2]
          resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
          if resource:
            should_commit = True
            resource.is_360p_done = True
        
      if 'output_720p0.ts' in path:
          should_commit = True
          resource_id = path.split(os.path.sep)[-2]
          resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
          if resource:
            should_commit = True
            resource.is_480p_done = True
        
      if 'output_1080p0.ts' in path:
          resource_id = path.split(os.path.sep)[-2]
          resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
          if resource:
            should_commit = True
            resource.is_720p_done = True
          
      if should_commit:
          current_db_session = db.session.object_session(resource)
          if current_db_session:
            current_db_session.add(resource)
            current_db_session.commit()
          else:
            db.session.add(resource)
          db.session.commit()
        
      res_path = os.path.split(path)[0]
      files = next(os.walk(res_path))[2]
      if len(files) == 0 and 'output_1080p' in path:
          resource_id = path.split(os.path.sep)[-2]
          delete_chunk_upload(resource_id)
          
          

      except Exception as ex:
    
      print("Exception in save_stream_file: ", ex)


def get_default_filepreview_by_content_type(content_type):
    if content_type in [
        'application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'application/vnd.ms-fontobject', 'application/epub+zip', 'application/vnd.oasis.opendocument.text',
        'application/x-abiword', 'application/x-freearc', 'application/epub'
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
  with requests.get(url, stream=True) as r:
    with open(filename, 'wb') as f:
      for chunk in r.iter_content(chunk_size=8192): 
          f.write(chunk)

    
def save_pdf_preview(resource, file):
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
  try:
    unique_filename = str(uuid.uuid4())
    output_image = os.path.join(f'{unique_filename}.png')

    if not resource.is_multipart:
      combined_file_name = f"{resource.id}-{resource.name}"

      ffmpeg_command = [
        'ffmpeg', '-i', combined_file_name, '-ss', '00:00:00', '-frames:v', '1', output_image
      ]
      command = subprocess.run(
        ffmpeg_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
      )
    else:
      ffmpeg_command = [
          'ffmpeg', '-i', file, '-ss', '00:00:00', '-frames:v', '1', output_image
      ]
      command = subprocess.run(
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
    print(f"Exception in saving Video preview: {ex}")

  return resource


def get_auth_token_from_company_user(company_user: str, company: str, user: str):
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
    print("Auth Token error during restart")
  
  return None

def convert_to_mp3_file(file, resource):
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
       from main import app
       from .service import delete_chunk_upload
       from . import pubsub_utils
   
       with app.app_context():
         u
         
         
         try:
           
           app.config['MP4_CONVERT_LOCK'].acquire()
         
         # For Cloud environment, we can use Pub/Sub for async processing
         if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
             # Publish a message to Pub/Sub for mp4 conversion
             pubsub_utils.publish_mp4_conversion_task(resource.id)
             return
             
         storage_client = get_storage_client()
         bucket_name = get_eino_storage_bucket_name()
         bucket = storage_client.bucket(bucket_name)
         resource_key = get_resource_storage_key(resource)
         blob = bucket.blob(resource_key)
         
         signed_url = get_signed_url(resource_key, expiration=3600, method='GET')
         output_filename = f"{resource.name.split('.')[0]}.mp4"
         output_name = f"{resource.id}-{output_filename}"
         mkv_resource_key = resource_key
         res_key = resource_key.split('/')
         res_key[-1] = output_name
         new_resource_key = '/'.join(res_key)
         
         command = ['ffmpeg', '-i', signed_url, '-c:v', 'libx264', '-c:a', 'aac', '-crf', '20', output_name]
         subprocess.run(
           command, 
           stdout=subprocess.PIPE,
           stderr=subprocess.PIPE
         )

         is_valid_file = os.stat(output_name).st_size > 0
         if is_valid_file:
           if os.path.exists(output_name):
             new_blob = bucket.blob(new_resource_key)
             new_blob.upload_from_filename(output_name)
             os.remove(output_name)

           resource.name = output_filename
           db.session.commit()
           save_resource_to_db(resource, True)
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