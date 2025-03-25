import os
import time
import io
import json
import uuid
import shutil
import requests
import logging
from flask import request, jsonify, current_app
from . import utils
from . import pubsub_utils
from .models import Resource, Chunk
from extensions import db
from sqlalchemy import asc
import threading

def start_chunk_upload(auth_data, company_id, meta: str, department_id: str, need_processing: bool = False, file_upload_from_chat: bool = False, direct_upload: bool = False):
  length = request.headers.get('Upload-Length')
  metadata = utils.get_metadata(meta or '')
  is_multipart = True

  # Create the resource record
  resource = Resource(
    name=metadata.get('filename'),
    type=metadata.get('filetype'),
    directory=metadata.get('filedirectory'),
    size=length,
    chunks=[],
    created_by = auth_data['user']['uuid'],
    company = company_id,
    company_user = auth_data['company_user']['id'],
    department = department_id,
    is_multipart = is_multipart,
    need_processing = need_processing,
    file_upload_from_chat = file_upload_from_chat
  )
  db.session.add(resource)
  db.session.commit()  

  # For direct uploads, generate a signed URL for the browser to upload directly to GCS
  if direct_upload:
    session_uri = utils.create_resumable_upload_session(resource)
    resource.upload_id = session_uri
    db.session.commit()
    return { **metadata, 'id': f"{resource.id}", 'upload_url': session_uri }
  
  # For traditional uploads through the server
  else:
    storage_client = utils.get_storage_client()
    bucket_name = utils.get_eino_storage_bucket_name()
    key = utils.get_resource_storage_key(resource)
    
    if is_multipart:
      # Create a resumable upload session
      session_uri = utils.create_resumable_upload_session(resource)
      resource.upload_id = session_uri
      db.session.commit()

  return { **metadata, 'id': f"{resource.id}" }

def upload_chunk_data(resource_id: str):
  data = request.data
  resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
  if resource is None:
    raise Exception('Resource not found')
  
  file_upload_from_chat = resource.file_upload_from_chat

  chunk_id = f"{uuid.uuid4()}" 
  part = { 'ETag': None }

  storage_client = utils.get_storage_client()
  bucket_name = utils.get_eino_storage_bucket_name()
  bucket = storage_client.bucket(bucket_name)
  key = utils.get_resource_storage_key(resource)
  file_size = 0
  
  if resource.is_multipart:
    chunk_key = ''
    file_size = utils.get_chunk_file_size(chunk_id)
    
    # For GCS, we append to the upload
    blob = bucket.blob(key)
    
    # If it's the first chunk, we need to create a new upload
    if resource.chunks_uploaded == 0:
      with io.BytesIO(data) as f:
        blob.upload_from_file(f, size=len(data), content_type=resource.type)
    else:
      # Append the chunk to the existing object using a resumable upload
      with io.BytesIO(data) as f:
        blob.upload_from_file(f, size=len(data), content_type=resource.type)
  else:
    utils.create_chunk_file(f"{chunk_id}", data)
    try:
      chunk_key = utils.save_chunk_to_storage(resource_id, f"{chunk_id}")
      file_size = utils.get_chunk_file_size(chunk_id)
    except Exception as ex:
      print("Exception in file upload: ", ex)
    finally:
      utils.delete_chunk_file(f"{chunk_id}")

  chunk = Chunk(
    id = chunk_id,
    chunk_index = len(resource.chunks.all()) + 1,
    data_key = chunk_key,
    tag=part.get('ETag'),
    resource = resource
  )

  resource.offset += file_size
  resource.chunks_uploaded += 1
  db.session.add_all([chunk, resource])
  db.session.commit()

  # These fields were needed because, if chunk upload is completed, resource and chunks will get deleted from db
  # So, we need it to send as response
  chunk_id = chunk.id
  chunk_index = resource.chunks_uploaded
  
  if resource.offset >= resource.size:
    resource.status = 'UPLOAD_FINISHED'
    resource.is_completed = True

    if resource.is_multipart:
      file_size = resource.size % current_app.config['MULTIPART_FILESIZE']

    resource.offset = resource.size

    db.session.add_all([chunk, resource])
    db.session.commit()

    if resource.is_multipart:
      if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
        # Publish a message to process the file
        pubsub_utils.publish_file_processing_task(resource.id)
      else:
        # Process locally
        try:
          file = utils.get_signed_url(key)
          resource = utils.save_preview_image(resource, file)
        
          if utils.is_processing_needed(resource.type, resource.need_processing):
            threading.Thread(target=utils.convert_to_mp4, args=(resource,)).start()
        except Exception as ex:
          print("Exception in save preview: ", ex)
        finally:
          delete_chunk_upload(resource.id)
    else:
      is_video = utils.is_video_file(resource.type)
      need_processing = utils.is_processing_needed(resource.type, resource.need_processing)
      if need_processing:
        if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
          # Publish a message to process the file
          pubsub_utils.publish_file_processing_task(resource.id)
        else:
          threading.Thread(target=chunk_upload_completed, kwargs={'resource': resource}).start()
      else:
        resource = chunk_upload_completed(resource, need_lock=False)

    utils.save_resource_to_db(resource, need_auth=True, fileUploadFromChat=file_upload_from_chat)

  return {
    "id": chunk_id,
    "index": chunk_index,
    "size": file_size,
    'offset': resource.offset
  }

def complete_direct_upload(resource_id: str):
  """Completes a direct upload from browser to GCS."""
  resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
  if resource is None:
    raise Exception('Resource not found')
  
  file_upload_from_chat = resource.file_upload_from_chat
  
  # Mark resource as completed
  resource.status = 'UPLOAD_FINISHED'
  resource.is_completed = True
  resource.offset = resource.size
  
  db.session.add(resource)
  db.session.commit()
  
  # Process the file
  if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
    # Publish a message to process the file
    pubsub_utils.publish_file_processing_task(resource.id)
  else:
    # Process locally
    key = utils.get_resource_storage_key(resource)
    try:
      file = utils.get_signed_url(key)
      resource = utils.save_preview_image(resource, file)
    
      if utils.is_processing_needed(resource.type, resource.need_processing):
        threading.Thread(target=utils.convert_to_mp4, args=(resource,)).start()
    except Exception as ex:
      print("Exception in save preview: ", ex)
  
  # Save to the database
  utils.save_resource_to_db(resource, need_auth=True, fileUploadFromChat=file_upload_from_chat)
  
  return {
    "status": "success",
    "message": "Direct upload completed successfully"
  }

def chunk_upload_completed(resource: Resource, is_restart=False, need_lock=True):
  from main import app
  with app.app_context():
    combined_file_name = ''
    try:
      if need_lock:
        app.config['CHUNK_COMPLETION_LOCK'].acquire()
      storage_client = utils.get_storage_client()
      bucket_name = utils.get_eino_storage_bucket_name()
      bucket = storage_client.bucket(bucket_name)

      is_video = utils.is_video_file(resource.type)
      need_processing = utils.is_processing_needed(resource.type, resource.need_processing)

      resource = Resource.query.filter_by(id=resource.id, is_deleted=False).first()
      combined_file_name = f"{resource.id}-{resource.name}"
      if resource.is_multipart:
        # In GCS, we don't need to combine parts, as the object is already created
        is_video = utils.is_video_file(resource.type)
        need_processing = utils.is_processing_needed(resource.type, resource.need_processing)
        if need_processing:
          if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
            pubsub_utils.publish_mp4_conversion_task(resource.id)
          else:
            utils.convert_to_mp4(resource)
      else:
        combined_file = combine_chunks(resource)

        with open(combined_file_name, 'wb') as f:
          f.write(combined_file.getbuffer())

        try:
          resource = utils.save_preview_image(resource, combined_file)
        except Exception as ex:
          print("Exception in saving preview image: ", ex)
        resource_key = utils.get_resource_storage_key(resource)

        if not utils.is_audio_file(resource.type):
          blob = bucket.blob(resource_key)
          blob.upload_from_file(combined_file)
        else:
          mp3_audio_file = utils.convert_to_mp3_file(combined_file, resource)
          blob = bucket.blob(resource_key)
          blob.upload_from_file(mp3_audio_file)
          if os.path.exists(f'{resource.id}.mp3'):
            os.remove(f'{resource.id}.mp3')

        need_processing = utils.is_processing_needed(resource.type, resource.need_processing)
        if need_processing:
          if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
            pubsub_utils.publish_mp4_conversion_task(resource.id)
          else:
            utils.convert_to_mp4(resource)

        resource.status = 'UPLOAD_FINISHED'
        current_db_session = db.session.object_session(resource)
        if current_db_session:
          current_db_session.add(resource)
          current_db_session.commit()
        else:
          db.session.add(resource)
        
        db.session.commit()
        combined_file.close()

      utils.save_resource_to_db(resource, need_auth=True)

      delete_chunk_upload(resource.id)
      if os.path.exists(combined_file_name):
        os.remove(combined_file_name)
    except Exception as ex: 
      logging.error(f"Error in chunk_upload complete: {ex}")
    finally:
      if need_lock and app.config['CHUNK_COMPLETION_LOCK'].locked():
        app.config['CHUNK_COMPLETION_LOCK'].release()

      if combined_file_name and os.path.exists(combined_file_name):
        os.remove(combined_file_name)

  return resource

def combine_chunks(resource: Resource):
  from main import app

  combined_file = io.BytesIO()

  with app.app_context():
    resource = Resource.query.filter_by(id=resource.id, is_deleted=False).first()

    chunks = resource.chunks.order_by(asc(Chunk.chunk_index)).all()
    storage_client = utils.get_storage_client()
    bucket_name = utils.get_storage_bucket_name()
    bucket = storage_client.bucket(bucket_name)

    for chunk in chunks:
      blob = bucket.blob(chunk.data_key)
      data = blob.download_as_bytes()
      combined_file.write(data)
    combined_file.seek(0)

  return combined_file

def resume_chunk_upload(resource_id: str):
  time.sleep(2)
  resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
  if resource is None:
    return jsonify({}), 400

  res = {
    'Upload-Length': resource.size,
    'Upload-Offset': resource.offset,
  }

  return jsonify(res)

def delete_chunk_upload(resource_id: str, is_abort=False):
  try:
    time.sleep(2)
    resource = Resource.query.filter_by(id=resource_id).first()
    if resource is None:
      return jsonify({}), 400

    # For GCS, we can delete the object if needed
    if resource.is_multipart and is_abort:
      storage_client = utils.get_storage_client()
      bucket_name = utils.get_eino_storage_bucket_name()
      bucket = storage_client.bucket(bucket_name)
      key = utils.get_resource_storage_key(resource)
      blob = bucket.blob(key)
      if blob.exists():
        blob.delete()

    utils.delete_chunks(resource)
  except Exception as ex:
    print("Exception in delete chunk upload: ", ex)

def cleanup_and_restart_processing():
  try:
    from .models import Resource
    from app import observe_watchdog_events
    from main import app
    with app.app_context():
      print("Cleaning up...")
      if os.path.exists(app.config['WATCHDOG_FOLDER']):
        shutil.rmtree(app.config['WATCHDOG_FOLDER'])
      print("Cleanup done!")
      
      if not os.path.exists(app.config['WATCHDOG_FOLDER']):
        os.mkdir(app.config['WATCHDOG_FOLDER'])
      
      observe_watchdog_events(app)

      resources = Resource.query.filter_by(is_deleted=False).all() or []
      for resource in resources:
        app.config['CHUNK_COMPLETION_LOCK'].acquire()
        if resource.status in ['UPLOAD_FINISHED', 'VIDEO_PROCESSING']:
          # For Cloud Run environment, we should use Pub/Sub for async processing
          if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
            pubsub_utils.publish_file_processing_task(resource.id)
          else:
            threading.Thread(target=chunk_upload_completed, kwargs={'resource': resource, 'is_restart': True}).start()
        else:
          delete_chunk_upload(resource.id)
    
        if app.config['CHUNK_COMPLETION_LOCK'].locked():
          app.config['CHUNK_COMPLETION_LOCK'].release()
  except Exception as ex:
    logging.error(f"Exception in cleanup_and_restart_processing : {ex}")
  finally:
    if app.config['CHUNK_COMPLETION_LOCK'].locked():
      app.config['CHUNK_COMPLETION_LOCK'].release()