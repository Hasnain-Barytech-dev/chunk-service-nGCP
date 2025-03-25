import json
import uuid
from flask import request, jsonify
from . import utils
from . import service
from . import pubsub_utils
from decorators.authorize import token_required

# API View functions
@token_required
def start_chunk_upload(auth_data):
  meta = request.headers.get('Upload-Metadata')
  company_id = request.headers.get('X-Tenant-ID')
  department_id = request.headers.get('Department-Id')
  need_processing = request.args.get('need_processing') == 'true'
  file_upload_from_chat = request.headers.get('fileuploadedfromchat')
  direct_upload = request.args.get('direct_upload') == 'true'
  
  if not meta:
    return jsonify({}), 400
  
  if file_upload_from_chat == "false":
    file_upload_from_chat = False
  elif file_upload_from_chat == "true":
      file_upload_from_chat = True
  else:
      file_upload_from_chat = False 
  if not meta:
    return jsonify({}), 400

  response = service.start_chunk_upload(
    auth_data, 
    company_id, 
    meta, 
    department_id, 
    need_processing, 
    file_upload_from_chat,
    direct_upload
  )

  return utils.get_upload_response(response=json.dumps(response), status=201, extra_headers={ 'Location': response.get('id') })

def upload_chunk_data(resource_id: str):
  response = service.upload_chunk_data(resource_id)

  extra_header = {
    'Upload-Offset': response['offset']
  }

  return utils.get_upload_response(response=json.dumps(response), status=200, extra_headers=extra_header)

@token_required
def complete_direct_upload(auth_data, resource_id: str):
  """Endpoint to mark a direct upload as complete."""
  response = service.complete_direct_upload(resource_id)
  return jsonify(response), 200

@token_required
def resume_chunk_upload(auth_data, resource_id: str):
  response = service.resume_chunk_upload(resource_id)

  return utils.get_upload_response(response=json.dumps(response), status=200, extra_headers=response)

@token_required
def delete_chunk_upload(auth_data, resource_id: str):
  response = service.delete_chunk_upload(resource_id, is_abort=True)

  return jsonify({}), 204

def pubsub_handler():
  """Handles Pub/Sub push messages."""
  # Validate the request
  data = pubsub_utils.validate_pubsub_message(request)
  if not data:
    return jsonify({"status": "invalid_message"}), 400
  
  # Process the message based on task type
  task_type = data.get('task_type')
  resource_id = data.get('resource_id')
  
  if not resource_id:
    return jsonify({"status": "missing_resource_id"}), 400
  
  if task_type == 'process_file':
    # Process the file
    from .models import Resource
    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
    if resource:
      service.chunk_upload_completed(resource, need_lock=False)
  
  elif task_type == 'convert_to_mp4':
    # Convert to MP4
    from .models import Resource
    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
    if resource:
      utils.convert_to_mp4(resource)
  
  elif task_type == 'process_media':
    # Process media for HLS streaming
    file_path = data.get('file_path')
    output_folder = data.get('output_folder')
    qualities = data.get('qualities')
    
    if not all([file_path, output_folder, qualities]):
      return jsonify({"status": "missing_parameters"}), 400
    
    # Process the media
    from .models import Resource
    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
    if resource:
      utils.create_steam(file_path, resource)
  
  else:
    return jsonify({"status": "unknown_task_type"}), 400
  
  return jsonify({"status": "success"}), 200