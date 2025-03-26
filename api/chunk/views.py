import json
import uuid
import os
from flask import request, jsonify, current_app
from . import utils
from . import service
from . import pubsub_utils
from . import adaptive_streaming
from decorators.authorize import token_required
from .models import Resource
from extensions import db

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
    """Handles Pub/Sub push messages for adaptive streaming and file processing."""
    # Validate the request
    data = pubsub_utils.validate_pubsub_message(request)
    if not data:
        return jsonify({"status": "invalid_message"}), 400
    
    # Process the message based on task type
    task_type = data.get('task_type')
    resource_id = data.get('resource_id')
    
    if not resource_id:
        return jsonify({"status": "missing_resource_id"}), 400
    
    # Get the resource
    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
    if not resource:
        return jsonify({"status": "resource_not_found"}), 404
    
    if task_type == 'process_file':
        # Process the file
        service.chunk_upload_completed(resource, need_lock=False)
    
    elif task_type == 'convert_to_mp4':
        # Convert to MP4
        utils.convert_to_mp4(resource)
    
    elif task_type == 'process_media':
        # Process media for HLS streaming
        file_path = data.get('file_path')
        output_folder = data.get('output_folder')
        qualities = data.get('qualities')
        
        if not all([file_path, output_folder, qualities]):
            return jsonify({"status": "missing_parameters"}), 400
        
        # Process the media
        storage_client = utils.get_storage_client()
        bucket_name = utils.get_eino_storage_bucket_name()
        bucket = storage_client.bucket(bucket_name)
        
        # Generate HLS streams
        utils.generate_hls_streams(file_path, output_folder, resource, qualities, bucket)
    
    elif task_type == 'generate_dash':
        # Generate MPEG-DASH streaming assets
        file_path = data.get('file_path')
        output_folder = data.get('output_folder')
        
        if not all([file_path, output_folder]):
            return jsonify({"status": "missing_parameters"}), 400
        
        # Generate DASH manifest and segments
        dash_manifest = adaptive_streaming.generate_dash_manifest(file_path, output_folder, resource, [
            {'name': '360p', 'resolution': '640x360', 'bitrate': '1M'},
            {'name': '480p', 'resolution': '854x480', 'bitrate': '2M'},
            {'name': '720p', 'resolution': '1280x720', 'bitrate': '4M'},
            {'name': '1080p', 'resolution': '1920x1080', 'bitrate': '8M'},
        ])
        
        # Upload DASH assets to GCS
        if dash_manifest:
            storage_client = utils.get_storage_client()
            bucket_name = utils.get_eino_storage_bucket_name()
            bucket = storage_client.bucket(bucket_name)
            
            # Upload all DASH files to GCS
            adaptive_streaming.upload_streaming_assets(
                os.path.dirname(dash_manifest),
                output_folder,
                bucket
            )
            
            # Update resource with DASH URL
            dash_url = f"https://storage.googleapis.com/{bucket_name}/{output_folder}/manifest.mpd"
            resource.dash_url = dash_url
            db.session.commit()
            
            # Save resource to DB with updated URL
            utils.save_resource_to_db(resource, need_auth=True)
    
    else:
        return jsonify({"status": "unknown_task_type"}), 400
    
    return jsonify({"status": "success"}), 200

@token_required
def get_streaming_url(auth_data, resource_id: str):
    """Get streaming URLs for a resource."""
    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
    if not resource:
        return jsonify({"error": "Resource not found"}), 404
    
    # Get adaptive streaming URLs
    streaming_urls = adaptive_streaming.get_adaptive_streaming_urls(resource)
    
    return jsonify({
        "resource_id": resource_id,
        "name": resource.name,
        "type": resource.type,
        "streaming_urls": streaming_urls
    }), 200

@token_required
def get_transcoding_status(auth_data, resource_id: str):
    """Get the current transcoding status for a resource."""
    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
    if not resource:
        return jsonify({"error": "Resource not found"}), 404
    
    # Check transcoding progress
    progress = adaptive_streaming.monitor_transcoding_progress(resource_id)
    
    return jsonify(progress), 200

@token_required
def check_video_compatibility(auth_data):
    """
    Check if an uploaded video is compatible with adaptive streaming.
    This helps provide early feedback before processing starts.
    """
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
        
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
        
    # Save to temporary location
    temp_path = f"/tmp/{uuid.uuid4()}{os.path.splitext(file.filename)[1]}"
    file.save(temp_path)
    
    try:
        # Check compatibility
        compatibility = adaptive_streaming.check_file_compatibility(temp_path)
        return jsonify(compatibility), 200
    finally:
        # Clean up temporary file
        if os.path.exists(temp_path):
            os.remove(temp_path)

@token_required
def start_adaptive_streaming_job(auth_data, resource_id: str):
    """
    Manually start or restart adaptive streaming job for a resource.
    Useful for retrying failed transcoding jobs.
    """
    resource = Resource.query.filter_by(id=resource_id, is_deleted=False).first()
    if not resource:
        return jsonify({"error": "Resource not found"}), 404
    
    if not utils.is_video_file(resource.type):
        return jsonify({"error": "Resource is not a video file"}), 400
    
    # Get resource storage key and signed URL
    resource_key = utils.get_resource_storage_key(resource)
    signed_url = utils.get_signed_url(resource_key, expiration=3600)
    
    # Define output folder
    output_folder = f"hls_media/{resource.company}/{resource.created_by}/{resource.id}"
    
    # Define quality variants
    qualities = [
        {'name': '360p', 'resolution': '640x360', 'bitrate': '1M', 'crf': '28', 'bandwidth': '1000000'},
        {'name': '480p', 'resolution': '854x480', 'bitrate': '2M', 'crf': '26', 'bandwidth': '2000000'},
        {'name': '720p', 'resolution': '1280x720', 'bitrate': '4M', 'crf': '24', 'bandwidth': '4000000'},
        {'name': '1080p', 'resolution': '1920x1080', 'bitrate': '8M', 'crf': '22', 'bandwidth': '8000000'}
    ]
    
    # Reset quality flags if re-processing
    resource.is_360p_done = False
    resource.is_480p_done = False
    resource.is_720p_done = False
    resource.is_1080p_done = False
    resource.need_processing = True
    db.session.commit()
    
    # Submit job
    if current_app.config.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', False):
        message_id = pubsub_utils.publish_media_processing_task(
            resource.id, 
            signed_url, 
            output_folder, 
            qualities
        )
        return jsonify({
            "status": "processing_started",
            "message_id": message_id,
            "resource_id": resource_id
        }), 202
    else:
        # Process immediately
        threading.Thread(
            target=utils.create_stream,
            args=(signed_url, resource)
        ).start()
        
        return jsonify({
            "status": "processing_started",
            "resource_id": resource_id
        }), 202
