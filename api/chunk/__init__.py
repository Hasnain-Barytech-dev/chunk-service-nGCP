from flask import Blueprint
from . import views

upload_blueprint = Blueprint('chunk_blueprint', __name__, url_prefix='/chunk')
api_blueprint = upload_blueprint

# Basic chunk upload endpoints
upload_blueprint.add_url_rule('/upload/', 'upload', methods=['POST'], view_func=views.start_chunk_upload)
upload_blueprint.add_url_rule('/upload/<resource_id>', 'upload_chunk', methods=['PATCH'], view_func=views.upload_chunk_data)
upload_blueprint.add_url_rule('/upload/<resource_id>', 'resume_chunk', methods=['HEAD'], view_func=views.resume_chunk_upload)
upload_blueprint.add_url_rule('/upload/<resource_id>', 'delete_chunk', methods=['DELETE'], view_func=views.delete_chunk_upload)

# Direct upload completion endpoint
upload_blueprint.add_url_rule('/upload/<resource_id>/complete', 'complete_direct_upload', methods=['POST'], view_func=views.complete_direct_upload)

# Pub/Sub push notification endpoint
upload_blueprint.add_url_rule('/pubsub', 'pubsub_handler', methods=['POST'], view_func=views.pubsub_handler)

# Adaptive streaming endpoints
upload_blueprint.add_url_rule('/streaming/<resource_id>/url', 'get_streaming_url', methods=['GET'], view_func=views.get_streaming_url)
upload_blueprint.add_url_rule('/streaming/<resource_id>/status', 'get_transcoding_status', methods=['GET'], view_func=views.get_transcoding_status)
upload_blueprint.add_url_rule('/streaming/check-compatibility', 'check_video_compatibility', methods=['POST'], view_func=views.check_video_compatibility)
upload_blueprint.add_url_rule('/streaming/<resource_id>/start', 'start_adaptive_streaming_job', methods=['POST'], view_func=views.start_adaptive_streaming_job)

# Alias for backward compatibility
upload_blueprint = Blueprint('chunk_blueprint', __name__, url_prefix='/chunk')
