from flask import Blueprint
from . import views

upload_blueprint = Blueprint('chunk_blueprint', __name__, url_prefix='/chunk')

# URL Mappings...

upload_blueprint.add_url_rule('/upload/', 'upload', methods=['POST'], view_func=views.start_chunk_upload)
upload_blueprint.add_url_rule('/upload/<resource_id>', 'upload_chunk', methods=['PATCH'], view_func=views.upload_chunk_data)
upload_blueprint.add_url_rule('/upload/<resource_id>', 'resume_chunk', methods=['HEAD'], view_func=views.resume_chunk_upload)
upload_blueprint.add_url_rule('/upload/<resource_id>', 'delete_chunk', methods=['DELETE'], view_func=views.delete_chunk_upload)
