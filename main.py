import os
from app import create_app
from api.chunk.service import cleanup_and_restart_processing
from config import Config
import threading

# Get the PORT from the environment (for Cloud Run)
port = int(os.environ.get('PORT', 8181))

# Determine environment from the environment variable
environment = os.environ.get('FLASK_ENV', Config.environment)

app = create_app(environment)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Cloud Run."""
    return {'status': 'healthy'}, 200

@app.teardown_request
def session_clear(exception=None):
    from extensions import db
    db.session.remove()
    if exception and db.session.is_active:
        db.session.rollback()

if __name__ == '__main__':
    # Only do cleanup in non-Cloud Run environments
    # In Cloud Run, each instance is ephemeral, so cleanup is unnecessary
    is_cloud_run = os.environ.get('K_SERVICE', '') != ''
    
    if not is_cloud_run:
        # Start cleanup thread for non-Cloud Run environments
        threading.Thread(target=cleanup_and_restart_processing).start()

    # Run the app
    app.run(host='0.0.0.0', port=port, use_reloader=False)