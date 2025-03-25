import os
from flask import Flask
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from extensions import cors, db, migrate, serializer
import config
from api import api_blueprint

def create_app(env: str = 'DEV'):
  app = Flask(__name__)

  # Add Configurations based on Environment
  if env == 'DEV':
    app.config.from_object(config.DevConfig)
  elif env == 'LOCAL':
    app.config.from_object(config.LocalConfig)
  elif env == 'STAGING':
    app.config.from_object(config.StagingConfig)
  elif env == 'PRODUCTION':
    app.config.from_object(config.ProdConfig)

  with app.app_context():
    import_db_models()
    
    # Set all extensions
    cors.init_app(app)
    db.init_app(app)
    migrate.init_app(app)
    serializer.init_app(app)

  # Blueprints...
  app.register_blueprint(api_blueprint)
  
  return app

def import_db_models():
  # These are just imported so that, flask migration will take these tables during migration
  from api.chunk.models import Resource, Chunk

def observe_watchdog_events(app):
  from api.chunk.utils import save_hls_file, save_stream_file

  if not os.path.exists(app.config['WATCHDOG_FOLDER']):
    os.mkdir(app.config['WATCHDOG_FOLDER'])

  event_handler = FileSystemEventHandler()
  event_handler.on_modified = save_stream_file

  observer = Observer()
  observer.schedule(event_handler, app.config['WATCHDOG_FOLDER'], recursive=True)
  observer.start()




