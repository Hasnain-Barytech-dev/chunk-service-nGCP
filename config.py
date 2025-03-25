import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

class Config(object):
  environment = 'DEV'
  DEBUG = True
  SECRET_KEY = os.environ.get('SECRET_KEY', 'django-insecure-fcoz5ytg^w9*foncl#_a4o%hpr&e5u@d*g_8ia_!ll(p0$tg*d')

  # GCP Configuration
  GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'eino-app')
  GCP_SERVICE_ACCOUNT_FILE = os.environ.get('GCP_SERVICE_ACCOUNT_FILE', 'service-account.json')
  
  # GCS Configuration (replacing AWS S3)
  GCS_STORAGE_BUCKET_NAME = os.environ.get('GCS_STORAGE_BUCKET_NAME', 'eino-video-processing-dev')
  GCS_STORAGE_EINO_BUCKET_NAME = os.environ.get('GCS_STORAGE_EINO_BUCKET_NAME', 'eino-bucket-dev')
  
  # Pub/Sub Configuration
  PUBSUB_FILE_PROCESSING_TOPIC = os.environ.get('PUBSUB_FILE_PROCESSING_TOPIC', 'file-processing')
  PUBSUB_MEDIA_PROCESSING_TOPIC = os.environ.get('PUBSUB_MEDIA_PROCESSING_TOPIC', 'media-processing')
  USE_PUBSUB_FOR_MEDIA_PROCESSING = os.environ.get('USE_PUBSUB_FOR_MEDIA_PROCESSING', 'false').lower() == 'true'
  
  # Django API URL
  DJANGO_BASE_URL = os.environ.get('DJANGO_BASE_URL', 'https://dev-api.eino.world')
  
  # Database Configuration
  SQLALCHEMY_POOL_RECYCLE = 1800
  SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_pre_ping": True,
    "pool_size": int(os.environ.get('DB_POOL_SIZE', '5')),
    "max_overflow": int(os.environ.get('DB_MAX_OVERFLOW', '10'))
  }
  SQLALCHEMY_TRACK_MODIFICATIONS = False

  # Thread Configuration
  THREAD_MAX_WORKERS = int(os.environ.get('THREAD_MAX_WORKERS', '4'))
  THREAD_POOL_EXECUTOR = ThreadPoolExecutor(max_workers=int(os.environ.get('THREAD_MAX_WORKERS', '4')))
  CHUNK_COMPLETION_LOCK = Lock()
  FILE_SAVE_LOCK = Lock()
  MULTIPART_FILESIZE = int(os.environ.get('MULTIPART_FILESIZE', '10485760'))  # 10MB
  MP4_CONVERT_LOCK = Lock()

  WATCHDOG_FOLDER = os.path.join(os.getcwd(), 'hls_media')


class LocalConfig(Config):
  DATABASE_USERNAME = os.environ.get('DB_USERNAME', 'postgres')
  DATABASE_PASSWORD = os.environ.get('DB_PASSWORD', 'password')
  DATABASE_URL = os.environ.get('DB_HOST', 'localhost:5432')
  DATABASE_NAME = os.environ.get('DB_NAME', 'CHUNK_LOCAL')
  SQLALCHEMY_DATABASE_URI = f'postgresql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_URL}/{DATABASE_NAME}'
  
  GCS_STORAGE_BUCKET_NAME = os.environ.get('GCS_STORAGE_BUCKET_NAME', 'eino-video-processing-dev')
  GCS_STORAGE_EINO_BUCKET_NAME = os.environ.get('GCS_STORAGE_EINO_BUCKET_NAME', 'eino-bucket-dev')

  DJANGO_BASE_URL = os.environ.get('DJANGO_BASE_URL', 'https://dev-api.eino.world')
  TEMPLATE_IMAGES_PATH = str(os.getcwd())
  USE_PUBSUB_FOR_MEDIA_PROCESSING = False


class DevConfig(Config):
  DATABASE_USERNAME = os.environ.get('DB_USERNAME', 'postgres')
  DATABASE_PASSWORD = os.environ.get('DB_PASSWORD', 'password')
  DATABASE_URL = os.environ.get('DB_HOST', 'localhost:5432')
  DATABASE_NAME = os.environ.get('DB_NAME', 'CHUNK_DEV')
  SQLALCHEMY_DATABASE_URI = f'postgresql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_URL}/{DATABASE_NAME}'
  
  GCS_STORAGE_BUCKET_NAME = os.environ.get('GCS_STORAGE_BUCKET_NAME', 'eino-video-processing-dev')
  GCS_STORAGE_EINO_BUCKET_NAME = os.environ.get('GCS_STORAGE_EINO_BUCKET_NAME', 'eino-bucket-dev')

  DJANGO_BASE_URL = os.environ.get('DJANGO_BASE_URL', 'https://dev-api.eino.world')
  TEMPLATE_IMAGES_PATH = os.path.join(os.getcwd(), 'chunk-service')


class StagingConfig(Config):
  DATABASE_USERNAME = os.environ.get('DB_USERNAME', 'postgres')
  DATABASE_PASSWORD = os.environ.get('DB_PASSWORD', 'password')
  DATABASE_URL = os.environ.get('DB_HOST', 'localhost:5432')
  DATABASE_NAME = os.environ.get('DB_NAME', 'CHUNK_STAGING')
  SQLALCHEMY_DATABASE_URI = f'postgresql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_URL}/{DATABASE_NAME}'
  
  GCS_STORAGE_BUCKET_NAME = os.environ.get('GCS_STORAGE_BUCKET_NAME', 'eino-video-processing-staging')
  GCS_STORAGE_EINO_BUCKET_NAME = os.environ.get('GCS_STORAGE_EINO_BUCKET_NAME', 'eino-bucket-staging')

  DJANGO_BASE_URL = os.environ.get('DJANGO_BASE_URL', 'https://staging-api.eino.world')
  TEMPLATE_IMAGES_PATH = os.path.join(os.getcwd(), 'chunk-service')


class ProdConfig(Config):
  DATABASE_USERNAME = os.environ.get('DB_USERNAME', 'postgres')
  DATABASE_PASSWORD = os.environ.get('DB_PASSWORD', 'password')
  DATABASE_URL = os.environ.get('DB_HOST', 'localhost:5432')
  DATABASE_NAME = os.environ.get('DB_NAME', 'CHUNK_PROD')
  SQLALCHEMY_DATABASE_URI = f'postgresql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_URL}/{DATABASE_NAME}'
  
  GCS_STORAGE_BUCKET_NAME = os.environ.get('GCS_STORAGE_BUCKET_NAME', 'eino-video-processing-prod')
  GCS_STORAGE_EINO_BUCKET_NAME = os.environ.get('GCS_STORAGE_EINO_BUCKET_NAME', 'einome-bucket')

  DJANGO_BASE_URL = os.environ.get('DJANGO_BASE_URL', 'https://api.eino.world')
  TEMPLATE_IMAGES_PATH = os.path.join(os.getcwd(), 'chunk-service')