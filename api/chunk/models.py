from extensions import db
from sqlalchemy.orm import Mapped, mapped_column
import uuid

from . import utils

class Resource(db.Model):
  __tablename__ = 'resource'

  id = db.Column(db.String(100), unique=True, primary_key=True, default=utils.get_random_uuid)
  name = db.Column(db.String(500), nullable=True, default='')
  type = db.Column(db.String(100), nullable=True, default='')
  directory = db.Column(db.String(1000), nullable=True, default='')
  size = db.Column(db.BigInteger, nullable=False)
  offset = db.Column(db.BigInteger, nullable=True, default=0)
  paused = db.Column(db.Boolean, default=False)
  status = db.Column(db.String(100), default='CHUNK_UPLOADING')
  is_completed = db.Column(db.Boolean, default=False)
  chunks_uploaded = db.Column(db.BigInteger, default=0)
  preview_image = db.Column(db.String(250), nullable=True)

  created_by = db.Column(db.String(250), nullable=True)
  company = db.Column(db.String(250), nullable=True)
  company_user = db.Column(db.String(250), nullable=True)
  department = db.Column(db.String(250), nullable=True)

  is_360p_done = db.Column(db.Boolean, default=False)
  is_480p_done = db.Column(db.Boolean, default=False)
  is_720p_done = db.Column(db.Boolean, default=False)
  is_1080p_done = db.Column(db.Boolean, default=False)
  upload_id = db.Column(db.String(250), nullable=True)
  is_multipart = db.Column(db.Boolean, default=False)
  need_processing = db.Column(db.Boolean, default=False)
  is_deleted = db.Column(db.Boolean, default=False)
  file_upload_from_chat = db.Column(db.Boolean, default=False)
  chunks = db.relationship('Chunk', backref='resource', lazy='dynamic')


class Chunk(db.Model):
  __tablename__ = 'resource_chunks'

  id = db.Column(db.String(120), unique=True, primary_key=True, default=utils.get_random_uuid)
  chunk_index = db.Column(db.Integer, nullable=True)
  data_key = db.Column(db.String(1000), nullable=False)
  tag = db.Column(db.String(1000), nullable=True)
  is_deleted = db.Column(db.Boolean, default=False)

  resource_id = db.Column(db.String(120), db.ForeignKey('resource.id'), nullable=False)
  


