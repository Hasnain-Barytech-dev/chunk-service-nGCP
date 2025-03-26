import json
import logging
import base64
from google.cloud import pubsub_v1
from flask import current_app

def get_publisher_client():
    """Returns a Pub/Sub publisher client."""
    # Check if credentials are specified, otherwise use default service account
    credentials_path = current_app.config.get('GCP_SERVICE_ACCOUNT_FILE')
    project_id = current_app.config.get('GCP_PROJECT_ID')
    
    if credentials_path:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        return pubsub_v1.PublisherClient(credentials=credentials)
    return pubsub_v1.PublisherClient()

def publish_message(topic_name, message_data):
    """Publishes a message to the specified Pub/Sub topic."""
    try:
        project_id = current_app.config.get('GCP_PROJECT_ID')
        publisher = get_publisher_client()
        topic_path = publisher.topic_path(project_id, topic_name)
        
        message_bytes = json.dumps(message_data).encode('utf-8')
        future = publisher.publish(topic_path, data=message_bytes)
        message_id = future.result()
        
        logging.info(f"Published message with ID: {message_id} to topic: {topic_path}")
        return message_id
    except Exception as e:
        logging.error(f"Error publishing message to Pub/Sub: {e}")
        raise

def publish_file_processing_task(resource_id):
    """Publishes a file processing task to the Pub/Sub topic."""
    topic_name = current_app.config.get('PUBSUB_FILE_PROCESSING_TOPIC')
    message_data = {
        'resource_id': resource_id,
        'task_type': 'process_file'
    }
    return publish_message(topic_name, message_data)

def publish_mp4_conversion_task(resource_id):
    """Publishes an MP4 conversion task to the Pub/Sub topic."""
    topic_name = current_app.config.get('PUBSUB_MEDIA_PROCESSING_TOPIC')
    message_data = {
        'resource_id': resource_id,
        'task_type': 'convert_to_mp4'
    }
    return publish_message(topic_name, message_data)

def publish_media_processing_task(resource_id, file_path, output_folder, qualities):
    """
    Publishes a media processing task to the Pub/Sub topic for HLS streaming generation.
    
    Args:
        resource_id: ID of the resource to process
        file_path: Path to the source video file
        output_folder: Output folder for HLS segments and playlists
        qualities: List of quality definitions for transcoding
        
    Returns:
        Message ID if successful
    """
    topic_name = current_app.config.get('PUBSUB_MEDIA_PROCESSING_TOPIC')
    message_data = {
        'resource_id': resource_id,
        'file_path': file_path,
        'output_folder': output_folder,
        'qualities': qualities,
        'task_type': 'process_media'
    }
    return publish_message(topic_name, message_data)

def publish_dash_generation_task(resource_id, file_path, output_folder):
    """
    Publishes a DASH generation task to the Pub/Sub topic.
    
    Args:
        resource_id: ID of the resource to process
        file_path: Path to the source video file
        output_folder: Output folder for DASH segments and manifest
        
    Returns:
        Message ID if successful
    """
    topic_name = current_app.config.get('PUBSUB_MEDIA_PROCESSING_TOPIC')
    message_data = {
        'resource_id': resource_id,
        'file_path': file_path,
        'output_folder': output_folder,
        'task_type': 'generate_dash'
    }
    return publish_message(topic_name, message_data)

def publish_thumbnail_generation_task(resource_id, file_path, output_folder, timestamps=None):
    """
    Publishes a thumbnail generation task to the Pub/Sub topic.
    
    Args:
        resource_id: ID of the resource to process
        file_path: Path to the source video file
        output_folder: Output folder for thumbnails
        timestamps: Optional list of timestamps for thumbnail extraction
        
    Returns:
        Message ID if successful
    """
    topic_name = current_app.config.get('PUBSUB_MEDIA_PROCESSING_TOPIC')
    message_data = {
        'resource_id': resource_id,
        'file_path': file_path,
        'output_folder': output_folder,
        'timestamps': timestamps or [0],  # Default to first frame
        'task_type': 'generate_thumbnails'
    }
    return publish_message(topic_name, message_data)

def create_subscription(subscription_id, topic_id, endpoint_url):
    """Creates a push subscription to a Pub/Sub topic with the given endpoint URL."""
    try:
        project_id = current_app.config.get('GCP_PROJECT_ID')
        subscriber = pubsub_v1.SubscriberClient()
        topic_path = subscriber.topic_path(project_id, topic_id)
        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        
        push_config = pubsub_v1.types.PushConfig(
            push_endpoint=endpoint_url
        )
        
        subscription = subscriber.create_subscription(
            request={
                "name": subscription_path,
                "topic": topic_path,
                "push_config": push_config,
            }
        )
        
        logging.info(f"Created push subscription: {subscription.name}")
        return subscription
    except Exception as e:
        logging.error(f"Error creating subscription: {e}")
        raise

def validate_pubsub_message(request):
    """Validates that a request is a Pub/Sub message.
    
    Returns:
        The message data if valid, None otherwise.
    """
    try:
        # Check request format
        envelope = json.loads(request.data.decode('utf-8'))
        
        if not envelope:
            logging.error("Invalid Pub/Sub message: no JSON data")
            return None
            
        if 'message' not in envelope:
            logging.error("Invalid Pub/Sub message: no message field")
            return None
            
        # Extract message data
        pubsub_message = envelope['message']
        
        if 'data' not in pubsub_message:
            logging.error("Invalid Pub/Sub message: no data field")
            return None
            
        # Decode message data - using base64 module for better compatibility
        try:
            decoded_data = base64.b64decode(pubsub_message['data'])
            data = json.loads(decoded_data)
        except Exception as decode_error:
            logging.error(f"Error decoding base64 data: {decode_error}")
            # Try legacy format as fallback
            try:
                data = json.loads(pubsub_message['data'].decode('base64'))
            except Exception as legacy_error:
                logging.error(f"Error with legacy decoding: {legacy_error}")
                return None
        
        return data
    except Exception as e:
        logging.error(f"Error validating Pub/Sub message: {e}")
        return None

def delete_subscription(subscription_id):
    """Deletes a Pub/Sub subscription."""
    try:
        project_id = current_app.config.get('GCP_PROJECT_ID')
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        
        subscriber.delete_subscription(request={"subscription": subscription_path})
        logging.info(f"Deleted subscription: {subscription_path}")
    except Exception as e:
        logging.error(f"Error deleting subscription: {e}")
        raise
