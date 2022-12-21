import base64
import json
import os

from google.cloud import pubsub_v1 , storage, speech, translate_v2 as translate, vision, videointelligence
from google.cloud.videointelligence import Feature as detectFeature


vision_client = vision.ImageAnnotatorClient()
translate_client = translate.Client()
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()
speech_client = speech.SpeechClient()
video_client = videointelligence.VideoIntelligenceServiceClient()


project_id = os.environ["GCP_PROJECT"]
bucket_name = "bucket_api_results"


def process_video(event, context):
    # Create  a list of features to be extracted from the video
    features = [    detectFeature.OBJECT_TRACKING,
                    detectFeature.LABEL_DETECTION,
                    detectFeature.SHOT_CHANGE_DETECTION,
                    detectFeature.SPEECH_TRANSCRIPTION,
                    detectFeature.LOGO_RECOGNITION,
                    detectFeature.EXPLICIT_CONTENT_DETECTION,
                    detectFeature.TEXT_DETECTION,
                    detectFeature.FACE_DETECTION,
                    detectFeature.PERSON_DETECTION
                ]

    # Create a speech transcription configoration
    transcript_config = videointelligence.SpeechTranscriptionConfig(
    language_code="en-US", enable_automatic_punctuation=True
    )

    # Create a person detection configoration
    person_config = videointelligence.PersonDetectionConfig(
        include_bounding_boxes=True,
        include_attributes=False,
        include_pose_landmarks=True,
    )

    # Create a face detection configoration
    face_config = videointelligence.FaceDetectionConfig(
        include_bounding_boxes=True, include_attributes=True
    )

    # Create a video context with the above configorations
    video_context = videointelligence.VideoContext(
        speech_transcription_config=transcript_config,
        person_detection_config=person_config,
        face_detection_config=face_config)

    gcs_uri = f"gs://{event['bucket']}/{event['name']}"
    json_name = event['name'].split(".")[0]
    output_uri = f"gs://{bucket_name}/{json_name}.json"

    # Start the video annotation request and save to the result to the output_uri
    operation = video_client.annotate_video(
        request={"features": features,
                 "input_uri": gcs_uri,
                 "output_uri": output_uri,
                 "video_context": video_context}
    )

    print("\nProcessing video.", operation)

    result = operation.result(timeout=300)

    print("\n finnished processing.")
    

def detect_text(bucket, filename):
    print("Looking for text in image {}".format(filename))

    futures = []

    image = vision.Image(
        source=vision.ImageSource(gcs_image_uri=f"gs://{bucket}/{filename}")
    )
    # Detect text in the image and extract the text
    text_detection_response = vision_client.text_detection(image=image)
    annotations = text_detection_response.text_annotations
    if len(annotations) > 0:
        text = annotations[0].description
    else:
        text = ""
    print("Extracted text {} from image ({} chars).".format(text, len(text)))

    # Detect the language of the text
    detect_language_response = translate_client.detect_language(text)
    src_lang = detect_language_response["language"]
    print("Detected language {} for text {}.".format(src_lang, text))

    # Submit a message to the bus for each target language
    to_langs = os.environ["TO_LANG"].split(",")
    for target_lang in to_langs:
        topic_name = os.environ["TRANSLATE_TOPIC"]
        if src_lang == target_lang or src_lang == "und":
            topic_name = os.environ["RESULT_TOPIC"]
        message = {
            "text": text,
            "filename": filename,
            "lang": target_lang,
            "src_lang": src_lang,
        }
        message_data = json.dumps(message).encode("utf-8")
        topic_path = publisher.topic_path(project_id, topic_name)
        future = publisher.publish(topic_path, data=message_data)
        futures.append(future)
    for future in futures:
        future.result()



def detect_speech(bucket, filename):
    print("Looking for speech in audio {}".format(filename))

    # Detect speech in the audio file 
    audio = speech.RecognitionAudio(uri=f"gs://{bucket}/{filename}")
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
        sample_rate_hertz=48000,
        enable_automatic_punctuation=True,
        language_code="en-US",
    )
    response = speech_client.recognize(config=config, audio=audio)
    text = ""
    # Extract the text from the response json
    for result in response.results:
        text += result.alternatives[0].transcript

    print("Extracted text {} from audio ({} chars).".format(text, len(text)))

    # Save the text using the same filename as the audio file to the bucket using the result topic
    topic_name = "results_topic-sub"
    massage = {"text": text, "filename": filename,"type":"audio"}
    message_data = json.dumps(massage).encode("utf-8")
    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, data=message_data)
    future.result()


def validate_message(message, param):
    var = message.get(param)
    if not var:
        raise ValueError(
            "{} is not provided. Make sure you have \
                          property {} in the request".format(
                param, param
            )
        )
    return var


# Triggered from a change to a Cloud Storage bucket - when the upload file is a image .
def process_image(file, context):

    bucket = validate_message(file, "bucket")
    name = validate_message(file, "name")

    detect_text(bucket, name)

    print("File {} processed.".format(file["name"]))


# Triggered from a change to a Cloud Storage bucket - when the upload file is a audio .
def process_audio(file, context):
    bucket = validate_message(file, "bucket")
    name = validate_message(file, "name")

    detect_speech(bucket, name)

    print("File {} processed.".format(file["name"]))



# the main function to trigger the cloud function
def trigger_from_cloud_storge(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    file_type = file["name"].split(".")[-1]
    print(f"file type: {file_type}")

    if file_type in ["jpg", "png", "jpeg"]:
        print("Image file detected.")
        # process the image
        process_image(file, context)

    # ditect if the file is a audio file
    elif file_type in ["mp3", "wav"]:
        print("Audio file detected.")
        # process the audio file
        process_audio(file, context)
    elif file_type in ["mp4"]:
        print("Video file detected.")
        # process the video file
        process_video(file, context)
    
    else:
        print("Not a valid file type.")


def translate_text(event, context):
    if event.get("data"):
        message_data = base64.b64decode(event["data"]).decode("utf-8")
        message = json.loads(message_data)
    else:
        raise ValueError("Data sector is missing in the Pub/Sub message.")

    text = validate_message(message, "text")
    filename = validate_message(message, "filename")
    target_lang = validate_message(message, "lang")
    src_lang = validate_message(message, "src_lang")

    print("Translating text into {}.".format(target_lang))
    translated_text = translate_client.translate(
        text, target_language=target_lang, source_language=src_lang
    )
    topic_name = os.environ["RESULT_TOPIC"]
    message = {
        "text": translated_text["translatedText"],
        "filename": filename,
        "lang": target_lang,
    }
    message_data = json.dumps(message).encode("utf-8")
    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, data=message_data)
    future.result()



def save_result(event, context):
    if event.get("data"):
        message_data = base64.b64decode(event["data"]).decode("utf-8")
        message = json.loads(message_data)
    else:
        raise ValueError("Data sector is missing in the Pub/Sub message.")

    text = validate_message(message, "text")
    filename = validate_message(message, "filename")
    

    # Check if the message is a result from the translation or from the speech to text API
    filename_parts = filename.split(".")[0]
    if not (message.get("type")) == "audio":
        lang = validate_message(message, "lang")
        result_filename = "{}_{}.txt".format(filename_parts, lang)
    else:
        result_filename = "{}.txt".format(filename_parts)

    print("Received request to save file {}.".format(filename))
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(result_filename)

    print("Saving result to {} in bucket {}.".format(result_filename, bucket_name))

    blob.upload_from_string(text)

    print("File saved.")
