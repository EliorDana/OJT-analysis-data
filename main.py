import base64
import json
import os

from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import speech_v1
from google.cloud import translate_v2 as translate
from google.cloud import vision

vision_client = vision.ImageAnnotatorClient()
translate_client = translate.Client()
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()
speech_client = speech_v1.SpeechClient()

project_id = os.environ["GCP_PROJECT"]

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
    audio = speech_v1.RecognitionAudio(uri=f"gs://{bucket}/{filename}")
    config = speech_v1.RecognitionConfig(
        encoding=speech_v1.RecognitionConfig.AudioEncoding.LINEAR16,
        sample_rate_hertz=16000,
        enable_automatic_punctuation=True,
        language_code="en-US",
    )
    response = speech_client.recognize(config=config, audio=audio)
    text = ""
    # Extract the text from the response json
    for result in response.results:
        text += result.alternatives[0].transcript

    # Save the text to a file and save the file to the bucket
    bucket_name = os.environ["RESULT_BUCKET"]
    result_filename = filename.split(".")[0] + ".txt"
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(text)
    print("Saved text to file {} in bucket {}.".format(result_filename, bucket_name))


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
    """Cloud Function triggered by Cloud Storage when a file is changed.
    Args:
        file (dict): Metadata of the changed file, provided by the triggering
                                 Cloud Storage event.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to stdout and Stackdriver Logging
    """
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
    lang = validate_message(message, "lang")

    print("Received request to save file {}.".format(filename))

    bucket_name = os.environ["RESULT_BUCKET"]
    result_filename = "{}_{}.txt".format(filename, lang)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(result_filename)

    print("Saving result to {} in bucket {}.".format(result_filename, bucket_name))

    blob.upload_from_string(text)

    print("File saved.")
