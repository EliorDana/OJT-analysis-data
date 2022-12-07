

def process_image(file, context):
    pass
def process_audio(file, context):
    pass
def process_video(event, context):
    pass

# the main function to trigger the cloud function
def trigger_from_cloud_storge(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    # print(f"my file{file}")
    # print(f"Processing file: {file['name']}.")
    # ditect the file type
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
    

    # ditect if the file is video
    elif file_type in ["mp4", "avi"]:
        print("Video file detected.")
        # process the video file
        process_video(file, context)

    else:
        print("File type not supported.")
        raise ValueError("File type not supported.")
