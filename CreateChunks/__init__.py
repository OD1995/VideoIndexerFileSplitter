# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from azure.storage.blob import BlockBlobService
from moviepy.editor import VideoFileClip, AudioFileClip
import os
from datetime import datetime, timedelta
from math import ceil
from MyFunctions import get_SAS_URL
import tempfile
from urllib.parse import unquote
import json

def main(inputs: str) -> str:
    ## Arguments
    fileType,fileURL,container = inputs.split("__________")
    logging.info(f"fileType: {fileType}")
    logging.info(f"fileURL: {fileURL}")
    logging.info(f"container: {container}")
    logging.info("changes 10")
    ## Create bbs
    bbs = BlockBlobService(connection_string=os.getenv("fsevideosConnectionString"))
    ## Create SAS URL
    sasURL = get_SAS_URL(
        fileURL=fileURL,
        block_blob_service=bbs,
        container=container
    )
    logging.info(f"sasURL: {sasURL}")
    ## Set the size of the clips you want (in seconds)
    chunk_length_secs = 3600
    ## Set file name to be used
    fileName = unquote(fileURL.split("/")[-1])
    ## Create clip
    if fileType == "MP4":
        clip = VideoFileClip(sasURL)
    elif fileType == "MP3":
        clip = AudioFileClip(sasURL)
    else:
        raise ValueError("wrong file type")
    ## Get file extension
    fileExtension = fileURL.split(".")[-1]
    ## Get number of chunks (files to be created)
    chunk_count = ceil(clip.duration / chunk_length_secs)
    ## Loop through the chunks
    for a in range(chunk_count):
        ## Create a string to add to the front of the file name for this chunk
        filePrefix = f"{a+1}of{chunk_count}"
        ## If it's the last subclip, do last stopping point until end
        if a == chunk_count - 1:
            subclip = clip.subclip(
                            t_start=a*chunk_length_secs,
                            t_end=clip.duration
                            )
        ## Otherwise just do the next `chunk_length_secs`
        else:
            subclip = clip.subclip(
                            t_start=a*chunk_length_secs,
                            t_end=(a+1)*chunk_length_secs
                            )
        logging.info(f"File: {filePrefix}")
        logging.info(f"Duration: {subclip.duration} seconds")
        A = datetime.now()
        ## Download locally to temporary then upload to Azure
        # with tempfile.gettempdir() as dirpath:
        fn = f"{filePrefix}_{fileName}"
        tempFilePath = "/tmp/" + fn
        # tempFilePath = tempfile.NamedTemporaryFile(suffix=f".{fileExtension}").name
        logging.info(f"tempFilePath: {tempFilePath}")
        if fileType == "MP4":
            subclip.write_videofile(filename=tempFilePath)
        elif fileType == "MP3":
            subclip.write_audiofile(filename=tempFilePath)
        logging.info("Clip has been saved locally")
            
        bbs.create_blob_from_path(
            container_name=container,
            blob_name=f"{filePrefix}_{fileName}",
            file_path=tempFilePath
        )
        B = datetime.now()
        logging.info(f"Uploaded, time taken: {B-A}")

    return f"{chunk_count} files uploaded to the `{container}` container"
