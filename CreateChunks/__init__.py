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
import ffmpy
import subprocess

def main(inputs: str) -> str:
    ## Arguments
    fileType,fileURL,container = inputs.split("__________")
    logging.info(f"fileType: {fileType}")
    logging.info(f"fileURL: {fileURL}")
    logging.info(f"container: {container}")
    logging.info("changes 11")
    ## Set file name to be used and get container it came from
    fileName = unquote(fileURL.split("/")[-1]) # (contains file extension)
    container = fileURL.split("/")[-2]
    ## Create bbs
    bbs = BlockBlobService(connection_string=os.getenv("fsevideosConnectionString"))
    ## Create SAS URL
    sasURL = get_SAS_URL(
        fileURL=fileURL,
        block_blob_service=bbs,
        container=container
    )
    logging.info(f"sasURL: {sasURL}")
    ## Download blob to temporary location
    tempClipFilePath = "/tmp/" + fileName
    bbs.get_blob_to_path(
        container_name=container,
        blob_name=fileName,
        file_path=tempClipFilePath
    )
    logging.info(f'file saved to "{tempClipFilePath}"')
    ## Set the size of the clips you want (in seconds)
    chunk_length_secs = 3600
    ## Create clip
    if fileType == "MP4":
        clip = VideoFileClip(tempClipFilePath)
    elif fileType == "MP3":
        clip = AudioFileClip(tempClipFilePath)
    else:
        raise ValueError("wrong file type")
    ## Get number of chunks (files to be created)
    logging.info(f"clip.duration: {clip.duration}")
    chunk_count = ceil(clip.duration / chunk_length_secs)
    ## Loop through the chunks
    for a in range(chunk_count):
        ## Get prefix to use
        subclipPrefix = f"{a+1}of{chunk_count}"
        subclipFileName = f"{subclipPrefix}_{fileName}"
        logging.info(f"clip: {subclipPrefix}")
        A = datetime.now()
        ## If we're on the last subclip
        if a + 1 == chunk_count:
            subclipDurationSeconds = clip.duration - (a * chunk_length_secs)
        else:
            subclipDurationSeconds = chunk_length_secs
            
        startSeconds = chunk_length_secs * a
        
        fileOutPath = "/tmp/" + subclipFileName
        
        # ff = ffmpy.FFmpeg(
        #     executable="./ffmpeg",
        #     inputs={
        #             tempClipFilePath : f"-ss {startSeconds}"
        #             },
        #     outputs={
        #             fileOutPath : f"-t {subclipDurationSeconds} -c copy"
        #             }
        # )
        # logging.info(f"ff.cmd: {ff.cmd}")
        # ff.run()

        # ffmpegCommand = f'./ffmpeg -ss {startSeconds} -i "{tempClipFilePath}" -t {subclipDurationSeconds} -c copy -bsf:a aac_adtstoasc "{fileOutPath}"'
        ffmpegCommand = f'./ffmpeg -ss {startSeconds} -i "{tempClipFilePath}" -t {subclipDurationSeconds} -bsf:a aac_adtstoasc -acodec copy -vcodec copy "{fileOutPath}"'
        logging.info(f"ffmpegCommand: {ffmpegCommand}")
        # p = subprocess.Popen(ffmpegCommand)
        # p.wait()
        result = os.popen(ffmpegCommand).read()
        logging.info("command run")
        logging.info(f"result: {result}")
            
        bbs.create_blob_from_path(
            container_name=container,
            blob_name=subclipFileName,
            file_path=fileOutPath
        )
        B = datetime.now()
        logging.info(f"{subclipPrefix} uploaded, time taken: {B-A}")

    return f"{chunk_count} files uploaded to the `{container}` container"
