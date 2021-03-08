# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import ContentSettings
from moviepy.editor import VideoFileClip, AudioFileClip
import os
from datetime import datetime, timedelta
from math import ceil
from MyFunctions import get_SAS_URL
import tempfile
from urllib.parse import unquote
import json
# import ffmpy
import subprocess

def main(inputDict: dict) -> str:
    ## Arguments
    fileType = inputDict['fileType']
    fileURL = inputDict["fileURL"]
    container = inputDict["container"]
    selector = inputDict["selector"]
    logging.info(f"fileType: {fileType}")
    logging.info(f"fileURL: {fileURL}")
    logging.info(f"container: {container}")
    logging.info(f"selector: {selector}")
    ## `selector` takes the format "XofY" or "ALL"
    _all_ = selector == "ALL"
    X = None
    Y = None
    if not _all_:
        X = int(selector.split("of")[0])
        Y = int(selector.split("of")[1])
    logging.info("changes 12")
    ## Set file name to be used and get container it came from
    fileName = unquote(fileURL.split("/")[-1]) # (contains file extension)
    container = fileURL.split("/")[-2]
    ## Create bbs
    bbs = BlockBlobService(connection_string=os.getenv("fsevideosConnectionString"))
    logging.info("bbs created")
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
    ## If not _all_, make sure Y is the expected number
    if not _all_:
        if chunk_count != Y:
            vem = (
                f"Based on the selector passed ({selector}), "
                f"there should be {Y} chunks, but instead there "
                f"are {chunk_count} chunks."
            )
            raise ValueError(vem)
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
        startHMS = "{:0>8}".format(str(timedelta(seconds=startSeconds)))
        endHMS = "{:0>8}".format(str(timedelta(seconds=startSeconds+chunk_length_secs)))
        
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
        if (_all_) or (X == a + 1):
            logging.info(f'startSeconds: {startSeconds}')
            logging.info(f"tempClipFilePath: {tempClipFilePath}")
            logging.info(f"subclipDurationSeconds: {subclipDurationSeconds}")
            logging.info(f"fileOutPath: {fileOutPath}")
            logging.info(f"startHMS: {startHMS}")
            logging.info(f"endHMS: {endHMS}")


            # # ffmpegCommand = f'./ffmpeg -ss {startSeconds} -i "{tempClipFilePath}" -t {subclipDurationSeconds} -c copy -bsf:a aac_adtstoasc "{fileOutPath}"'
            # ffmpegCommand = f'./ffmpeg -ss {startSeconds} -i "{tempClipFilePath}" -t {subclipDurationSeconds} -bsf:a aac_adtstoasc -acodec copy -vcodec copy "{fileOutPath}"'
            ffmpegCommand = f'./ffmpeg -i "{tempClipFilePath}" -ss {startHMS} -to {endHMS} -c copy "{fileOutPath}"'
            logging.info(f"ffmpegCommand: {ffmpegCommand}")
            # p = subprocess.Popen(ffmpegCommand)
            # p.wait()
            result = os.popen(ffmpegCommand).read()
            logging.info("command run")
            logging.info(f"result: {result}")

            ## Create subclip using moviepy
            # t_start = startSeconds
            # t_end = startSeconds+subclipDurationSeconds
            # logging.info(f"t_start: {t_start}")
            # logging.info(f"t_end: {t_end}")
            # subclip = clip.subclip(
            #     t_start=t_start,
            #     t_end=t_end
            # )
            # logging.info("subclip created")
            # # temp_file_path = tempfile.gettempdir() + "/temp-audio.m4a"
            # temp_file_path = "/tmp/temp-audio.m4a"
            # ## Save to path
            # subclip.write_videofile(
            #     filename=fileOutPath,
            #     verbose=False,
            #     logger=None,
            #     temp_audiofile=temp_file_path,
            #     remove_temp=True,
            #     audio_codec="aac"
            # )
            # logging.info("subclip written to file")
            # subclip.close()
            # logging.info("subclip closed")
            contentType = "video/mp4" if fileType == "MP4" else "audio/mpeg3"
            bbs.create_blob_from_path(
                container_name=container,
                blob_name=subclipFileName,
                file_path=fileOutPath,
                content_settings=ContentSettings(
                    content_type=contentType
                )
            )
            ## Delete created file from temporary storage
            os.remove(fileOutPath)
            B = datetime.now()
            logging.info(f"{subclipPrefix} uploaded, time taken: {B-A}")
        
        else:
            logging.info("this ffmpeg run is not happening")

        
    ## Delete original file from temporary storage
    os.remove(tempClipFilePath)

    return f"{chunk_count} files uploaded to the `{container}` container"
