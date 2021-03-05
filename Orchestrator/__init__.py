# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json

import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):

    logging.info("Orchestration started")

    ## Get data from the queue
    logging.info("About to create dict from context._input")
    logging.info(f"type(context._input): {type(context._input)}")
    logging.info(f"context._input: {context._input}")
    ## For some reason, there is a " at the start and end in the string
    ##    [1:-1] removes this problem
    fileURL,container = context._input[1:-1].split("__________")
    logging.info(f"fileURL: {fileURL}")
    logging.info(f"container: {container}")
    ## Work out file type
    if fileURL.lower().endswith(".mp4"):
        fileType = "MP4"
    elif fileURL.lower().endswith(".mp3") | fileURL.lower().endswith(".wav"):
        fileType = "MP3"
    else:
        fileType = "neither"
    logging.info(f"fileType: {fileType}")

    result = yield context.call_activity(
        'CreateChunks',
        f"{fileType}__________{fileURL}__________{container}"
    )

    return result

main = df.Orchestrator.create(orchestrator_function)