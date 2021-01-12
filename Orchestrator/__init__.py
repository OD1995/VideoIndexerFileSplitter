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
    qDict = json.loads(context._input)
    fileURL = qDict['fileURL']
    container = qDict['container']

    ## Work out file type
    if fileURL.lower().endswith(".mp4"):
        fileType = "MP4"
    elif fileURL.lower().endswith(".mp3") | fileURL.lower().endswith(".wav"):
        fileType = "MP3"
    else:
        fileType = "neither"

    result = yield context.call_activity(
        'CreateChunks',
        json.dumps([
            fileType,
            fileURL,
            container
        ])
    )

    return result

main = df.Orchestrator.create(orchestrator_function)