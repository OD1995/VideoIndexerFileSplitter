import logging

import azure.functions as func
import azure.durable_functions as df


async def main(msg: func.QueueMessage, starter: str) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    client = df.DurableOrchestrationClient(starter)

    instance_id = await client.start_new(
        orchestration_function_name="Orchestrator",
        instance_id=None,
        client_input=msg.get_body().decode('utf-8')
    )
