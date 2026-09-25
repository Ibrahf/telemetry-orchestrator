"""Worker process: polls the task queue and executes workflows and activities.

Run:  python -m telemetry.worker
"""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from . import config
from .activities import ALL_ACTIVITIES
from .workflows import TelemetryBatchWorkflow


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    client = await Client.connect(config.TEMPORAL_ADDRESS)
    # Activities are plain (synchronous) functions, so they run on a thread pool.
    with ThreadPoolExecutor(max_workers=10) as executor:
        worker = Worker(
            client,
            task_queue=config.TASK_QUEUE,
            workflows=[TelemetryBatchWorkflow],
            activities=ALL_ACTIVITIES,
            activity_executor=executor,
        )
        logging.info("Worker listening on task queue '%s'", config.TASK_QUEUE)
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
