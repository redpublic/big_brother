import asyncio
import inspect
import logging


logger = logging.getLogger(__name__)


class ScheduledTask:
    """
    Class to run some logic periodically. Task object should implement method run.

    Methods
    -------

    async start()
        Schedule provided task

    async stop()
        Stop scheduled task
    """
    _is_running = False

    def __init__(self, task, period):
        self.task = task
        self.period = period

    async def start(self):
        logger.info(f'Scheduled task with callback {self.task} has started')
        self._is_running = True

        while self._is_running:
            await self.task.run()
            await asyncio.sleep(self.period)

    def stop(self):
        logger.info(f'Scheduled task with callback {self.task} has stoped')
        self._is_running = False
