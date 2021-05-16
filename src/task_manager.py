import logging
import tarfile
from multiprocessing import Queue, Process, Lock
from pathlib import Path

logger = logging.getLogger(__name__)


class TaskConsumer(Process):
    def __init__(self, queue, idx, lock, **kwargs):
        super().__init__()
        self._queue = queue
        self._idx = idx
        self._lock = lock

    def run(self):
        """Build some CPU-intensive tasks."""
        while True:
            path = Path(self._queue.get(block=True))
            with self._lock:
                logger.info(f"Work '{self._idx}' is compressing {path}.")
            with tarfile.open(path.with_suffix(".tar.gz"), "w:gz") as tar:
                tar.add(path, arcname=path.name)
            path.unlink()


class TaskManager:
    def __init__(self, num_of_processes=1):
        self._num_of_processes = num_of_processes
        self._queue = Queue()
        self._lock = Lock()

    def add_task(self, path):
        self._queue.put(path)

    def run(self):
        ## Create a list to hold running Processor object instances...
        processes = []
        for i in range(self._num_of_processes):
            p = TaskConsumer(queue=self._queue, idx=i, lock=self._lock)
            p.start()
            processes.append(p)
        [proc.join() for proc in processes]
