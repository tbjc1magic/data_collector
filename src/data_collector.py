# from data_collectors
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

from data_collector_protos import data_collector_service_pb2
from data_collector_protos import data_collector_service_pb2_grpc
from grpc import aio
from grpc_reflection.v1alpha import reflection

from task_manager import TaskManager

logger = logging.getLogger(__name__)


class DataCollectorServicer(data_collector_service_pb2_grpc.DataCollectorServicer):
    def __init__(self, base_path, task_manager: TaskManager):
        self.file_handlers = {}
        self._base_path = base_path
        self._task_manager = task_manager

    async def SaveData(
        self, request: data_collector_service_pb2.SaveDataRequest, context
    ):
        parent_dir = datetime.utcnow().strftime("%Y_%m_%d")
        directory = Path(self._base_path, parent_dir)
        directory.mkdir(parents=True, exist_ok=True)
        path = Path(directory, request.data_type).with_suffix(".log")

        if (
            request.data_type in self.file_handlers
            and self.file_handlers[request.data_type]["parent_dir"] != parent_dir
        ):
            self.file_handlers[request.data_type]["handler"].close()
            self._task_manager.add_task(self.file_handlers[request.data_type]["path"])
            self.file_handlers.pop(request.data_type)

        if request.data_type not in self.file_handlers:
            self.file_handlers[request.data_type] = {
                "parent_dir": parent_dir,
                "handler": open(path, "a"),
                "path": path,
            }

        log = json.dumps(
            {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "log": request.log_message,
            }
        )
        self.file_handlers[request.data_type]["handler"].write(log)
        self.file_handlers[request.data_type]["handler"].write("\n")
        self.file_handlers[request.data_type]["handler"].flush()
        return data_collector_service_pb2.SaveDataResponse(
            state=data_collector_service_pb2.SaveDataResponse.State.SUCCEEDED
        )


async def main():
    server = aio.server()
    task_manager = TaskManager()
    task_manager_thread = asyncio.to_thread(task_manager.run)
    data_collector_service_pb2_grpc.add_DataCollectorServicer_to_server(
        DataCollectorServicer("/home/tbjc1magic/log", task_manager), server
    )
    service_names = (
        data_collector_service_pb2.DESCRIPTOR.services_by_name[
            "DataCollector"
        ].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(service_names, server)
    server.add_insecure_port("[::]:9999")
    await server.start()
    await asyncio.gather(server.wait_for_termination(), task_manager_thread)


if __name__ == "__main__":
    logging.basicConfig(
        filename="data_collector.log", encoding="utf-8", level=logging.INFO
    )
    logging.getLogger().addHandler(logging.StreamHandler())
    asyncio.run(main())
