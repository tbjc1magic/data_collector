# from data_collectors
import asyncio
import json
from datetime import datetime
from pathlib import Path

from data_collector_protos import data_collector_service_pb2
from data_collector_protos import data_collector_service_pb2_grpc
from grpc import aio
from grpc_reflection.v1alpha import reflection


class DataCollectorServicer(data_collector_service_pb2_grpc.DataCollectorServicer):
    def __init__(self, base_path):
        self.file_handlers = {}
        self._base_path = base_path

    async def SaveData(
            self, request: data_collector_service_pb2.SaveDataRequest, context
    ):
        parent_dir = datetime.utcnow().strftime("%Y_%m_%d")
        directory = Path(self._base_path, parent_dir)
        directory.mkdir(parents=True, exist_ok=True)
        path = Path(directory, request.data_type).with_suffix(".log")

        if request.data_type in self.file_handlers and \
                self.file_handlers[request.data_type]["parent_dir"] != parent_dir:
            self.file_handlers[request.data_type]["handler"].close()
            self.file_handlers.pop(request.data_type)

        if request.data_type not in self.file_handlers:
            self.file_handlers[request.data_type] = {"parent_dir": parent_dir,
                                                     "handler": open(path, "a")}

        self.file_handlers[request.data_type]["handler"].write(
            json.dumps({"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "log": request.log_message}) + "\n")
        self.file_handlers[request.data_type]["handler"].flush()
        return data_collector_service_pb2.SaveDataResponse(
            state=data_collector_service_pb2.SaveDataResponse.State.SUCCEEDED)


async def main():
    server = aio.server()
    data_collector_service_pb2_grpc.add_DataCollectorServicer_to_server(
        DataCollectorServicer("/home/tbjc1magic/log"), server
    )
    service_names = (
        data_collector_service_pb2.DESCRIPTOR.services_by_name["DataCollector"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(service_names, server)
    server.add_insecure_port("[::]:8889")
    await server.start()
    await server.wait_for_termination()

    print(123)


if __name__ == "__main__":
    asyncio.run(main())
