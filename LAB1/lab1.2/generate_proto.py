"""Genereaza modulele Python gRPC din contractul protobuf."""

from pathlib import Path

import grpc_tools
from grpc_tools import protoc


def main() -> None:
    directory = Path(__file__).resolve().parent
    include = Path(grpc_tools.__file__).resolve().parent / "_proto"
    result = protoc.main([
        "grpc_tools.protoc",
        f"-I{directory}",
        f"-I{include}",
        f"--python_out={directory}",
        f"--grpc_python_out={directory}",
        str(directory / "message_broker.proto"),
    ])
    if result != 0:
        raise SystemExit(result)
    print("Modulele protobuf au fost generate.")


if __name__ == "__main__":
    main()
