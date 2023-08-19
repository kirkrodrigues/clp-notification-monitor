import argparse
import logging
import logging.handlers
import pathlib
import sys
from typing import List, Optional

from clp_notification_monitor.seaweedfs_monitor.seaweedfs_grpc_client import SeaweedFSClient

"""
Global logger
"""
logger: logging.Logger


def logger_init(log_file_path_str: Optional[str], log_level: int) -> None:
    """
    Initializes the global logger with the given log level.

    :param log_file_path_str: Path to store the log files. If None is given,
        logs will be written into stdout.
    :param log_level: Target log level.
    """
    global logger
    logger_handler: logging.Handler
    if log_file_path_str is not None:
        log_file_path: pathlib.Path = pathlib.Path(log_file_path_str).resolve()
        if log_file_path.is_dir():
            log_file_path /= "clp_notification_monitor.log"
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        logger_handler = logging.handlers.RotatingFileHandler(
            filename=log_file_path, maxBytes=1024 * 1024 * 8, backupCount=7
        )
    else:
        logger_handler = logging.StreamHandler(sys.stdout)

    logger = logging.getLogger("clp_notification_monitor")
    logger.setLevel(log_level)
    logging_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    logger_handler.setFormatter(logging_formatter)
    logger_handler.setLevel(log_level)
    logger.addHandler(logger_handler)


def main(argv: List[str]) -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Execution Arguments")
    parser.add_argument(
        "--seaweed-filer-endpoint",
        required=True,
        help="The endpoint of the SeaweedFS Filer server.",
    )
    parser.parse_args(argv[1:])
    args: argparse.Namespace = parser.parse_args(argv[1:])

    logger_init("./logs/", logging.INFO)
    seaweedfs_client: SeaweedFSClient
    endpoint: str = args.seaweed_filer_endpoint
    try:
        seaweedfs_client = SeaweedFSClient("clp—testing", endpoint, logger)
    except Exception as e:
        logger.error(f"Failed to initialize seaweedfs client: {e}")
        return -1

    try:
        for notification in seaweedfs_client.s3_file_ingestion_listener():
            print(
                f"Ingestion on bucket <{notification.s3_bucket}>: <{notification.s3_path}> with"
                f" size: {notification.file_size}. Fids:"
            )
            for fid in notification.fid_list:
                print(f"{fid.vid},{fid.file_key}|{fid.file_cookie}")
        seaweedfs_client.close()
        return 0
    except Exception as e:
        logger.error(f"Exiting on error. Error message: {e}")
        seaweedfs_client.close()
        return -1
