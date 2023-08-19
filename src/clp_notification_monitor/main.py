import argparse
import logging
import logging.handlers
import pathlib
import sys
import time
from datetime import datetime
from threading import Thread
from typing import List, Optional

import pymongo

from clp_notification_monitor.compression_buffer.compression_buffer import CompressionBuffer
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


def submit_compression_jobs(compression_buffer: CompressionBuffer, polling_period: int) -> None:
    """
    :param compression_buffer: Compression buffer.
    :param polling_period: The polling period in seconds.
    """
    while True:
        compression_buffer.wait_for_compression_jobs()
        while False is compression_buffer.try_compress():
            time.sleep(polling_period)


def main(argv: List[str]) -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="CLP SeaweedFS Notification Monitor"
    )
    parser.add_argument(
        "--seaweed-filer-endpoint",
        required=True,
        help="The endpoint of the SeaweedFS Filer server.",
    )
    parser.add_argument(
        "--seaweed-s3-endpoint",
        required=True,
        help="The endpoint of the SeaweedFS S3 server.",
    )
    parser.add_argument("--db-uri", required=True, help="Regional compression DB uri")
    parser.parse_args(argv[1:])
    args: argparse.Namespace = parser.parse_args(argv[1:])

    logger_init("./logs/", logging.INFO)
    endpoint: str = args.seaweed_filer_endpoint
    s3_endpoint: str = args.seaweed_s3_endpoint
    db_uri: str = args.db_uri

    seaweedfs_client: SeaweedFSClient
    try:
        seaweedfs_client = SeaweedFSClient("clp—testing", endpoint, logger)
    except Exception as e:
        logger.error(f"Failed to initiate seaweedfs client: {e}")
        return -1

    mongodb: pymongo.mongo_client.MongoClient
    archive_db: pymongo.database.Database
    jobs_collection: pymongo.collection.Collection
    try:
        mongodb = pymongo.mongo_client.MongoClient(db_uri)
        archive_db = mongodb.get_default_database()
        jobs_collection = archive_db["cjobs"]
    except Exception as e:
        logger.error(f"Failed to initiate MongoDB: {e}")

    compression_buffer: CompressionBuffer
    try:
        compression_buffer = CompressionBuffer(
            logger=logger,
            jobs_collection=jobs_collection,
            s3_endpoint=s3_endpoint,
            max_buffer_size=2 * 1024 * 1024,  # 2MB
            min_refresh_period=5 * 1000,  # 5 seconds
        )
    except Exception as e:
        logger.error(f"Failed to initiate Compression Buffer: {e}")

    job_submission_thread: Thread
    try:
        job_submission_thread = Thread(target=submit_compression_jobs, args=(compression_buffer, 1))
        job_submission_thread.start()
        job_submission_thread.join()
    except Exception as e:
        logger.error(f"Failed to initiate Job Submission Thread: {e}")

    try:
        for notification in seaweedfs_client.s3_file_ingestion_listener(store_fid=False):
            compression_buffer.append(
                notification.s3_full_path, notification.file_size, datetime.now()
            )
        seaweedfs_client.close()
        return 0
    except Exception as e:
        logger.error(f"Exiting on error. Error message: {e}")
        seaweedfs_client.close()
        return -1
