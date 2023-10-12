from datetime import datetime, timedelta
from logging import Logger
from pathlib import Path, PurePath
from threading import Condition, Lock
from typing import Dict, List, Optional


class SingleBuffer:
    def __init__(self) -> None:
        self.path_list: List[PurePath] = []
        self.total_buffer_size: int = 0
        self.first_path_timestamp: Optional[datetime] = None

    def clear(self) -> None:
        self.path_list = []
        self.total_buffer_size = 0
        self.first_path_timestamp = None


class CompressionBuffer:
    def __init__(
        self,
        logger: Logger,
        max_buffer_size: int,
        min_refresh_period: int,
        group_by_paths_under_prefix: Path,
    ) -> None:
        self._logger: Logger = logger

        # self.__path_list: List[PurePath] = []
        # self.__total_buffer_size: int = 0
        # self.__first_path_timestamp: Optional[datetime] = None

        self.__max_buffer_size: int = max_buffer_size
        self.__min_refresh_period: timedelta = timedelta(milliseconds=min_refresh_period)

        self.__named_buffers: Dict[str, SingleBuffer] = {}
        self.__ready_buffers: List[str] = []
        self.__group_by_paths_under_prefix: Path = group_by_paths_under_prefix

        # TODO: change the granularity of lock to each buffer
        self.__lock: Lock = Lock()
        self.__populate_buffer_cv: Condition = Condition(self.__lock)

    def append(
        self,
        s3_path: PurePath,
        object_size: int,
        process_timestamp: datetime,
    ) -> None:
        with self.__populate_buffer_cv:
            relative_path: PurePath
            index_name: str
            try:
                relative_path = s3_path.relative_to(self.__group_by_paths_under_prefix)
                index_name = relative_path.parts[0]
            except (KeyError, ValueError):
                # This file does not belong to any named buffer
                index_name = ""

            # Get the correct buffer
            target_buffer: SingleBuffer
            try:
                target_buffer = self.__named_buffers[index_name]
            except KeyError:
                target_buffer = SingleBuffer()
                self.__named_buffers[index_name] = target_buffer

            # Add path to buffer
            target_buffer.total_buffer_size += object_size
            if target_buffer.first_path_timestamp is None:
                target_buffer.first_path_timestamp = process_timestamp
            target_buffer.path_list.append(s3_path)

            # Notify the thread waiting for a compression job
            self.__populate_buffer_cv.notify_all()

    def wait_for_compression_jobs(self) -> None:
        """
        Block the current process until the buffer is populated.
        """
        with self.__populate_buffer_cv:
            while True:
                all_buffers_empty: bool = True
                for buf in self.__named_buffers.values():
                    if buf.first_path_timestamp is not None:
                        all_buffers_empty = False
                        break
                if not all_buffers_empty:
                    break
                self.__populate_buffer_cv.wait()

    def _get_buffers_ready_for_compression(self) -> List[str]:
        ready_buffers: List[str] = []
        for buf_name, buf in self.__named_buffers.items():
            if buf.first_path_timestamp is None:
                continue

            # Job triggered by reaching maximum archive size
            if buf.total_buffer_size > self.__max_buffer_size:
                self._logger.info(
                    f"Buffer {buf_name} ready for compression (max buffer size exceeded). "
                    f"Total buffer size: {buf.total_buffer_size}. "
                    f"Max buffer size: {self.__max_buffer_size}."
                )
                ready_buffers.append(buf_name)
                continue

            # Job triggered by the minimum buffer refresh frequency
            total_elapsed_time = datetime.now() - buf.first_path_timestamp
            if total_elapsed_time > self.__min_refresh_period:
                self._logger.info(
                    f"Buffer {buf_name} ready for compression (min refresh period exceeded). "
                    f"Total elapsed time: {total_elapsed_time.total_seconds()} seconds. "
                    f"First chunk timestamp: {buf.first_path_timestamp}."
                )
                ready_buffers.append(buf_name)

        return ready_buffers

    def get_paths_to_compress(self) -> List[PurePath]:
        """
        :return: The path list of the current buffer if the compression job can
        be fired. Otherwise, returns an empty list.
        """
        if 0 == len(self.__ready_buffers):
            self.__lock.acquire()
            self.__ready_buffers = self._get_buffers_ready_for_compression()
            if 0 == len(self.__ready_buffers):
                self.__lock.release()
                return []

        # Do not release the lock until all target buffers have been processed.
        top_buffer_name: str = self.__ready_buffers.pop(0)
        target_buffer: SingleBuffer = self.__named_buffers[top_buffer_name]

        # Get a deepcopy of the current path list
        path_list_to_return = list(target_buffer.path_list)
        target_buffer.clear()

        # If the popped buffer is the last one in the list, release the lock
        if 0 == len(self.__ready_buffers):
            self.__lock.release()

        return path_list_to_return
