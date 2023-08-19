from pathlib import PurePath
from typing import List


class SeaweedFID:
    """
    This class represents a SeaweedFS unique FID.

    It contains a volume id, a file key, and a file cookie.
    """

    def __init__(self, vid: int, file_key: int, file_cookie: int):
        self.vid: int = vid
        self.file_key: int = file_key
        self.file_cookie: int = file_cookie


class S3NotificationMessage:
    """
    This class represents a notification message returned by the SeaweedFS
    monitor, assuming the file is ingested using AWS S3 API.

    It contains all the necessary metadata that CLP database requires.
    """

    def __init__(self, s3_bucket: str, s3_path: str, file_size: int, fid_list: List[SeaweedFID]):
        """
        :param s3_bucket: S3 bucket name of the ingested file.
        :param s3_path: Full path in the S3 bucket.
        :param file_size: The size of the ingested file.
        :param fid: A list of SeaweedFS FID that matches all the chunks.
        """
        self.s3_bucket: str = s3_bucket
        self.s3_path: str = s3_path
        self.fid_list: List[SeaweedFID] = fid_list
        self.file_size: int = file_size
        self.s3_full_path: PurePath = PurePath(s3_bucket) / PurePath(s3_path)
