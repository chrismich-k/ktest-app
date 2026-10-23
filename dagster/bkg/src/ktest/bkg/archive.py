"""Remote-zip reads against the raw BKG VG250 archive in the read-only
raw-data S3 bucket (rustfs, "raw-r" credentials). Fetches only the
end-of-central-directory + central directory plus the one .gpkg member
requested - zipfile.ZipFile never sees the whole archive.

RangedS3File/open_remote_zip are the same class/function as
ktest.mastr.archive's - duplicated, not shared, matching this repo's
existing convention of zero cross-code-location Python dependencies
between smard/tradinghub_eu/mastr.
"""

import io
import re
import zipfile

_RANGE_READ_BUFFER = 256 * 1024


class RangedS3File(io.RawIOBase):
    """Seekable, read-only file-like object backed by ranged S3 GetObject
    requests against one (bucket, key). Implements exactly what
    zipfile.ZipFile needs. SEEK_END requires the object's total size,
    fetched once via head_object in __init__ - zipfile's own _EndRecData
    does `fp.seek(0, 2)` before anything else, so this can't be deferred.
    """

    def __init__(self, s3_client, bucket: str, key: str):
        super().__init__()
        self._client = s3_client
        self._bucket = bucket
        self._key = key
        self._size = s3_client.head_object(Bucket=bucket, Key=key)[
            "ContentLength"
        ]
        self._pos = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._size + offset
        else:
            raise ValueError(f"invalid whence: {whence}")
        return self._pos

    def tell(self) -> int:
        return self._pos

    def readinto(self, b) -> int:
        n = len(b)
        if n == 0 or self._pos >= self._size:
            return 0
        end = min(self._pos + n, self._size) - 1
        body = self._client.get_object(
            Bucket=self._bucket,
            Key=self._key,
            Range=f"bytes={self._pos}-{end}",
        )["Body"].read()
        b[: len(body)] = body
        self._pos += len(body)
        return len(body)


def open_remote_zip(s3_client, bucket: str, key: str) -> zipfile.ZipFile:
    raw = RangedS3File(s3_client, bucket, key)
    # Buffered to coalesce zipfile's many small sequential reads into fewer
    # Range GETs.
    return zipfile.ZipFile(
        io.BufferedReader(raw, buffer_size=_RANGE_READ_BUFFER)
    )


_GPKG_RE = re.compile(r"^[^/]+/DE_VG250\.gpkg$")


def find_gpkg_member(zf: zipfile.ZipFile) -> str:
    """Find the one DE_VG250.gpkg member - its containing directory name
    (e.g. "vg250_ebenen_0101/") is plausibly date-dependent and not
    hardcoded here, only the filename itself is pinned.
    """
    matches = [name for name in zf.namelist() if _GPKG_RE.match(name)]
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Expected exactly one DE_VG250.gpkg member, found "
            f"{len(matches)}: {matches}"
        )
    return matches[0]


def discover_archive_key(
    s3_client, bucket: str, prefix: str, stichtag: str
) -> str:
    """Find the one .zip under `<prefix>/stichtag=<stichtag>/` - the
    filename isn't guaranteed stable across years, so this is always a
    listing, never a constructed path."""
    list_prefix = f"{prefix}/stichtag={stichtag}/"
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=list_prefix)
    zip_keys = [
        o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".zip")
    ]
    if len(zip_keys) != 1:
        raise FileNotFoundError(
            f"Expected exactly one .zip under s3://{bucket}/{list_prefix}, "
            f"found {len(zip_keys)}: {zip_keys}"
        )
    return zip_keys[0]


_SUCCESS_RE_TEMPLATE = (
    r"^{prefix}/stichtag=([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})/_SUCCESS$"
)


def discover_available_stichtage(
    s3_client, bucket: str, prefix: str
) -> list[str]:
    """List every `stichtag=<date>` folder under `<prefix>/` that is fully
    uploaded - looks for a `_SUCCESS` marker object (written by the external
    uploader strictly after its archive zip finishes uploading), same
    contract as ktest.mastr.archive's.
    """
    pattern = re.compile(_SUCCESS_RE_TEMPLATE.format(prefix=re.escape(prefix)))
    paginator = s3_client.get_paginator("list_objects_v2")
    stichtage = []
    for page in paginator.paginate(
        Bucket=bucket, Prefix=f"{prefix}/stichtag="
    ):
        for obj in page.get("Contents", []):
            if m := pattern.match(obj["Key"]):
                stichtage.append(m.group(1))
    return stichtage
