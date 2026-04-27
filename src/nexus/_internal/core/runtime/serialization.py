import io
import pickle
from typing import Any, BinaryIO

from nexus.utils.exceptions import InternalFrameworkException


class _UnsafeDeepDiffUnpickler(pickle.Unpickler):
    # Needed because deepdiff.pickle_dump writes this persistent id.
    def persistent_load(self, pid: Any) -> Any:
        if pid == "<<NoneType>>":
            return type(None)
        raise pickle.UnpicklingError(f"Unsupported persistent id: {pid!r}")


def unsafe_pickle_load(
    content: bytes | str | None = None,
    file_obj: BinaryIO | None = None,
    safe_to_import: object | None = None,
) -> Any:
    # safe_to_import intentionally ignored (unsafe mode)
    _ = safe_to_import
    if content is None and file_obj is None:
        raise ValueError("Pass content or file_obj")
    if isinstance(content, str):
        content = content.encode("utf-8")
    data_stream: BinaryIO
    if content is not None:
        data_stream = io.BytesIO(content)
    else:
        if file_obj is None:
            raise InternalFrameworkException("nothing to deserialize from")
        data_stream = file_obj
    return _UnsafeDeepDiffUnpickler(data_stream).load()
