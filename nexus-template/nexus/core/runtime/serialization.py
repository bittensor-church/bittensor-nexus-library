import io
import pickle
from typing import Any

class _UnsafeDeepDiffUnpickler(pickle.Unpickler):
    # Needed because deepdiff.pickle_dump writes this persistent id.
    def persistent_load(self, pid: Any) -> Any:
        if pid == "<<NoneType>>":
            return type(None)
        raise pickle.UnpicklingError(f"Unsupported persistent id: {pid!r}")

def unsafe_pickle_load(content: bytes | str | None = None, file_obj=None, safe_to_import=None):
    # safe_to_import intentionally ignored (unsafe mode)
    if content is None and file_obj is None:
        raise ValueError("Pass content or file_obj")
    if isinstance(content, str):
        content = content.encode("utf-8")
    if content is not None:
        file_obj = io.BytesIO(content)
    return _UnsafeDeepDiffUnpickler(file_obj).load()