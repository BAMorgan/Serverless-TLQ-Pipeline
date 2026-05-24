import sys
import types
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_SRC = REPO_ROOT / "python" / "src"
if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))


class _FakeS3Client:
    def download_file(self, *args, **kwargs):
        raise RuntimeError("S3 is not available in unit tests")

    def upload_file(self, *args, **kwargs):
        raise RuntimeError("S3 is not available in unit tests")


fake_boto3 = types.SimpleNamespace(client=lambda *args, **kwargs: _FakeS3Client())
sys.modules.setdefault("boto3", fake_boto3)
