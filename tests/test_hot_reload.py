"""Lambda hot reload watcher tests."""
import os
import tempfile
import time
from unittest.mock import MagicMock


class TestLambdaWatcher:
    def test_hash_dir_consistent(self):
        from localrun.watcher import LambdaWatcher
        mock_svc = MagicMock()
        mock_svc.functions = {}
        with tempfile.TemporaryDirectory() as d:
            fn_dir = os.path.join(d, "my-function")
            os.makedirs(fn_dir)
            with open(os.path.join(fn_dir, "index.py"), "w") as f:
                f.write("def handler(event, ctx): return {}")
            watcher = LambdaWatcher(d, mock_svc)
            h1 = watcher._hash_dir(fn_dir)
            h2 = watcher._hash_dir(fn_dir)
            assert h1 == h2
            assert isinstance(h1, str)
            assert len(h1) > 0

    def test_watcher_detects_file_change_and_reloads(self):
        from localrun.watcher import LambdaWatcher
        from localrun.services.lambda_service import LambdaFunction

        mock_fn = MagicMock(spec=LambdaFunction)
        mock_fn.code_zip = b""
        mock_svc = MagicMock()
        mock_svc.functions = {"my-function": mock_fn}

        with tempfile.TemporaryDirectory() as d:
            fn_dir = os.path.join(d, "my-function")
            os.makedirs(fn_dir)
            with open(os.path.join(fn_dir, "index.py"), "w") as f:
                f.write("def handler(event, ctx): return {}")

            watcher = LambdaWatcher(d, mock_svc)
            # Initial scan to set baseline hashes
            watcher._scan()
            # Verify no reload happened (first scan just sets hash)
            initial_code = mock_fn.code_zip

            # Now modify the file
            time.sleep(0.05)
            with open(os.path.join(fn_dir, "index.py"), "w") as f:
                f.write("def handler(event, ctx): return {'modified': True}")

            # Scan again — should detect change and reload
            watcher._scan()

            # code_zip should have been updated
            assert mock_fn.code_zip != b""
            assert isinstance(mock_fn.code_zip, bytes)
            assert len(mock_fn.code_zip) > 0
