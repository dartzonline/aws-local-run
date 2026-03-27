"""File watcher for Lambda hot reload."""
import hashlib, io, logging, os, threading, time, zipfile

logger = logging.getLogger("localrun.watcher")


class LambdaWatcher:
    def __init__(self, watch_dir, lambda_svc):
        self.watch_dir = os.path.abspath(watch_dir)
        self.lambda_svc = lambda_svc
        self._hashes = {}
        self._stop = threading.Event()

    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        logger.info("Watching %s for Lambda changes", self.watch_dir)
        while not self._stop.is_set():
            self._scan()
            time.sleep(1.0)

    def _scan(self):
        if not os.path.isdir(self.watch_dir):
            return
        for entry in os.scandir(self.watch_dir):
            if not entry.is_dir():
                continue
            fn_name = entry.name
            fn_hash = self._hash_dir(entry.path)
            if self._hashes.get(fn_name) != fn_hash:
                self._hashes[fn_name] = fn_hash
                if fn_name in self.lambda_svc.functions:
                    self._reload(fn_name, entry.path)

    def _hash_dir(self, dirpath):
        h = hashlib.md5()
        for root, dirs, files in os.walk(dirpath):
            dirs.sort()
            for fname in sorted(files):
                fpath = os.path.join(root, fname)
                h.update(fname.encode())
                try:
                    with open(fpath, "rb") as f:
                        h.update(f.read())
                except OSError:
                    pass
        return h.hexdigest()

    def _reload(self, fn_name, dirpath):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for root, dirs, files in os.walk(dirpath):
                for fname in files:
                    fpath = os.path.join(root, fname)
                    arcname = os.path.relpath(fpath, dirpath)
                    zf.write(fpath, arcname)
        fn = self.lambda_svc.functions.get(fn_name)
        if fn:
            fn.code_zip = buf.getvalue()
            logger.info("Hot reloaded Lambda: %s", fn_name)
            print(f"  Hot reloaded: {fn_name}")
