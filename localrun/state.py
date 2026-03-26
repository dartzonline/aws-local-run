"""State persistence manager for LocalRun — saves and loads as JSON."""
import base64
import dataclasses
import json
import logging
import os

logger = logging.getLogger("localrun.state")


def _serialize(obj):
    """Recursively convert obj to something JSON-serializable."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, bytes):
        # base64-encode binary data so it survives JSON round-trips
        return {"__bytes__": base64.b64encode(obj).decode("ascii")}
    if isinstance(obj, dict):
        # JSON only allows string keys — convert tuples and other non-string keys
        result = {}
        for k, v in obj.items():
            if isinstance(k, tuple):
                key = "__tuple__" + json.dumps(list(k))
            else:
                key = str(k) if not isinstance(k, str) else k
            result[key] = _serialize(v)
        return result
    if isinstance(obj, (list, tuple)):
        return [_serialize(v) for v in obj]
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        d = {f.name: _serialize(getattr(obj, f.name)) for f in dataclasses.fields(obj)}
        d["__class__"] = type(obj).__qualname__
        return d
    # last resort — try __dict__, otherwise stringify
    try:
        return _serialize(vars(obj))
    except TypeError:
        return str(obj)


def _deserialize(data):
    """Reverse _serialize. Returns plain dicts/lists, bytes for binary markers."""
    if data is None or isinstance(data, (bool, int, float, str)):
        return data
    if isinstance(data, list):
        return [_deserialize(v) for v in data]
    if isinstance(data, dict):
        if "__bytes__" in data:
            return base64.b64decode(data["__bytes__"])
        result = {}
        for k, v in data.items():
            if k.startswith("__tuple__"):
                k = tuple(json.loads(k[len("__tuple__"):]))
            result[k] = _deserialize(v)
        return result
    return data


def _engine_state(engine):
    """Return just the data attrs of an engine, skipping injected refs and callables."""
    skip = {"sqs", "sns"}  # cross-service refs injected by gateway — don't persist
    out = {}
    for k, v in vars(engine).items():
        if k.startswith("_") or k in skip or callable(v):
            continue
        out[k] = _serialize(v)
    return out


class StateManager:
    def __init__(self, data_dir=None):
        self._data_dir = data_dir

    def _state_file(self, path=None):
        if path:
            return path
        from localrun.config import get_config
        data_dir = self._data_dir or get_config().data_dir
        if not data_dir:
            return None
        os.makedirs(data_dir, exist_ok=True)
        return os.path.join(data_dir, "localrun_state.json")

    def save_state(self, engines, path=None):
        state_file = self._state_file(path)
        if not state_file:
            logger.warning("Cannot save state: LOCALRUN_DATA_DIR is not set")
            return False
        try:
            state = {}
            for name, engine in engines.items():
                state[name] = _engine_state(engine)
            with open(state_file, "w") as f:
                json.dump(state, f, indent=2)
            logger.info("Saved state to %s", state_file)
            return True
        except Exception as e:
            logger.error("Failed to save state: %s", e)
            return False

    def load_state(self, engines, path=None):
        state_file = self._state_file(path)
        if not state_file:
            logger.warning("Cannot load state: LOCALRUN_DATA_DIR is not set")
            return False
        # If the JSON file doesn't exist but an old .pkl does, just warn and skip
        if not os.path.exists(state_file):
            pkl = state_file.replace(".json", ".pkl")
            if os.path.exists(pkl):
                logger.warning("Old pickle state file found at %s — not auto-migrated. Delete it or use the new JSON format.", pkl)
            return False
        try:
            with open(state_file, "r") as f:
                state = json.load(f)
            for name, data in state.items():
                engine = engines.get(name)
                if not engine:
                    continue
                data = _deserialize(data)
                for attr, val in data.items():
                    if hasattr(engine, attr):
                        setattr(engine, attr, val)
            logger.info("Loaded state from %s", state_file)
            return True
        except Exception as e:
            logger.error("Failed to load state: %s", e)
            return False
