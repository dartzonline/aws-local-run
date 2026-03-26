"""Plugin registry for custom service emulators.

Users can register a custom service by calling register_plugin() from their
code, or by declaring a 'localrun.plugins' entry point in their package.

Each entry point should point to a zero-argument function that calls
register_plugin() with the service name and a factory callable.

Example pyproject.toml entry:
  [project.entry-points."localrun.plugins"]
  my_service = "my_package:setup_my_service"

Where setup_my_service does:
  from localrun.plugins import register_plugin
  def setup_my_service():
      register_plugin("myservice", MyServiceClass)
"""

import importlib.metadata
import logging

logger = logging.getLogger("localrun.plugins")

# List of {"name": str, "factory": callable}
_plugins = []


def register_plugin(name, factory):
    """Register a custom service engine.

    name    - the service key used to route requests (e.g. "myservice")
    factory - a callable that returns an engine object with a handle(req, path) method
    """
    _plugins.append({"name": name, "factory": factory})
    logger.info("Registered plugin: %s", name)


def load_entry_points():
    """Discover plugins registered under the 'localrun.plugins' entry point group."""
    try:
        eps = importlib.metadata.entry_points(group="localrun.plugins")
    except Exception:
        return

    for ep in eps:
        try:
            func = ep.load()
            func()
            logger.info("Loaded plugin entry point: %s", ep.name)
        except Exception as e:
            logger.warning("Failed to load plugin %s: %s", ep.name, e)


def inject_into_engines(engines):
    """Instantiate all registered plugins and add them to the engines dict."""
    for plugin in _plugins:
        name = plugin["name"]
        try:
            engine = plugin["factory"]()
            engines[name] = engine
            logger.info("Injected plugin engine: %s", name)
        except Exception as e:
            logger.warning("Plugin %s factory failed: %s", name, e)


def get_plugins():
    """Return the list of currently registered plugin descriptors."""
    return list(_plugins)
