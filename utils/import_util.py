# -*- coding: utf-8 -*-
import os
import logging
import inspect
import pkgutil
import importlib

logger = logging.getLogger(__name__)


def import_submodules(package, recursive=True):
    """ Import all submodules of a module, recursively, including subpackages

    :param package: package (name or actual module)
    :type package: str | module
    :rtype: dict[str, types.ModuleType]
    """
    if isinstance(package, str):
        package = package.replace('/', '.').replace(os.sep, '.')
        package = importlib.import_module(package)
    results = {}
    for _, name, is_pkg in pkgutil.walk_packages(package.__path__):
        if name.startswith('_'):
            continue
        full_name = package.__name__ + '.' + name
        try:
            results[full_name] = importlib.import_module(full_name)
        except Exception as e:
            logger.exception('Failed to import %s: %s', full_name, e)
        if recursive and is_pkg:
            results.update(import_submodules(full_name))
    return results


def discovery_items_in_package(package, func_lookup=inspect.isfunction):
    """
        discovery all function at most depth(2) in specified package
    """
    functions = []
    _modules = import_submodules(package)
    for _k, _m in _modules.items():
        functions.extend(inspect.getmembers(_m, func_lookup))

    return functions


def load_modules(path, func_lookup=None):
    """
        加载指定目录下的所有类(不包含同名的类)
    """
    models = {}

    path = path.replace('/', '.').replace(os.sep, '.')
    package = importlib.import_module(path)
    all_modules = discovery_items_in_package(package, func_lookup)

    for _k, _m in all_modules:
        models[_k] = _m
    return models
