"""
Copyright (C) 2005-2015 Splunk Inc. All Rights Reserved.

Commonly used design partten for python user, includes:
  - singleton (Decorator function used to build singleton)
"""

from functools import wraps
import traceback


def singleton(class_):
    """
    Singleton decoorator function.
    """
    instances = {}

    @wraps(class_)
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance


class Singleton(type):
    """
    Singleton meta class
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs)
        return cls._instances[cls]


def catch_all(logger):
    def catch_all_call(func):
        def __call__(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                logger.error("Failed to execute function=%s, error=%s",
                             func.__name__, traceback.format_exc())
        return __call__
    return catch_all_call
