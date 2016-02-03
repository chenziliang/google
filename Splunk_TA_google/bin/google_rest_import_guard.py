"""
This module is used to filter and reload PATH.
"""

import os.path as op
import sys
import re

ta_name = op.basename(op.dirname(op.dirname(op.abspath(__file__))))
ta_lib_name = re.sub(r"[^\w]+", "_", ta_name.lower())

assert ta_name or ta_name == "package", "TA name is None or package"

pattern = re.compile(r"[\\/]etc[\\/]apps[\\/][^\\/]+[\\/]bin[\\/]?$")
new_paths = [path for path in sys.path
             if not pattern.search(path) or ta_name in path]
new_paths.insert(0, op.sep.join([op.dirname(__file__), ta_lib_name]))
new_paths.insert(0, op.dirname(__file__))
sys.path = new_paths
