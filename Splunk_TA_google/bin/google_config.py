import os.path as op
import re
import base64
import json
import socket
from contextlib import contextmanager
import itertools
import random
import time
import multiprocessing

import splunktalib.modinput as modinput
import splunktalib.conf_manager.ta_conf_manager as tcm
import splunktalib.file_monitor as fm
import splunktalib.state_store as ss
import splunktalib.splunk_cluster as sc
import splunktalib.common.util as utils
from splunktalib.common import log

import google_ta_common.google_consts as c
import pubsub_mod.google_pubsub_consts as gpc


logger = log.Logs().get_logger("main")


def _get_conf_files():
    cur_dir = op.dirname(op.dirname(op.abspath(__file__)))
    files = []
    all_confs = [c.myta_global_settings_conf, c.myta_cred_conf,
                 c.myta_data_collection_conf]
    for f in all_confs:
        files.append(op.join(cur_dir, "local", f))
    logger.info(files)
    return files


def create_conf_monitor(callback):
    return fm.FileMonitor(callback, _get_conf_files())


def _use_kv_store(server_info):
    # Only when release >= 6.2, in SHC, use global KV store
    if server_info.version() < "6.2":
        return False

    if server_info.is_shc_member():
        return True

    return False


def create_state_store(meta_configs, appname, server_info):
    if _use_kv_store(server_info):
        store = ss.get_state_store(
            meta_configs, appname, appname.lower(),
            use_kv_store=True)
        logger.info("Use KVStore to do ckpt")
    else:
        store = ss.get_state_store(meta_configs, appname)
    return store


def setup_signal_handler(data_loader, gconfig):
    """
    Setup signal handlers
    :data_loader: data_loader.DataLoader instance
    """

    def _handle_exit(signum, frame):
        if gconfig is not None and gconfig.is_dispatcher():
            logger.info("Remove file lock")
            store = create_state_store(
                gconfig.metas, gconfig._appname,
                gconfig._server_info)
            store.delete_state(FileLock.lock_key)

        logger.info("google_pubsub is going to exit...")

        if data_loader is not None:
            data_loader.stop()

    utils.handle_tear_down_signals(_handle_exit)


def extract_hostname_port(uri):
    assert uri

    hostname, port = None, None

    if uri.startswith("https://"):
        uri = uri[8:]
    elif uri.startswith("http://"):
        uri = uri[7:]

    m = re.search(r"([^/:]+):(\d+)", uri)
    if m:
        hostname = m.group(1)
        port = m.group(2)
    return hostname, port


@contextmanager
def save_and_restore(stanza, keys, exceptions=()):
    vals = [stanza.get(key) for key in keys]
    yield
    for key, val in itertools.izip(keys, vals):
        if (key, val) not in exceptions:
            stanza[key] = val


class FileLock(object):
    lock_key = "ckpt.lock"

    def __init__(self, meta_configs, appname, server_info):
        self._locked = False

        random.seed(time.time())
        time.sleep(random.uniform(1, 3))

        self._store = create_state_store(meta_configs, appname, server_info)
        while 1:
            state = self._store.get_state(self.lock_key)
            if not state or time.time() - state["time"] > 300:
                state = {
                    "pid": multiprocessing.current_process().ident,
                    "time": time.time(),
                }
                # grap it
                self._store.update_state(self.lock_key, state)
                self._locked = True
                logger.info("Grapped ckpt.lock")
                break
            else:
                logger.debug("Ckpt lock is held by other mod process")
                time.sleep(1)

    def locked(self):
        return self._locked

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._locked:
            self._store.delete_state(self.lock_key)
            logger.info("Removed ckpt.lock")


class GoogleConfig(object):
    _appname = c.splunk_ta_google
    _current_hostname = socket.gethostname()
    _current_addrs = []
    try:
        _current_addrs = socket.gethostbyname_ex(_current_hostname)
    except Exception:
        pass
    _current_hostname = _current_hostname.lower()

    def __init__(self):
        metas, stanzas = modinput.get_modinput_configs_from_stdin()
        self.metas = metas
        self.stanzas = stanzas
        self._task_configs = []
        self._server_info = sc.ServerInfo(self.metas[c.server_uri],
                                          self.metas[c.session_key])
        setup_signal_handler(None, self)
        self._servername, self._port = extract_hostname_port(
            self.metas[c.server_uri])

        self._task_configs = self._get_tasks()

    def _get_tasks(self):
        conf_mgr = tcm.TAConfManager(
            c.myta_data_collection_conf, self.metas[c.server_uri],
            self.metas[c.session_key])
        conf_mgr.reload()
        data_collections = conf_mgr.all(return_acl=False)
        if not data_collections:
            return []

        conf_mgr.set_conf_file(c.myta_global_settings_conf)
        conf_mgr.set_encrypt_keys(["proxy_username", "proxy_password"])
        conf_mgr.reload()
        global_settings = conf_mgr.all(return_acl=False)

        conf_mgr.set_conf_file(c.myta_cred_conf)
        conf_mgr.reload()
        google_creds = conf_mgr.all(return_acl=False)
        for creds in google_creds.itervalues():
            # decoded = utils.unescape_json_control_chars(
            #     base64.b64decode(creds[gpc.google_credentials]))
            decoded = base64.b64decode(creds[gpc.google_credentials])
            creds[gpc.google_credentials] = json.loads(decoded)

        keys = ["index", "name"]
        for task in data_collections.itervalues():
            cred_name = task[gpc.google_credentials_name]
            if cred_name not in google_creds:
                logger.error("Invalid credential name=%s", cred_name)
                continue

            with save_and_restore(task, keys):
                task.update(global_settings[c.global_settings])
                task.update(global_settings[c.proxy_settings])
                task.update(google_creds[cred_name])
                task.update(self.metas)
            task[c.google_service] = c.google_pubsub

        return data_collections.values()

    def get_tasks(self):
        return self._task_configs

    def is_dispatcher(self, server_info=None):
        conf_mgr = tcm.TAConfManager(
            c.myta_global_settings_conf, self.metas[c.server_uri],
            self.metas[c.session_key])
        global_settings = conf_mgr.all(return_acl=False)

        if global_settings[c.node_settings][gpc.role] != gpc.dispatcher:
            return False

        if not server_info:
            server_info = sc.ServerInfo(self.metas[c.server_uri],
                                        self.metas[c.session_key])

        if server_info.is_shc_member():
            # In SHC env, only captain is able to dispatch the tasks
            if not server_info.is_captain():
                logger.info("This SH is not captain, ignore task dispatching")
                return False
        return True

    def is_current_host(self, hostname):
        hostname, port = extract_hostname_port(hostname)
        if port != self._port:
            return False

        local_hosts = ["localhost", "127.0.0.1", self._servername]
        if hostname in local_hosts:
            return True

        hostname = hostname.lower()
        if hostname == self._current_hostname:
            return True

        if not self._current_addrs:
            return False

        if (hostname == self._current_addrs[0] or
                hostname in self._current_addrs[-1]):
            return True

        return False
