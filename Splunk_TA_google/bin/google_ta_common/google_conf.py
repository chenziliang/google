import base64
import json

from splunktalib.common import log
logger = log.Logs().get_logger("main")


import splunktalib.modinput as modinput
import splunktalib.conf_manager.ta_conf_manager as tcm
import splunktalib.common.util as utils
import google_ta_common.google_consts as c


class GoogleConfig(object):
    _appname = c.splunk_ta_google

    def __init__(self, service):
        self.service = service
        metas, stanzas = modinput.get_modinput_configs_from_stdin()
        self.metas = metas
        self.stanzas = stanzas
        self._task_configs = []
        self._task_configs = self._get_tasks()

    @staticmethod
    def data_collection_conf():
        pass

    def _get_tasks(self):
        conf_mgr = tcm.TAConfManager(
            self.data_collection_conf(), self.metas[c.server_uri],
            self.metas[c.session_key])
        conf_mgr.reload()
        data_collections = conf_mgr.all(return_acl=False)
        if not data_collections:
            return []

        conf_mgr.set_conf_file(c.myta_global_settings_conf)
        conf_mgr.set_encrypt_keys([c.proxy_username, c.proxy_password])
        conf_mgr.reload()
        global_settings = conf_mgr.all(return_acl=False)

        conf_mgr.set_conf_file(c.myta_cred_conf)
        conf_mgr.reload()
        google_creds = conf_mgr.all(return_acl=False)
        for creds in google_creds.itervalues():
            # decoded = utils.unescape_json_control_chars(
            #     base64.b64decode(creds[gpc.google_credentials]))
            decoded = base64.b64decode(creds[c.google_credentials])
            creds[c.google_credentials] = json.loads(decoded)

        keys = [c.index, c.name]
        for task in data_collections.itervalues():
            cred_name = task[c.google_credentials_name]
            if cred_name not in google_creds:
                logger.error("Invalid credential name=%s", cred_name)
                continue

            with utils.save_and_restore(task, keys):
                task.update(global_settings[c.global_settings])
                task.update(global_settings[c.proxy_settings])
                task.update(google_creds[cred_name])
                task.update(self.metas)
            task[c.google_service] = self.service

        return data_collections.values()

    def get_tasks(self):
        return self._task_configs
