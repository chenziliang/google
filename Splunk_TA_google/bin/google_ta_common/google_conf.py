import base64
import json

from splunktalib.common import log
logger = log.Logs().get_logger("main")


import splunktalib.modinput as modinput
import splunktalib.conf_manager.ta_conf_manager as tcm
import splunktalib.common.util as utils
import splunktalib.hec_config as hc
import google_ta_common.google_consts as ggc


class GoogleConfig(object):
    _appname = ggc.splunk_ta_google

    def __init__(self, service):
        self.service = service
        metas, stanzas = modinput.get_modinput_configs_from_stdin()
        self.metas = metas
        self.stanzas = stanzas
        self._task_configs = self._get_tasks()
        self._handle_hec()

    @staticmethod
    def data_collection_conf():
        return None

    @staticmethod
    def _metric_key_and_sep():
        return None, None

    def _get_tasks(self):
        conf_mgr = tcm.TAConfManager(
            self.data_collection_conf(), self.metas[ggc.server_uri],
            self.metas[ggc.session_key], appname=self._appname)
        conf_mgr.reload()
        data_collections = conf_mgr.all(return_acl=False)
        if not data_collections:
            return []

        conf_mgr.set_conf_file(ggc.myta_global_settings_conf)
        conf_mgr.set_encrypt_keys([ggc.proxy_username, ggc.proxy_password])
        conf_mgr.reload()
        global_settings = conf_mgr.all(return_acl=False)

        conf_mgr.set_conf_file(ggc.myta_cred_conf)
        conf_mgr.reload()
        google_creds = conf_mgr.all(return_acl=False)
        for creds in google_creds.itervalues():
            # decoded = utils.unescape_json_control_chars(
            #     base64.b64decode(creds[gpggc.google_credentials]))
            decoded = base64.b64decode(creds[ggc.google_credentials])
            creds[ggc.google_credentials] = json.loads(decoded)
        return self._expand_tasks(
            global_settings, google_creds, data_collections)

    def _expand_tasks(self, global_settings, google_creds, data_collections):
        keys = [ggc.index, ggc.name]
        metric_key, metric_sep = self._metric_key_and_sep()
        all_tasks = []
        for task in data_collections.itervalues():
            cred_name = task[ggc.google_credentials_name]
            if cred_name not in google_creds:
                logger.error("Invalid credential name=%s", cred_name)
                continue

            # Expand metrics
            if metric_sep:
                metrics = task[metric_key].split(metric_sep)
            else:
                metrics = [task[metric_key]]

            for metric in metrics:
                metric = metric.strip()
                if not metric:
                    continue

                new_task = {}
                new_task.update(task)
                with utils.save_and_restore(new_task, keys):
                    new_task.update(global_settings[ggc.global_settings])
                    new_task.update(global_settings[ggc.proxy_settings])
                    new_task.update(google_creds[cred_name])
                    new_task.update(self.metas)
                new_task[ggc.google_service] = self.service
                new_task[ggc.appname] = self._appname
                new_task[metric_key] = metric
                all_tasks.append(new_task)

        return all_tasks

    def get_tasks(self):
        return self._task_configs

    def _handle_hec(self):
        if not self._task_configs:
            return

        use_hec = utils.is_true(self._task_configs[0].get(ggc.use_hec))
        use_raw_hec = utils.is_true(self._task_configs[0].get(ggc.use_raw_hec))
        if not use_hec and not use_raw_hec:
            return

        hec = hc.HECConfig(
            self.metas[ggc.server_uri], self.metas[ggc.session_key])

        hec_input = hec.get_http_input("google_cloud_platform")
        port = self._task_configs[0].get(ggc.hec_port, 8088)
        if not hec_input:
            logger.info("Create HEC data input")
            hec_settings = {
                "enableSSL": 1,
                "port": port,
                "output_mode": "json",
            }
            hec.update_settings(hec_settings)
            input_settings = {
                "name": "google_cloud_platform",
                "description": "HTTP input for Google Cloud Platform AddOn"
            }
            hec.create_http_input(input_settings)
            hec_input = hec.get_http_input("google_cloud_platform")

        hostname, _ = utils.extract_hostname_port(
            self.metas[ggc.server_uri])
        hec_uri = "https://{hostname}:{port}".format(
            hostname=hostname, port=port)
        if hec_input:
            for task in self._task_configs:
                task.update(hec_input[0])
                task["hec_server_uri"] = hec_uri
        else:
            raise Exception("Failed to get HTTP input configuration")
