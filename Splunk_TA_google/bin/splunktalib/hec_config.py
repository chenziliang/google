import traceback

import splunktalib.conf_manager.conf_manager as cm
import splunktalib.conf_manager.request as req
from splunktalib.common import log

logger = log.Logs().get_logger("util")


class HECConfig(object):
    """
    HTTP Event Collector configuration
    """

    input_type = "http"

    def __init__(self, splunkd_uri, session_key):
        self._conf_mgr = cm.ConfManager(splunkd_uri, session_key,
                                        app_name="splunk_httpinput")

    def update_settings(self, settings):
        """
        :settings: dict object
        {
        "enableSSL": 1/0,
        "disabled": 1/0,
        "useDeploymentServer": 1/0,
        "port": 8088,
        "output_mode": "json",
        }
        """

        try:
            self._conf_mgr.update_data_input(
                self.input_type, self.input_type, settings)
        except Exception:
            logger.error("Failed to update httpinput settings, reason=%s",
                         traceback.format_exc())
            raise

    def create_http_input(self, stanza):
        """
        :stanza: dict object
        {
        "name": "akamai",
        "index": "main", (optional)
        "sourcetype": "akamai:cm:json", (optional)
        "description": "xxx", (optional)
        "token": "A0-5800-406B-9224-8E1DC4E720B6", (optional)
        }
        """

        try:
            self._conf_mgr.create_data_input(
                self.input_type, stanza["name"], stanza)
        except req.ConfExistsException:
            pass
        except Exception:
            logger.error("Failed to create httpinput=%s, reason=%s",
                         stanza["name"], traceback.format_exc())
            raise

    def update_http_input(self, stanza):
        res = self.get_http_input(stanza["name"])
        if res is None:
            return self.create_http_input(stanza)

        self._conf_mgr.update_data_input(
            self.input_type, stanza["name"], stanza)

    def delete_http_input(self, name):
        """
        :name: string, http input name
        """

        try:
            self._conf_mgr.delete_data_input(self.input_type, name)
        except req.ConfNotExistsException:
            pass
        except Exception:
            logger.error("Failed to delete httpinput=%s, reason=%s",
                         name, traceback.format_exc())
            raise

    def get_http_input(self, name):
        """
        :name: string, http input name
        :return: list of http input config if successful or
        None when there is such http input or
        raise exception if other exception happened
        """

        try:
            return self._conf_mgr.get_data_input(self.input_type, name)
        except req.ConfNotExistsException:
            return None
        except Exception:
            logger.error("Failed to get httpinput=%s, reason=%s",
                         name, traceback.format_exc())
            raise


if __name__ == "__main__":
    import splunktalib.credentials as cred

    hosts = ["https://localhost:8089"]
    for host in hosts:
        session_key = cred.CredentialManager.get_session_key(
            "admin", "admin", host)

        hic = HECConfig(host, session_key)

        settings = {
            "enableSSL": 1,
            "disabled": 0,
            "useDeploymentServer": 0,
            "port": 8088,
        }
        hic.update_settings(settings)

        stanza = {
            "name": "akamai",
            "index": "main",
            "token": "A0-5800-406B-9224-8E1DC4E720B6",
            "sourcetype": "akamai:cm:json",
            "description": "akamai http input",
        }

        hic.delete_http_input("akamai")
        hic.create_http_input(stanza)
        print hic.get_http_input("akamai")
        hic.delete_http_input("akamai")
        hic.update_http_input(stanza)
        print hic.get_http_input("akamai")
