import base64
import json

import google_rest_import_guard as grig

# import splunk.clilib.cli_common as scc
import splunk.admin as admin

import splunktalib.common.pattern as scp
import splunktalib.common.util as scu

import pubsub_mod.google_pubsub_consts as gpc
import google_wrapper.pubsub_wrapper as gpw


from splunktalib.common import log

logger = log.Logs().get_logger("custom_rest")


class GoogleSubscriptions(admin.MConfigHandler):
    valid_params = ["google_credentials", "google_project"]

    def setup(self):
        for param in self.valid_params:
            self.supportedArgs.addOptArg(param)

    @scp.catch_all(logger)
    def handleList(self, conf_info):
        logger.info("start list google subscriptions")
        for required in self.valid_params:
            if not self.callerArgs or not self.callerArgs[required]:
                logger.error("Missing Google credentials")
                return

        creds = self.callerArgs[gpc.google_credentials][0]
        creds = scu.unescape_json_control_chars(base64.b64decode(creds))
        creds = json.loads(creds)

        project = self.callerArgs[gpc.google_project][0]
        config = {
            gpc.google_credentials: creds,
        }
        ps = gpw.GooglePubSub(logger, config)
        subscriptions = [sub["name"].split("/")[-1]
                         for sub in ps.subscriptions(project)]
        conf_info["google_subscriptions"].append(
            "subscriptions", subscriptions)
        logger.info("end list google subscriptions")


def main():
    admin.init(GoogleSubscriptions, admin.CONTEXT_NONE)


if __name__ == "__main__":
    main()
