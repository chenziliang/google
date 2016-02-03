import base64
import json

import google_rest_import_guard as grig

# import splunk.clilib.cli_common as scc
import splunk.admin as admin

import splunktalib.common.pattern as scp
import splunktalib.common.util as scu

import pubsub_mod.google_pubsub_consts as gpc
import google_wrapper.resource_manager_wrapper as grm


from splunktalib.common import log

logger = log.Logs().get_logger("custom_rest")


class GoogleProjects(admin.MConfigHandler):
    valid_params = ["google_credentials"]

    def setup(self):
        for param in self.valid_params:
            self.supportedArgs.addOptArg(param)

    @scp.catch_all(logger)
    def handleList(self, conf_info):
        logger.info("start list google projects")
        if not self.callerArgs or not self.callerArgs[gpc.google_credentials]:
            logger.error("Missing Google credentials")
            return

        creds = self.callerArgs[gpc.google_credentials][0]
        creds = scu.unescape_json_control_chars(base64.b64decode(creds))
        creds = json.loads(creds)
        config = {
            gpc.google_credentials: creds,
        }
        res_mgr = grm.GoogleResourceManager(logger, config)
        projects = [project["name"] for project in res_mgr.projects()]
        conf_info["google_projects"].append("projects", projects)
        logger.info("end list google projects")


def main():
    admin.init(GoogleProjects, admin.CONTEXT_NONE)


if __name__ == "__main__":
    main()
