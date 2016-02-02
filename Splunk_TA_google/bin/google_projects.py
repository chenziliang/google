import traceback


import splunk.clilib.cli_common as scc
import splunk.admin as admin


from splunktalib.common import log
# import splunktalib.common.util as utils


logger = log.Logs().get_logger("custom_rest")


class GoogleProjects(admin.MConfigHandler):
    valid_params = ["google_credentials"]

    def setup(self):
        for param in self.valid_params:
            self.supportedArgs.addOptArg(param)

    def handleList(self, conf_info):
        logger.info("start list google projects")
        self._doList(self.callerArgs, conf_info)
        # conf_info['RegionsResult'].append('regions', ["abc"])
        logger.info("end list google projects")

    def _doList(self, caller_args, conf_info):
        logger.info(caller_args)


def main():
    admin.init(GoogleProjects, admin.CONTEXT_NONE)


if __name__ == "__main__":
    main()
