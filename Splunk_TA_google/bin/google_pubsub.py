#!/usr/bin/python

"""
This is the main entry point for My TA
"""

import time

import splunktalib.common.util as utils
import splunktalib.common.pattern as gcp
import splunktalib.orphan_process_monitor as opm
from splunktalib.common import log

import google_config as gconfig
import google_concurrent_data_loader as gcdl
import google_ta_common.ta_common as tacommon


utils.remove_http_proxy_env_vars()
utils.disable_stdout_buffer()

logger = log.Logs().get_logger("main")


def print_scheme():
    title = "Splunk AddOn for Google"
    description = "Collect and index PubSub data for Google"
    tacommon.print_scheme(title, description)


@gcp.catch_all(logger)
def run():
    """
    Main loop. Run this TA forever
    """

    logger.info("Start google_pubsub")
    metas, tasks = tacommon.get_configs(
        gconfig.GoogleConfig, "google_pubsub", logger)

    if not tasks:
        return

    loader = gcdl.GoogleDataLoaderManager(tasks)
    tacommon.setup_signal_handler(loader, logger)

    conf_change_handler = tacommon.get_file_change_handler(loader, logger)
    conf_monitor = gconfig.create_conf_monitor(conf_change_handler)
    loader.add_timer(conf_monitor, time.time(), 10)

    orphan_checker = opm.OrphanProcessChecker(loader.stop)
    loader.add_timer(orphan_checker.check_orphan, time.time(), 1)

    for i in range(15):
        if loader.received_stop_signal():
            return
        time.sleep(1)

    loader.start()
    logger.info("End google_pubsub")


def main():
    """
    Main entry point
    """

    tacommon.main(print_scheme, run)


if __name__ == "__main__":
    main()
