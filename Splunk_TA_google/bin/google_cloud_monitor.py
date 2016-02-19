#!/usr/bin/python

"""
This is the main entry point for My TA
"""

import time

from splunktalib.common import log
import cloud_monitor_mod.google_cloud_monitor_consts as gmc
logger = log.Logs(gmc.monitor_ns).get_logger("main")

import splunktalib.common.util as utils
import splunktalib.common.pattern as gcp
import splunktalib.data_loader_mgr as dlm
import google_ta_common.ta_common as tacommon
import cloud_monitor_mod.google_cloud_monitor_conf as mconf
import cloud_monitor_mod.google_cloud_monitor_data_loader as gmdl


utils.remove_http_proxy_env_vars()
utils.disable_stdout_buffer()


def print_scheme():
    title = "Splunk AddOn for Google"
    description = "Collect and index Google Cloud Monitor data"
    tacommon.print_scheme(title, description)


@gcp.catch_all(logger)
def run():
    """
    Main loop. Run this TA forever
    """

    logger.info("Start google_cloud_monitor")
    metas, tasks = tacommon.get_configs(
        mconf.GoogleCloudMonitorConfig, "google_cloud_monitor", logger)

    if not tasks:
        return

    loader_mgr = dlm.create_data_loader_mgr(tasks[0])
    tacommon.setup_signal_handler(loader_mgr, logger)

    conf_change_handler = tacommon.get_file_change_handler(loader_mgr, logger)
    conf_monitor = tacommon.create_conf_monitor(
        conf_change_handler, [gmc.myta_data_collection_conf])
    loader_mgr.add_timer(conf_monitor, time.time(), 10)

    jobs = [gmdl.GoogleCloudMonitorDataLoader(task) for task in tasks]
    loader_mgr.start(jobs)
    logger.info("End google_cloud_monitor")


def main():
    """
    Main entry point
    """

    tacommon.main(print_scheme, run)


if __name__ == "__main__":
    main()
