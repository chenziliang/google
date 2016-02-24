import traceback
import threading
from datetime import datetime

from splunktalib.common import log
logger = log.Logs().get_logger("main")


import google_ta_common.google_consts as ggc
import google_wrapper.cloud_monitor_wrapper as gmw
import cloud_monitor_mod.google_cloud_monitor_consts as gmc
import cloud_monitor_mod.google_cloud_monitor_checkpointer as ckpt


class GoogleCloudMonitorDataLoader(object):

    def __init__(self, config):
        """
        :config: dict object
        {
            "appname": xxx,
            "use_kv_store": xxx,
            "proxy_url": xxx,
            "proxy_port": xxx,
            "proxy_username": xxx,
            "proxy_password": xxx,
            "proxy_rdns": xxx,
            "proxy_type": xxx,
            "google_credentials": xxx,
            "google_project": xxx,
            "google_metric": xxx,
            "index": xxx,
        }
        """

        config[ggc.polling_interval] = int(config[ggc.polling_interval])
        self._config = config
        self._source = "{project}:{metric}".format(
            project=self._config[ggc.google_project],
            metric=self._config[gmc.google_metric])
        self._store = ckpt.GoogleCloudMonitorCheckpointer(config)
        self._lock = threading.Lock()
        self._stopped = False

    def get_interval(self):
        return self._config[ggc.polling_interval]

    def get_props(self):
        return self._config

    def stop(self):
        self._stopped = True
        logger.info("Stopping GooglePubSubDataLoader")

    def __call__(self):
        self.index_data()

    def index_data(self):
        if self._lock.locked():
            logger.info("Last time of data collection for project=%s, "
                        "metric=%s is not done",
                        self._config[ggc.google_project],
                        self._config[gmc.google_metric])
            return

        with self._lock:
            self._do_index()

    def _do_index(self):
        logger.info("Start collecting data for project=%s, metric=%s, from=%s",
                    self._config[ggc.google_project],
                    self._config[gmc.google_metric],
                    self._store.oldest())
        try:
            self._do_safe_index()
        except Exception:
            logger.error(
                "Failed to collect data for project=%s, metric=%s, error=%s",
                self._config[ggc.google_project],
                self._config[gmc.google_metric], traceback.format_exc())
        logger.info("End of collecting data for project=%s, metric=%s",
                    self._config[ggc.google_project],
                    self._config[gmc.google_metric])

    def _do_safe_index(self):
        # 1) Cache max_events in memory before indexing for batch processing

        params = {
            ggc.google_project: self._config[ggc.google_project],
            gmc.google_metric: self._config[gmc.google_metric],
        }

        mon = gmw.GoogleCloudMonitor(logger, self._config)
        now = datetime.utcnow()
        oldest = ckpt.strp_metric_date(self._store.oldest())
        polling_interval = self._config[ggc.polling_interval]
        done, win = False, int(self._config.get("cm_win", 3600))
        while not done and not self._stopped:
            youngest, done = ckpt.calculate_youngest(
                oldest, polling_interval, now, win)
            params[gmc.oldest] = ckpt.strf_metric_date(oldest)
            params[gmc.youngest] = ckpt.strf_metric_date(youngest)
            logger.debug("Collect data for project=%s, metric=%s, win=[%s, %s]",
                         params[ggc.google_project], params[gmc.google_metric],
                         params[gmc.oldest], params[gmc.youngest])

            metrics = mon.list_metrics(params)
            if metrics:
                self._write_events(metrics)
            self._store.set_oldest(params[gmc.youngest])
            oldest = youngest

    def _write_events(self, metrics):
        msgs_str = [metric for metric in metrics]
        events = self._config[ggc.event_writer].create_events(
            index=self._config[ggc.index], host=None, source=self._source,
            sourcetype="google:cloudmonitor", time=None, unbroken=False,
            done=False, events=msgs_str)
        self._config[ggc.event_writer].write_events(events)


if __name__ == "__main__":
    import os
    import splunktalib.event_writer as ew

    writer = ew.ModinputEventWriter()
    writer.start()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "zlchenken-78c88c5c115b.json"
    config = {
        ggc.data_loader: writer,
        ggc.event_writer: writer,
        ggc.checkpoint_dir: ".",
        ggc.server_uri: "https://localhost:8089",
        ggc.server_host: "localhost",
        ggc.index: "main",
        ggc.google_project: "zlchenken",
        gmc.google_metric: "pubsub.googleapis.com/subscription/pull_request_count",
        gmc.oldest: "2016-01-20T00:00:00-00:00",
        ggc.checkpoint_dir: ".",
        ggc.polling_interval: 30,
        ggc.name: "test_cm",
        ggc.appname: "Splunk_TA_google-cloudplatform",
    }

    loader = GoogleCloudMonitorDataLoader(config)
    loader.index_data()
    writer.tear_down()
