import base64
from datetime import datetime
from datetime import timedelta

import splunktalib.state_store as sss
import google_ta_common.google_consts as ggc
import cloud_monitor_mod.google_cloud_monitor_consts as gcmc

metric_date_fmt = "%Y-%m-%dT%H:%M:%S"


def add_timezone(metric_date_str):
    if not metric_date_str.endswith("-00:00"):
        metric_date_str = "{}-00:00".format(metric_date_str)
    return metric_date_str


def strip_off_timezone(metric_date_str):
    pos = metric_date_str.rfind("-00:00")
    if pos > 0:
        metric_date_str = metric_date_str[:pos]
    return metric_date_str


def strp_metric_date(metric_date_str):
    metric_date_str = strip_off_timezone(metric_date_str)
    return datetime.strptime(metric_date_str, metric_date_fmt)


def strf_metric_date(metric_date):
    mdate = datetime.strftime(metric_date, metric_date_fmt)
    return "{}-00:00".format(mdate)


def calculate_youngest(oldest, polling_interval, now):
    """
    return (youngest, done)
    """

    win = max(polling_interval, 600)
    youngest = oldest + timedelta(seconds=win)
    done = False
    if youngest >= now:
        youngest = now
        done = True

    return (youngest, done)


class GoogleCloudMonitorCheckpointer(object):

    def __init__(self, config):
        self._config = config
        self._key = base64.b64encode(config[ggc.name])
        self._store = sss.get_state_store(
            config, config[ggc.appname], collection_name=self._key,
            use_kv_store=config.get(ggc.use_kv_store))
        self._state = self._get_state()

    def _get_state(self):
        state = self._store.get_state(self._key)
        if not state:
            state = {
                gcmc.oldest: strip_off_timezone(self._config[gcmc.oldest]),
                "version": 1,
            }
        return state

    def oldest(self):
        return self._state[gcmc.oldest]

    def set_oldest(self, oldest, commit=True):
        oldest = strip_off_timezone(oldest)
        self._state[gcmc.oldest] = oldest
        if commit:
            self._store.update_state(self._key, self._state)

    def delete(self):
        self._store.delete_state(self._key)