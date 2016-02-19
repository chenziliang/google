import google_ta_common.google_conf as gconf
import google_ta_common.google_consts as ggc
import cloud_monitor_mod.google_cloud_monitor_consts as gmc


class GoogleCloudMonitorConfig(gconf.GoogleConfig):

    def __init__(self):
        super(GoogleCloudMonitorConfig, self).__init__(
            ggc.google_cloud_monitor)

    @staticmethod
    def data_collection_conf():
        return gmc.myta_data_collection_conf
