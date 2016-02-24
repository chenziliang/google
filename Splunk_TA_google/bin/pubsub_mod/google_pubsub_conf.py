import google_ta_common.google_conf as gconf
import google_ta_common.google_consts as ggc
import pubsub_mod.google_pubsub_consts as gpc


class GooglePubSubConfig(gconf.GoogleConfig):

    def __init__(self):
        super(GooglePubSubConfig, self).__init__(ggc.google_pubsub)

    @staticmethod
    def data_collection_conf():
        return gpc.myta_data_collection_conf

    @staticmethod
    def _metric_key_and_sep():
        return gpc.google_subscription, ","
