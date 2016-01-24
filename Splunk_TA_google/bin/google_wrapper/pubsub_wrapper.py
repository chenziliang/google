import traceback
import time
import base64

import google_wrapper.common as gwc


PUBSUB_SCOPES = ["https://www.googleapis.com/auth/pubsub"]


def get_full_subscription_name(project, subscription):
    """Return a fully qualified subscription name."""
    return gwc.fqrn("subscriptions", project, subscription)


def get_full_topic_name(project, topic):
    """Return a fully qualified topic name."""
    return gwc.fqrn('topics', project, topic)


class GooglePubSub(object):

    def __init__(self, logger, config):
        """
        :param: config
        {
            "proxy_url": xxx,
            "proxy_port": xxx,
            "proxy_username": xxx,
            "proxy_password": xxx,
            "proxy_rdns": xxx,
            "proxy_type": xxx,
            "google_credentials": xxx,
            "google_project": xxx,
            "google_subscription": xxx,
            "google_topic": xxx,
            "batch_size": xxx,
            "base64encoded": True/False,
        }
        """

        self._config = config
        self._config["scopes"] = PUBSUB_SCOPES
        self._logger = logger
        self._client = gwc.create_google_client(self._config)

    def pull_messages(self):
        """Pull messages from a given subscription."""

        subscription = get_full_subscription_name(
            self._config["google_project"],
            self._config["google_subscription"])

        body = {
            "returnImmediately": False,
            "maxMessages": self._config.get("batch_size", 100)
        }

        base64encoded = self._config.get("base64encoded")
        while 1:
            try:
                resp = self._client.projects().subscriptions().pull(
                    subscription=subscription, body=body).execute(
                    num_retries=3)
            except Exception:
                self._logger.error(
                    "Failed to pull messages from subscription=%s, error=%s",
                    subscription, traceback.format_exc())
                time.sleep(2)
                continue

            messages = resp.get("receivedMessages")
            if not messages:
                continue

            if base64encoded:
                for message in messages:
                    msg = message.get("message")
                    if msg and msg.get("data"):
                        msg["data"] = base64.b64decode(str(msg["data"]))

            yield messages

    def ack_messages(self, messages):
        ack_ids = []
        for message in messages:
            ack_ids.append(message.get("ackId"))

        ack_body = {"ackIds": ack_ids}
        subscription = get_full_subscription_name(
            self._config["google_project"],
            self._config["google_subscription"])

        self._client.projects().subscriptions().acknowledge(
            subscription=subscription, body=ack_body).execute(num_retries=3)

    def publish_messages(self, messages):
        topic = get_full_topic_name(
            self._config["google_project"], self._config["google_topic"])
        messages = [{"data": base64.b64encode(msg)} for msg in messages]
        body = {'messages': messages}
        return self._client.projects().topics().publish(
            topic=topic, body=body).execute(num_retries=3)


if __name__ == "__main__":
    import os
    import logging
    import threading

    logger = logging.getLogger("google")
    ch = logging.StreamHandler()
    logger.addHandler(ch)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "zlchenken-78c88c5c115b.json"
    config = {
        "google_project": "zlchenken",
        "google_topic": "test_topic",
        "google_subscription": "sub_test_topic",
        "base64encoded": True,
    }

    def pub():
        ps = GooglePubSub(logger, config)
        for i in range(10):
            messages = ["i am counting {} {}".format(i, j) for j in range(10)]
            print "publishing ", messages
            ps.publish_messages(messages)
            time.sleep(2)

    pubthr = threading.Thread(target=pub)
    pubthr.start()

    def sub():
        ps = GooglePubSub(logger, config)
        for messages in ps.pull_messages():
            print "consuming", messages
            ps.ack_messages(messages)

    subthr = threading.Thread(target=sub)
    subthr.start()

    pubthr.join()
    subthr.join()
