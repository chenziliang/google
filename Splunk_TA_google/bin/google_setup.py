"""
Copyright (C) 2005 - 2010 Splunk Inc. All Rights Reserved.
Description:  This skeleton python script handles the parameters in the
configuration page.

    handleList method: lists configurable parameters in the configuration page
    corresponds to handleractions = list in restmap.conf

    handleEdit method: controls the parameters and saves the values
    corresponds to handleractions = edit in restmap.conf
"""

import json
from base64 import b64encode
from base64 import b64decode


import splunk.clilib.cli_common as scc
import splunk.admin as admin

import google_rest_import_guard as grig

import splunktalib.common.util as utils
import splunktalib.common.log as log
from splunktalib.conf_manager import ta_conf_manager as ta_conf
from splunktalib.conf_manager import conf_manager as conf
import splunktalib.common.pattern as scp
import google_ta_common.google_consts as c
import pubsub_mod.google_pubsub_consts as gpc

logger = log.Logs().get_logger("setup")


def reload_confs(conf_mgr):
    conf_mgr.reload_conf(c.myta_global_settings_conf)
    conf_mgr.reload_conf(c.myta_cred_conf)
    conf_mgr.reload_conf(c.myta_data_collection_conf)


class ConfigApp(admin.MConfigHandler):
    valid_args = ("all_settings",)

    stanza_map = {
        c.global_settings: True,
        c.proxy_settings: True,
        c.credential_settings: True,
        c.data_collection_settings: True,
    }

    # encrypt_fields = (gpc.google_credentials, c.proxy_username,
    #                  c.proxy_password)
    encrypt_fields = (c.proxy_username, c.proxy_password)

    confs = ((c.credential_settings, c.myta_cred_conf),
             (c.data_collection_settings, c.myta_data_collection_conf))

    def setup(self):
        """
        Set up supported arguments
        """

        if self.requestedAction == admin.ACTION_EDIT:
            for arg in self.valid_args:
                self.supportedArgs.addOptArg(arg)

        conf_mgr = conf.ConfManager(scc.getMgmtUri(), self.getSessionKey(),
                                    app_name=self.appName)
        reload_confs(conf_mgr)

    @scp.catch_all(logger)
    def handleList(self, confInfo):
        logger.info("start list")

        all_settings = {}

        # 1) Global settings / Proxy settings
        ta_conf_mgr = ta_conf.TAConfManager(
            c.myta_global_settings_conf, scc.getMgmtUri(),
            self.getSessionKey())

        ta_conf_mgr.set_encrypt_keys(self.encrypt_fields)
        all_settings = ta_conf_mgr.all(return_acl=False)
        if not all_settings:
            all_settings = {}
        self._setNoneValues(all_settings.get(c.global_settings, {}))
        self._setNoneValues(all_settings.get(c.proxy_settings, {}))

        # 2) Credentials / Data colletions
        for name, conf_file in self.confs:
            ta_conf_mgr = ta_conf.TAConfManager(
                conf_file, scc.getMgmtUri(), self.getSessionKey())
            ta_conf_mgr.set_encrypt_keys(self.encrypt_fields)
            settings = ta_conf_mgr.all(return_acl=False)
            if settings:
                for setting in settings.itervalues():
                    cred = setting.get(gpc.google_credentials)
                    if cred:
                        setting[gpc.google_credentials] = b64decode(cred)
                self._setNoneValues(settings)
                all_settings.update({name: settings})

        # self._clearPasswords(all_settings, self.encrypt_fields)

        all_settings = json.dumps(all_settings)
        # all_settings = utils.unescape_json_control_chars(all_settings)
        confInfo[c.myta_settings].append(c.all_settings, all_settings)

        logger.info("end list")

    @scp.catch_all(logger)
    def handleEdit(self, confInfo):
        logger.info("start edit")

        # all_settings = utils.escape_json_control_chars(
        #    self.callerArgs.data[c.all_settings][0])
        all_settings = self.callerArgs.data[c.all_settings][0]
        all_settings = json.loads(all_settings)
        proxy_settings = all_settings[c.proxy_settings]
        if utils.is_false(proxy_settings.get(c.proxy_enabled, None)):
            del all_settings[c.proxy_settings]

        # 1) Global settings / Proxy settings
        ta_conf_mgr = ta_conf.TAConfManager(
            c.myta_global_settings_conf, scc.getMgmtUri(),
            self.getSessionKey())

        ta_conf_mgr.set_encrypt_keys(self.encrypt_fields)
        for k in (c.global_settings, c.proxy_settings):
            settings = all_settings.get(k, {})
            settings[c.name] = k
            ta_conf_mgr.update(settings)

        # 2) Credentials / Data collections
        for name, conf_file in self.confs:
            settings = all_settings.get(name, {})
            ta_conf_mgr = ta_conf.TAConfManager(
                conf_file, scc.getMgmtUri(), self.getSessionKey(),
                appname=self.appName)
            ta_conf_mgr.set_encrypt_keys(self.encrypt_fields)
            self._updateCredentials(settings, ta_conf_mgr)

        logger.info("end edit")

    def _updateCredentials(self, all_creds, ta_conf_mgr):
        all_origin_creds = ta_conf_mgr.all(return_acl=False)
        if all_origin_creds is None:
            all_origin_creds = {}

        for name, settings in all_creds.iteritems():
            settings[c.name] = name
            cred = settings.get(gpc.google_credentials)
            if cred:
                settings[gpc.google_credentials] = b64encode(cred)

            if name not in all_origin_creds:
                logger.info("new %s stanza", name)
                ta_conf_mgr.create(settings)
            else:
                ta_conf_mgr.update(settings)

        for name, settings, in all_origin_creds.iteritems():
            if name not in all_creds:
                logger.info("remove %s stanza", name)
                ta_conf_mgr.delete(name)

    @staticmethod
    def _clearPasswords(settings, cred_fields):
        for k, val in settings.iteritems():
            if isinstance(val, dict):
                return ConfigApp._clearPasswords(val, cred_fields)
            elif isinstance(val, (str, unicode)):
                if k in cred_fields:
                    settings[k] = ""

    @staticmethod
    def _setNoneValues(stanza):
        for k, v in stanza.iteritems():
            if v is None:
                stanza[k] = ""


admin.init(ConfigApp, admin.CONTEXT_APP_ONLY)
