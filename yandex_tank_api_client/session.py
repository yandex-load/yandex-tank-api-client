"""

Simple stateless client for yandex-tank-api server

Client:
    client class
APIError, RetryLater:
    API-specific exceptions
make_ini:
    helper function for building tank config

"""

import yaml
import json
import urllib2
import logging
import re

class APIError(RuntimeError):

    """
    Raised when tank replies with error code and valid json
    """

    def __init__(self, msg='', response={}):
        full_msg = '\n'.join((msg, yaml.safe_dump(response)))
        RuntimeError.__init__(self, full_msg)
        self.response = response


class RetryLater(APIError):

    """
    Raised when error code is 503
    """
    pass


class NothingDone(APIError):

    """
    Raised when no action was performed by yandex-tank-api server
    """
    pass


class Client(object):

    """
    Session-independent methods
    """

    def __init__(self, tank, api_port=8888):
        self._tank = tank
        if re.match('.*?\:\d+',tank):
            self.url_base = 'http://'+tank
	else:
	    self.url_base = 'http://%s:%s' % (tank, api_port)
        self.log = logging.getLogger("Tank " + tank)

    @property
    def tank(self):
        """Tank host"""
        return self._tank

    def get_test_artifact_list(self, session_id):
        '''['filename1', 'filename2', ...]'''
        url = '/artifact?session=' + session_id
        http_code, response = self._get_json(url)
        if http_code != 200:
            raise APIError("Failed to obtain artifact list", response)
        return response

    def download_test_artifact(self, remote_filename, local_filename, session_id):
        """
        Downloads single artifact file from tank
        """
        url = '/artifact?session=%s&filename=%s' % (session_id, remote_filename)
        http_code, contents = self._get_str(url)
        if http_code == 503:
            raise RetryLater(
                "Artifact too large, download temporarily unavailable")
        if http_code != 200:
            raise APIError("Failed to download artifact", json.loads(contents))
        artifact_file = open(local_filename, 'w')
        artifact_file.write(contents)

    def _get_str(self, url, post_contents=None):
        """Returns /GET or /POST response as string"""
        try:
            full_url = self.url_base + url
            response = urllib2.urlopen(full_url, post_contents)
        except urllib2.HTTPError as ex:
            if ex.code != 503:
                self.log.error("API request %s returned %s", url, ex.code)
            else:
                self.log.info("API request %s returned 503", url)
            http_code = ex.code
            str_response = ex.read()
        else:
            http_code = response.getcode()
            str_response = response.read()
        if len(str_response) < 1000 or http_code != 200:
            self.log.debug("API returned code %s, contents:\n%s",
                           http_code, str_response)
        else:
            self.log.debug(
                "API returned code %s, content length>1KB", http_code)
        return (http_code, str_response)

    def _get_json(self, url, post_contents=None):
        """Returns /GET or /POST response as JSON"""
        http_code, str_response = self._get_str(url, post_contents)
        try:
            json_response = json.loads(str_response)
        except:
            raise RuntimeError("Faield to parse json from tank: %s"
                               % str_response)
        return (http_code, json_response)


class Session(Client):

    """
    Tank session
    """

    def __init__(
            self,
            tank,
            config_contents,
            api_port=8888,
            stage='finished',
            test_id=None
    ):
        super(Session, self).__init__(tank, api_port)
        url = '/run?break=' + stage
        if test_id is not None:
            url += '&test=%s' % test_id
        http_code, response = self._get_json(url, config_contents)
        if http_code == 200:
            self._session = response['session']
        elif http_code == 503:
            raise RetryLater("Failed to start session", response)
        else:
            raise APIError("Failed to start session", response)

    @property
    def s_id(self):
        """Session ID"""
        return self._session

    def set_breakpoint(self, stage='finished'):
        '''{"session": session_id}'''
        url = '/run?session=%s&break=%s' % (self._session, stage)
        http_code, response = self._get_json(url)
        if http_code == 418:
            raise NothingDone("Cannot move break backwards", response)
        if http_code != 200:
            raise APIError("Failed to change breakpoint", response)

    def get_status(self):
        """Returns session status"""
        url = '/status?session=' + self._session
        http_code, response = self._get_json(url)
        if http_code != 200:
            raise APIError("Failed to obtain session status", response)
        response['tank'] = self.tank
        return response

    def stop(self):
        """Sends stop request for current session"""
        url = '/stop?session=' + self._session
        http_code, response = self._get_json(url)
        if http_code == 404:
            raise NothingDone("Session does not exist", response)
        if http_code == 409:
            raise NothingDone("Session already stopped", response)
        if http_code != 200:
            raise APIError("Failed to stop session", response)
        return response

    def get_artifact_list(self):
        '''['filename1', 'filename2', ...]'''
        return self.get_test_artifact_list(self._session)

    def download_artifact(self, remote_filename, local_filename):
        """Downloads artifact for current test session"""
        return self.download_test_artifact(
            remote_filename,
            local_filename,
            self._session
        )

    def upload(self, local_path, remote_filename):
        """
        Uploads single file to tank
        """
        url = '/upload?session=%s&filename=%s' % (
            self._session, remote_filename)
        contents = open(local_path, 'rb').read()
        http_code, reply = self._get_json(url, post_contents=contents)
        if http_code != 200:
            raise APIError("Failed to upload file", json.loads(reply))
        return reply


def make_ini(params):
    """
    params: list of tuples [('phantom.instances',1000),...]

    returns .ini-formatted output, options order is preserved
    """
    # TODO: group options from one section
    lines = []
    section = None
    for name, value in params:
        new_section, option = name.split('.', 1)
        if section != new_section:
            section = new_section
            lines.append('[%s]' % section)
        lines.append('='.join((option, str(value))))
    return '\n'.join(lines)
