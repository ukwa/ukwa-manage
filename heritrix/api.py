import sys

import requests
from requests.auth import HTTPDigestAuth


class API(object):


    def __init__(self, host='https://localhost:8443/engine',
        user='admin', passwd='', verbose=False, verify=False):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.config = {}
        if verbose:
            self.config['verbose'] = sys.stderr
        self.verify = verify

    def _post(self, action='', url='', data={}):
        if action == '':
            return None
        if not url:
            url = self.host
        data['action'] = action
        headers = {'Accept': 'application/xml'}
        r = requests.post(url, auth=HTTPDigestAuth(self.user, self.passwd),
            data=data, headers=headers, config=self.config,
            verify=self.verify)
        return r

    def add(self, addpath=''):
        action = 'add'
        if addpath == '':
            return None
        return self._post(action, data={'addpath': addpath})

    def create(self, createpath=''):
        action = 'create'
        if createpath == '':
            return None
        return self._post(action, data={'createpath': createpath})

    def rescan(self):
        action = 'rescan'
        return self._post(action)

    def _job_action(self, action='', job=''):
        if action == '' \
            or job == '':
            return None
        url = '%s/job/%s' % (self.host, job)
        return self._post(action=action, url=url)

    def build(self, job=''):
        return self._job_action(action='build', job=job)

    def launch(self, job=''):
        return self._job_action(action='launch', job=job)
        
    def pause(self, job=''):
        return self._job_action(action='pause', job=job)

    def unpause(self, job=''):
        return self._job_action(action='unpause', job=job)

    def terminate(self, job=''):
        return self._job_action(action='terminate', job=job)

    def teardown(self, job=''):
        return self._job_action(action='teardown', job=job)

    def checkpoint(self, job=''):
        return self._job_action(action='checkpoint', job=job)

    def copy(self, copyTo='', asProfile=False):
        if copyTo == '':
            return None
        url = '%s/job/%s' % (self.host, job)
        data = {'copyTo': copyTo}
        if asProfile:
            data['asProfile'] = 'on'
        else:
            data['asProfile'] = 'off'
        headers = {'Accept': 'application/xml'}
        r = requests.post(url=url, auth=(self.user, self.passwd),
            data=data, headers=headers, config=self.config,
            verify=self.verify)
        return r

    def submit(self, job='', urls=[], config={}):
        # NOTE: is a PUT.
        # FIXME: hmm, what would be useful here?
        #r = requests.post(url=url, auth=(self.user, self.passwd),
        #    verify=self.verify)
        return
        
        
