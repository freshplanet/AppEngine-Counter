# -*- coding: utf-8 -*-
'''
Copyright 2014 FreshPlanet (http://freshplanet.com | opensource@freshplanet.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import base64
import contextlib
import importlib
import logging
import os
import time
import unittest

from google.appengine.api import urlfetch_stub
from google.appengine.api.system import system_stub
from google.appengine.ext import testbed, ndb, deferred
import webapp2


class AppEngineTest(unittest.TestCase):
    """ Base class for unit tests """
    
    def setUp(self):
        
        try:
            u"I'm {firstName}".format(firstName='éùàò')
            self.fail("Please setup you environment to US-ASCII to better mimic GAE environment.")
        except UnicodeDecodeError:
            pass
        
        self.testbed = testbed.Testbed()
        # activate the testbed, which prepares the service stubs for use.
        self.testbed.activate()
        # declare which service stubs we are going to use.
        yamlPath = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
        self.testbed.init_datastore_v3_stub(auto_id_policy=testbed.AUTO_ID_POLICY_SCATTERED, require_indexes=True, root_path=yamlPath)
        self.testbed.init_memcache_stub()
        self.testbed.init_channel_stub()
        self.testbed.init_app_identity_stub()
        self.testbed.init_taskqueue_stub(root_path=yamlPath)
        self.testbed.init_blobstore_stub()
        self.testbed._register_stub('system', system_stub.SystemServiceStub())
        
    def tearDown(self):
        # restore the original stubs so that tests do not interfere with each other.
        self.testbed.deactivate()
    
    def callOurUrl(self, url, postData=None, headers=None, executeTaskQueue=True, app=None):
        """
        @param postData: dict of key values to be set as application/x-www-form-urlencoded in the body
        @param headers: dict of key values to set as headers
        @param executeTaskQueue: If True we execute the tasks in the task queues that are scheduled to be executed until the end of the URL call.
                                Basically, if your request handler enqueues a task to be executed as soon as possible, it'll get executed.
        @param app: WSGI app to use to execute the request. By default we use the one provided by 'getApp()'.
        """
        # TODO: Should we use 'webtest' framework instead?
        # GAE sdk checks for 'SERVER_SOFTWARE' value at some places.
        environ = {'SERVER_SOFTWARE': 'Devel'}
        
        # Build a request object passing the URI path to be tested.
        request = webapp2.Request.blank(url, environ=environ, headers=headers, POST=postData)
        
        app = app or self.getApp()
        
        # Cheat for deferred handler that is still a webapp one to set the proper context.
        # => in prod it is somehow set but not while we run from there.
        if not isinstance(app, webapp2.WSGIApplication):
            webapp2._local.request = request
        
        # Get a response for that request.
        response = request.get_response(app)
        
        if executeTaskQueue:
            self.executeTaskQueue(maxEta=time.time())
        
        return response
    
    def getApp(self):
        """ The WSGI Application to use when calling our end points URLs """
        try:
            main = importlib.import_module('main')
            return main.app
        except (ImportError, AttributeError):
            self.fail("Could not find a WSGI App to call the URL on")
    
    def executeTaskQueue(self, assertNbTasks=None, maxEta=None):
        """
        Runs all tasks queues until there are no tasks left.
        (there may be tasks left at the end if tasks from one queue are creating tasks into another queue)
        
        @param assertNbTasks: You can specify a number of tasks that should be executed.
        @param maxEta: Timestamp in seconds. Only tasks scheduled for before this timestamp will get executed.
        @return: list of URLs of tasks we executed
        """
        eta = (maxEta * 1e6) if maxEta else None
        results = self._executeTaskQueue(eta=eta)
        if results:
            # In case some tasks enqueue tasks in another queue, run it again
            results.extend(self._executeTaskQueue(eta=eta))
        
        executedTasks = [r[0] for r in results]
        if assertNbTasks is not None:
            msg = "We executed %i tasks while we expected to execute %s tasks" % (len(executedTasks), assertNbTasks)
            msg += "\n Tasks found=%s" % executedTasks
            self.assertEqual(assertNbTasks, len(executedTasks), msg)
        
        return executedTasks
    
    def executeTask(self, taskUrl, status=200):
        """
        Runs the first enqueued task found with the specified URL.
        
        @return: response code
        """
        results = self._executeTaskQueue(taskUrl=taskUrl)
        self.assertNotEqual(0, len(results), "Failed to find %s in task queues" % taskUrl)
        self.assertEqual(status, results[0][1].status_int)
        
    def _executeTaskQueue(self, taskUrl=None, eta=None):
        # Implemented thanks to:
        # http://stackoverflow.com/questions/6632809/gae-unit-testing-taskqueue-with-testbed
        queueService = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)
        queuesInfo = queueService.GetQueues()
        
        def _getNextTask(queueName):
            tasks = queueService.GetTasks(queueName)
            if taskUrl:
                tasks = [task for task in tasks if task['url'] == taskUrl]
            return min(tasks, key=lambda task: task['eta_usec']) if tasks else None
        
        executedTasks = []
        for info in queuesInfo:
            if info.get('mode') == 'pull':
                continue
            queueName = info['name']
            
            task = _getNextTask(queueName)
            while task:
                if eta and task['eta_usec'] > eta:
                    break
                params = base64.b64decode(task["body"])
                headers = {
                    'X-AppEngine-Taskname': task['name'],
                    'X-AppEngine-queuename': queueName
                }
                
                # Special handling for the deferred module which uses it's own WSGI application
                app = deferred.application if task['url'] == '/_ah/queue/deferred' else None
                
                response = self.callOurUrl(task["url"], postData=params, headers=headers, executeTaskQueue=False, app=app)
                
                executedTasks.append((task['url'], response))
                queueService.DeleteTask(queueName, task['name'])
                
                if taskUrl:
                    # Only want to process the first task
                    return executedTasks
                
                task = _getNextTask(queueName)
            
        return executedTasks
        
    def setUrlfetchHandler(self, urlMatch, callback):
        """ Shortcut to URLFetchServiceHook.setCallback. """
        if not getattr(self, '_urlFetchHook', None):
            self._urlFetchHook = URLFetchServiceHook()
            self.testbed._register_stub(testbed.URLFETCH_SERVICE_NAME, self._urlFetchHook)
        self._urlFetchHook.setCallback(urlMatch, callback)
    
    
#===============================================================================
# Services testing utilities
#===============================================================================

class URLFetchServiceHook(urlfetch_stub.URLFetchServiceStub):
    """ URLFetch service for tests """
    
    # If we should block any URLFetch calls that are not explicitly handled via 'setCallback'.
    # (most of the time we don't want tests to rely on external services to run tests)
    interceptAllCalls = True
    
    def __init__(self, service_name=testbed.URLFETCH_SERVICE_NAME):
        self.__callbacks = {}
        # Match any URL
        # If no match, don't call normal service.
        # we don't want tests to rely on external services to run tests.
        urlmatchers = [(lambda url: True, self._processRequest)]
        
        urlfetch_stub.URLFetchServiceStub.__init__(self, service_name=service_name, urlmatchers_to_fetch_functions=urlmatchers)
    
    def setCallback(self, urlMatch, callback):
        """
        Each time a URLFetch call is made to a URL containing "urlMatch", the callback is called.
        
        @param callback: function(url, payload, method, headers) -> (status:int, body:str)
                        The callback can optionally return a third item in the tuple: headers:dict
                        
                        If the callback doesn't return such tuple the call is made to the real service.
        """
        self.__callbacks[urlMatch] = callback
        
    def _processRequest(self, url, payload, method, headers, request, response, **kwds):
        
        proceedWithRealCall = not self.interceptAllCalls
        
        # Sort by match length so that the most precise get called
        patterns = sorted(self.__callbacks, key=len, reverse=True)
        for urlMatch in patterns:
            if urlMatch in url:
                cbResponse = self.__callbacks[urlMatch](url, payload, method, headers)
                if isinstance(cbResponse, tuple):
                    self._setResponseData(response, *cbResponse)
                    proceedWithRealCall = False
                else:
                    proceedWithRealCall = True
                break
        
        if proceedWithRealCall:
            urlfetch_stub.URLFetchServiceStub._RetrieveURL(url, payload, method, headers, request, response, **kwds)
        else:
            logging.error("AppEngineTest will not call: %s (%s %s)", url, method, payload)
            self._setResponseData(response, 404, '')
    
    def _setResponseData(self, response, status, content, headers=None):
        response.set_statuscode(status)
        response.set_content(content)
        if headers:
            for key, value in headers.iteritems():
                header_proto = response.add_header()
                header_proto.set_key(key)
                header_proto.set_value(value)
            
#===============================================================================
# NDB testing utilities
#===============================================================================


@contextlib.contextmanager
def ndbToplevel():
    """
    Act like ndb.toplevel decorator but in the form of a context.
    Useful when testing code that uses the NDB fire-and-forget feature.
    
    Does not work if you nest several "with ndbToplevel" statements.
    
    Usage:
    
    with ndbToplevel():
        user = User.get_by_id(12)
        ...
        
    """
    # Code inspired from ndb.toplevel
    ndb.tasklets._state.clear_all_pending()
    ctx = ndb.tasklets.make_default_context()
    ndb.tasklets.set_context(ctx)
    try:
        yield ctx
    finally:
        ndb.tasklets.set_context(None)
        ctx.flush().check_success()
        ndb.tasklets.eventloop.run()
