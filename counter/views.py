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
import datetime
import random

from google.appengine.ext import ndb
import webapp2

from counter.models import Counter


class SampleHandler(webapp2.RequestHandler):
    
    @ndb.toplevel
    def get(self):
        """
        Increments some Counters to play with the feature.
        """
        # Fill datastore with data to show case in admin view
        otherSliceId = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        for client in ['iOS', 'Android', 'Windows']:
            Counter.increment('newInstalls_' + client, random.randint(1, 5))
            Counter.increment('newInstalls_' + client, random.randint(1, 5), sliceId=otherSliceId)
            
        self.response.write("""
        Counters updated!
        Query for counters <a href="/admin/counters/?prefix=newInstalls">here</a>.
        """)
