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

from google.appengine.ext import ndb
from google.appengine.runtime.apiproxy_errors import RequestTooLargeError

from counter.models import Counter
import testsUtils


class MyCounter(Counter):
    
    rate = ndb.IntegerProperty()
    
    @classmethod
    def _updateFromMemcache(cls, cacheKey, opts):
        mc = super(MyCounter, cls)._updateFromMemcache(cacheKey, opts)
        if mc:
            mc.rate = 10
            mc.put()


class CounterTest(testsUtils.AppEngineTest):
        
    def testSlices(self):
        """ Test using default and custom slices while counting """
        # Default slice
        today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        Counter.increment('testDefault', 1).get_result()
        counter = Counter.getCurrent('testDefault').get_result()
        self.assertEqual(counter.value, 1)
        self.assertDictEqual(counter.bySlice, {today: 1})
        
        # Custom slice
        futs = [Counter.increment('testCustom', 1, sliceId="13")]
        futs.append(Counter.increment('testCustom', 2, sliceId="12"))
        [fut.get_result() for fut in futs]
        
        counter = Counter.getCurrent('testCustom').get_result()
        self.assertEqual(counter.value, 3)
        self.assertDictEqual(counter.bySlice, {"12": 2, "13": 1})
        
    def testShards(self):
        """ Test that 'getCurrent' correctly retrieves values from different shards """
        futs = []
        for _ in xrange(10):
            # Because of NDB issue I can't perform all at the same time
            # https://code.google.com/p/googleappengine/issues/detail?id=10617
            futs.append(Counter.increment('testShards', nbShards=3).get_result())
        successes = futs  # getAllResults(futs)
        self.assertEqual(10, sum(successes))
        
        counter = Counter.getCurrent('testShards', nbShards=3).get_result()
        self.assertEqual(counter.value, 10)
        
    def testSubClass(self):
        """ Test overriding '_updateFromMemcache' works """
        MyCounter.increment("lalala").get_result()
        mc = MyCounter.getCurrent("lalala").get_result()
        self.assertEqual(mc.value, 1)
        
        MyCounter.increment("lalala", nbShards=2).get_result()
        
        tasks = self.executeTaskQueue()
        self.assertIn('/_ah/queue/deferred', tasks)
        
        mc = MyCounter.getCurrent("lalala", nbShards=2).get_result()
        self.assertEqual(mc.value, 2)
        self.assertEqual(mc.rate, 10)
        
    def testMaxNameLength(self):
        longCounterName = 't' * Counter.NAME_MAX_LENGTH
        
        incrementSuccess = Counter.increment(longCounterName, 1).get_result()
        self.assertTrue(incrementSuccess)
        
        counter = Counter.getCurrent(longCounterName).get_result()
        self.assertEqual(counter.name, longCounterName, "Name integrity was not maintained")

    def testSize(self):
        """ Test if we can stack enough slices with our default 'daily tracking' behavior """
        counter = Counter()
        today = datetime.datetime.utcnow()
        counter.bySlice = {}
        for i in xrange(10 * 365):
            today += datetime.timedelta(days=1)
            counter.bySlice[str(today.date())] = 10000000 + i
        
        try:
            counter.put()
        except RequestTooLargeError:
            self.fail("Counter bySlice can't hold enough data!")
