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
import logging
import random

from google.appengine.api import datastore_errors, taskqueue
from google.appengine.ext import ndb, deferred
from google.appengine.runtime import apiproxy_errors


class Counter(ndb.Model):
    """
    Generic model to count something.
    
    The current implementation is designed to scale up to 500 calls per second using the default configuration.
    
    
    On rare occasions we needed a bit more than that and introduced a memcache-sharding feature:
    You can go up to 5,000 increments per second by specifying up to 10 shards while calling increment().
    Specifying more than 10 shards is not recommended as we would then have latency issues
    while updating the Counter model concurrently.
    
    Notes:
    - You can go further than this limit simply by using different Counter names - managing your own shards
    - '500 calls per second' is not a hard limit but rather a recommendation.
      Counter will use 2 memcache compute units (MCUs) per increment on a single memcache key,
      and each GB of memcache is designed to handle up to 10,000MCU per second.
      You will want to avoid having a single key consuming too much of it.
      500 increments per second is already 10% of this limit.
      See https://developers.google.com/appengine/docs/adminconsole/memcache
    - It is not designed to be called multiple times during the same request (not efficient and issues with NDB auto-batching).
      If you need to increment several times the same counter, aggregate this yourself first, then use Counter.increment().
    """
    # Counter is always updated inside a transaction, we do not need memcache for the entity itself
    # (also as we update the counter far more often than we need to read it)
    _use_memcache = False
    
    # Number of seconds between each scheduled tasks to read counter from memcache.
    # This defines the precision we want for the counter: if memcache happens to be flushed,
    # we lose up to this time worth of updates.
    # With 60 seconds, while memcache is seldom flushed more than once a day,
    # you can expect a decent precision.
    _MEMCACHE_LIFE_TIME = 60
    
    # Memcache.incr() and decr() do not support negative values.
    # => start with intermediary initial value
    # the max and min supported cached values are then [-2**63, 2**63-1]
    _MEMCACHE_INITIAL_VALUE = 2 ** 63
    
    # If you heavily use Counters, a single Task Queue may not be enough as it is limited to 500 tasks per seconds.
    # Specify here the names of the task queues we can use.
    QUEUES = ['default']
    
    # This specifies if for Counters that increase slowly, we should directly write to datastore
    # instead of using memcache & task queue.
    # This is useful if your current memcache configuration is under pressure
    # and keys get quickly evicted if not accessed.
    # We automatically switch to relying on memcache once your counter get enough updates.
    AVOID_MEMCACHE_AT_LOW_RATES = True
    
    # We introduced this key name prefix in case we want to add new features to Counter,
    # like automatically managing sharding in the datastore,
    # in which case we will want our own specific key names.
    # However currently we don't use that and the prefix may seem unnecessary.
    _PREFIX = 'counter_'
    
    # Maximum length for a counter name. ndb limits key to 500, we prefix the name with "counter_".
    NAME_MAX_LENGTH = 492

    # Total value for the counter.
    # Not indexed because that's too expensive to index a value that is frequently updated.
    # Most of the time we need to query counters by their names and not by their values.
    # If you want to index some counters values, you may consider subclassing Counter
    # and override the '_updateFromMemcache' method,
    # allowing you to regularly index the new value wherever suits you.
    value = ndb.IntegerProperty(indexed=False, default=0)
    
    # Besides tracking a global count with the 'value' property,
    # Counter also automatically tracks daily updates.
    # This is written to this property, that holds a dict mapping the slice ID (Like 'YYYY-MM-DD') to the value (int).
    # You can define your own slices if you need other time ranges for instance,
    # using the 'sliceId' keyword of the increment() method.
    # WARNING: keep in mind that AppEngine can only store up to 1MB of data per entity.
    # If you get too many different slices you will break your counter that will fail to update.
    # Note: 10 years worth of daily data with 10M count each day is only about 80KB so we consider the default behavior safe.
    bySlice = ndb.JsonProperty()
    
    @property
    def name(self):
        """ This Counter public name """
        # => remove the internal _PREFIX
        return self.key.id()[len(self._PREFIX):]
    
    @classmethod
    @ndb.tasklet
    def getCurrent(cls, counterName, nbShards=1, currentSlice=None):
        """
        Retrieve the Counter instance and update it locally with current values taken from memcache.
        
        @param counterName: The name of your counter
        
        @param nbShards: If you are using shards when calling increment() or decrement(),
                        specify the same number here to get an accurate real-time count.
        @param currentSlice: If you are using a custom sliceId when calling increment() or decrement(),
                        specify the current slice ID to get an accurate real-time count.
                        
        @return: A Future that will always give you a Counter instance,
                even if the counterName maps no existing counter
                (in which case the instance 'value' would be 0 and bySlice empty).
        @rtype: ndb.Future
        """
        if isinstance(counterName, unicode):
            counterName = counterName.encode('utf8')
        internalName = cls._PREFIX + counterName
        
        if not currentSlice:
            currentSlice = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        
        counterFut = cls.get_by_id_async(internalName)
        
        # Retrieve pending memcache updates
        cacheFuts = []
        for i in xrange(1, nbShards + 1):
            cacheKey = cls._buildCounterCacheKey({'slice': currentSlice, 'shard': i, 'name': internalName})
            cacheFuts.append(cls._memcacheReadCounter(cacheKey))
        cached = yield cacheFuts
        cachedDelta = sum([(value or 0) for value in cached])
        
        # Retrieve current counter
        counter = yield counterFut
        if not counter:
            counter = cls(id=internalName, bySlice={})
        
        # update counter with memcached updates
        if cachedDelta:
            counter.bySlice[currentSlice] = counter.bySlice.get(currentSlice, 0) + cachedDelta
            counter.value += cachedDelta
        
        raise ndb.Return(counter)
    
    @classmethod
    @ndb.tasklet
    def getByPrefix(cls, prefix, endPrefix=None, limit=30):
        """
        Query for the counters matching the given prefix.
        We do not fetch real time updates from memcache.
        
        @param endPrefix: If set, we query counters with names between prefix and endPrefix (endPrefix excluded)
        @param limit: Maximum number of Counter entities to retrieve
        
        @return: A future that will give a dict mapping counter names to their instance.
        @rtype: ndb.Future
        """
        if not endPrefix:
            endPrefix = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        startKey = ndb.Key(cls, cls._PREFIX + prefix)
        endKey = ndb.Key(cls, cls._PREFIX + endPrefix)
        
        counters = yield cls.query(cls.key >= startKey, cls.key < endKey).fetch_async(limit)
        results = {counter.name: counter for counter in counters}
        raise ndb.Return(results)
    
    @classmethod
    def increment(cls, counterName, toAdd=1, deadline=3, nbShards=1, sliceId=None):
        """
        Asynchronously increments the specified counter.
        Most common uses:
         Counter.increment('foo')
         Counter.increment('foo', 3)
        
        @param counterName: The name you want for your counter,
                            that you will need to retrieve it using either getCurrent() or getByPrefix().
                            Must be shorter than Counter.NAME_MAX_LENGTH
        @param toAdd: An integer specifying by how much increasing the current count.
        @param nbShards: Number of memcache shard to use for this counter.
                        Use this parameter if you expect more than 500 operations per second on your counter.
                        You will have to use the same number to read it if you want the current exact value
                        (see Counter.getCurrent)
        @param sliceId: By default we track counters variations day by day.
                        You can change this rate, for instance to have it hourly or by week.
                        In this case, each time you call increment() or decrement(), specify the right sliceId.
                        Ex: "2012-12-15" (default), "2012_week09", ...
                        
                        Be careful: make sure you will not end with too many different slices (fine if less than 10,000)
                                    otherwise the Counter entity may become too large (holding more than 1MB of data)
        @param deadline: Deadline for underlying RPC calls.
        
        @return: A future that will give you the operation success (bool)
        @rtype: ndb.Future
        """
        return cls._update(counterName, toAdd, deadline, nbShards, sliceId=sliceId)
    
    @classmethod
    def decrement(cls, counterName, toRemove=1, deadline=3, nbShards=1, sliceId=None):
        """
        Asynchronously decrements the specified counter.
        The final total count can be negative.
        
        @param toRemove: An integer specifying by how much decreasing the current count.
        @see: increment()
        @rtype: ndb.Future
        """
        return cls._update(counterName, -toRemove, deadline, nbShards, sliceId=sliceId)
    
    @classmethod
    @ndb.tasklet
    def _update(cls, counterName, delta, deadline=3, nbShards=1, sliceId=None):
        """ Implements increment() and decrement() """
        if delta == 0:
            raise ndb.Return(True)
        
        if sliceId is None:
            sliceId = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        
        if isinstance(counterName, unicode):
            counterName = counterName.encode('utf8')
        internalName = cls._PREFIX + counterName
        
        # We use one different memcache key per counter, shard and slice
        # but they will all update the same Counter entity.
        # Each key has it's own scheduled task to update the Counter entity.
        shardId = 1 if nbShards <= 1 else random.randint(1, nbShards)
        cacheOpts = {"slice": sliceId, "shard": shardId, "name": internalName}
        
        cacheKey = cls._buildCounterCacheKey(cacheOpts)
        
        ctx = ndb.get_context()
        newValueFut = cls._memcacheOffset(cacheKey, delta, deadline)
        scheduledFut = ctx.memcache_get(cacheKey + '_scheduled', deadline=deadline)
        
        newValue = yield newValueFut
        if newValue is None:
            logging.error("Could not update counter %s value in memcache", counterName)
            raise ndb.Return(False)
        
        # If memcache was empty, update the underlying entity (unless we are sharding or don't want that feature)
        if cls.AVOID_MEMCACHE_AT_LOW_RATES and newValue == delta and nbShards <= 1:
            try:
                yield ndb.transaction_async(lambda: cls._updateEntity(internalName, delta, sliceId, deadline))
            except (apiproxy_errors.DeadlineExceededError, datastore_errors.Timeout, datastore_errors.TransactionFailedError) as e:
                # In case of Timeout the Counter is likely being updated and under heavy access
                # so do nothing as another request will schedule the write if it's not already scheduled.
                logging.warn("Could not persist %s in datastore. memcache updated. (%r)", internalName, e)
            except datastore_errors.BadRequestError as e:
                # In case of BadRequestError, this may be because we execute too many things in //,
                # and some transactions are taking too long (the tasklet taking more time to be completed as other operations are on-going).
                if 'transaction has expired' in e.message:
                    logging.warn("Could not persist %s in datastore. memcache updated.", internalName, exc_info=1)
                else:
                    raise
            else:
                # Subtract the value previously added to memcache
                yield cls._memcacheOffset(cacheKey, -delta, deadline)
        
        # If memcache is not empty this means another update is in progress.
        # => Keep the value in memcache and wait for a task to process it.
        else:
            
            # schedule the task if needed
            isScheduled = yield scheduledFut
            if not isScheduled:
                # Add an expiration time to plan for next time we should enqueue a task
                canSchedule = yield ctx.memcache_add(cacheKey + '_scheduled', True, time=cls._MEMCACHE_LIFE_TIME)
                
                # Do not schedule again if already added(=> memcache.add returns False)
                if canSchedule:
                    # If we use several shards, set countdown at +/- 20% around MEMCACHE_LIFE_TIME
                    # To avoid having all tasks trying to update the Counter entity at the same time.
                    countdown = cls._MEMCACHE_LIFE_TIME if nbShards <= 1 else cls._MEMCACHE_LIFE_TIME * (0.8 + 0.4 * random.random())
                    
                    enqueued = yield _addTask(cls.QUEUES, cls._updateFromMemcache, cacheKey, cacheOpts, _countdown=countdown)
                    if not enqueued:
                        yield ctx.memcache_delete(cacheKey + '_scheduled')
                else:
                    logging.debug("%s already scheduled", internalName)
                
            # Otherwise, nothing to do
        
        raise ndb.Return(True)
    
    @classmethod
    @ndb.tasklet
    def reset(cls, counterName):
        """
        Set the counter total value back to 0.
        This does nothing on the bySlice details.
        
        @rtype: ndb.Future
        """
        internalName = cls._PREFIX + counterName
        
        ctx = ndb.get_context()
        deleteFut = ctx.memcache_delete(internalName)
        counter = cls.get_by_id(internalName)
        if counter:
            counter.value = 0
            yield counter.put_async()
        yield deleteFut
    
    @classmethod
    @ndb.tasklet
    def _updateEntity(cls, internalName, delta, sliceId, deadline=3):
        """ Update the counter entity. Designed to be run inside a transaction. """
        # Split deadline: 1/3 for GET, 2/3 for PUT
        counter = yield cls.get_by_id_async(internalName, deadline=deadline / 3.0)
        if not counter:
            counter = cls(id=internalName, bySlice={})
        
        counter.value += delta
        counter.bySlice[sliceId] = counter.bySlice.get(sliceId, 0) + delta
        
        yield counter.put_async(deadline=2 * deadline / 3.0)
        raise ndb.Return(counter)
        
    @classmethod
    def _updateFromMemcache(cls, cacheKey, opts):
        """
        Reads the current memcache count and write it to the datastore entity.
        This gets called from the Task Queue.
        
        You can override this method in a subclass to implement a hook on updates.
            @classmethod
            def _updateFromMemcache(cls, cacheKey, opts):
                counter = super(MyCounter, cls)._updateFromMemcache(cacheKey, opts)
                if counter:
                    logging.info("My current value is now %i!", counter.value)
        
        @param opts: dict with keys "name", "slice". Can also have key "decr".
        
        @return: The updated Counter entity or None if we could not update it.
        """
        delta = cls._memcacheReadCounter(cacheKey).get_result()
        logging.info("Update from memcache %s = %s", cacheKey, delta)
        
        if delta:
            if cls._memcacheOffset(cacheKey, -delta).get_result() is not None:
                try:
                    return ndb.transaction(lambda: cls._updateEntity(opts['name'], delta, opts['slice'], deadline=6))
                except:
                    logging.error("Exception, we will retry the task")
                    # Rollback
                    cls._memcacheOffset(cacheKey, delta).get_result()
                    # Re-raise so that the task is tried again
                    raise
            else:
                logging.warn("Could not decrement %s", cacheKey)
        elif delta is None:
            logging.warn("%s not resolved in memcache", cacheKey)
        return None
    
    @classmethod
    def _buildCounterCacheKey(cls, opts):
        """ Out of a dict describing the cached value, build the memcache key to use """
        return 'Counter_' + ','.join(['%s=%r' % (k, opts[k]) for k in sorted(opts)])
    
    @classmethod
    @ndb.tasklet
    def _memcacheOffset(cls, cacheKey, delta, deadline=3):
        """
        Increment or decrement a value in memcache using the right 'memcache_incr' or 'memcache_decr' method.
        
        @return: Future that will give the new value
        """
        args = (cacheKey,)
        kwargs = {'delta': delta, 'initial_value': cls._MEMCACHE_INITIAL_VALUE, 'deadline': deadline}
        if delta > 0:
            memcacheFunc = ndb.get_context().memcache_incr
        else:
            memcacheFunc = ndb.get_context().memcache_decr
            kwargs['delta'] = - delta
            
        newValue = yield memcacheFunc(*args, **kwargs)
        if newValue is None:
            # In practice this happen rarely (average 1 failure for 2,000 calls on January 23rd 2014)
            # Just retry once
            newValue = yield memcacheFunc(*args, **kwargs)
            if newValue is None:
                raise ndb.Return(None)
        
        raise ndb.Return(newValue - cls._MEMCACHE_INITIAL_VALUE)
    
    @classmethod
    @ndb.tasklet
    def _memcacheReadCounter(cls, cacheKey):
        """ Reads a value stored via '_memcacheCrement' """
        value = yield ndb.get_context().memcache_get(cacheKey)
        if value is not None:
            value -= cls._MEMCACHE_INITIAL_VALUE
        raise ndb.Return(value)


@ndb.tasklet
def _addTask(queues, func, *args, **kwargs):
    """
    At FreshPlanet we use our 'tasks' module instead,
    which allows to have deferred task being a non-blocking call.
    
    (we should open source it soon)
    """
    try:
        # Spread tasks into several queues as each queue as a limited processing rate.
        kwargs['_queue'] = random.choice(queues)
        
        deferred.defer(func, *args, **kwargs)
        raise ndb.Return(True)
    except taskqueue.Error:
        logging.exception("Could not enqueue the task")
        raise ndb.Return(False)
