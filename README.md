## AppEngine Counters ##

A Counter class to help you track events happening in your application in a simple efficient and scalable way.

There are already several implementations available out there, here is one that met our constraints:

- We need it to be as cost efficient as possible
- We do not need an exact count and can afford losing < 1% of the count value
- We increment a lot but seldom read the value (reading being mainly for admin dashboards)
- We have been using this Counter for years and it has proved to be robust in challenging environments:
several hundred of increments per second for a single counter, shared memcache where keys would only stay a few seconds if not used.

### Features ###
On top of the basic ability to increment/decrement counters:

- History of daily (or custom range) evolution of a counter value
- Basic dashboard to query for counters by prefix
- Basic dashboard to plot counters value over time 

### Further documentation and examples ###

See the [Counter class](https://github.com/freshplanet/AppEngine-Counter/blob/master/counter/models.py) documentation and the [views](https://github.com/freshplanet/AppEngine-Counter/blob/master/counter/views.py) module.

### Dependencies ###

The commited implementation only makes use of libraries available by default on Google App Engine.
However we recommend making use of our [improved deferred library](https://github.com/freshplanet/AppEngine-Deferred) to improve performances (replace models._addTask by tasks.addTask).

--

**GAME DEVELOPMENT in NYC at FreshPlanet/SongPop**

Join us at our new offices across from the Empire State Building on 34th and 5th Avenue.
FreshPlanet is a NYC based mobile game development firm and we are looking for senior Python engineers to join our back-end team.

Work on back-end services that support mobile games entertaining millions of players around the world.

Please contact Tom Cassidy (tcassidy@freshplanet.com) or apply at http://freshplanet.com/jobs/
