# duty
Ridiculously simple Javascript job queue, database-agnostic

### Introduction
Simply managing offline jobs shouldn't involve dedicated databases, standalone servers or complex configuration. It should just flow naturally with the rest of the code. Duty is a dumb-simple job queueu implementation that runs in native Javascript and by default, doesn't require any external database. 

> There are other, more elaborate job queue systems for Node js. Kue is a very recommended library, backed by Redis. However, Node lacks a simple job queue system that doesn't require any additional infrastructure to test and play around with, even on local servers. 

### Usage

```javascript
var duty = require( "duty" );

// add a job to the queue
duty( "test-job", { hello: "world" } )
    .on( "error", function ( err ) {})
    .on( "done", function ( result ) {});

// Meanwhile... elsewhere in the code
duty.register( "test-job", function ( data, done ) {
    // do your magic
    done( null, { ok: 1 } ); // 
});
```

### Jobs Persistence

Duty uses the `dbstream` standard for providing a portable persistency model. By default, duty ships with the `dbstream-memory` library, which saves all of the jobs data to local memory. This can be easily reconfigured to use other databases:

```javascript
var db = require("dbstream-memory");
var conn = db.connect( "mongodb://127.0.0.1:27017/test", { collection: "jobs" } );
duty.db( conn ); // use mongodb instead of memory
```

### Registering Listeners

`duty.register( name, fn, options )`

* **name** is a string for the job name
* **fn** is the listener function to handle jobs:
    - **data** the data object that was inserted to the queue
    - **done** a callback function that needs to be called to indicate that the processing is done, either successfully or with an error
        + **err** the error string or object
        + **result** the job result object
    - **this** runs with the context of the job itself, which is an EventEmitter
* **options**
    - **delay** [60000] number of milliseconds to wait after the queue is empty before attempting to poll more jobs
    - **ttl** [Infinity] number of milliseconds to allow for an inactivity timeout. If the job didn't finish within that time, or no `progress` events were emitted, the job will timeout with an error. This can happen when the code fails to call the `done` callback.
    - **concurrency** [1] number of listeners that can be invoked in parallel



