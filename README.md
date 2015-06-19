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
duty.register( "test-job", function ( data ) {
    // do your magic
    this.emit( "done", { ok: 1 } ); // 
});
```




