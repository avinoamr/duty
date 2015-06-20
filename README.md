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
    done( null, { ok: 1 } );
});
```

### Jobs Persistence

Duty uses the `dbstream` standard for providing a portable persistency model. By default, duty ships with the `dbstream-memory` library, which saves all of the jobs data to local memory. This can be easily reconfigured to use other databases:

```javascript
var db = require("dbstream-mongo");
var conn = db.connect( "mongodb://127.0.0.1:27017/test", { collection: "jobs" } );
duty.db( conn ); // use mongodb instead of memory
```

### Registering Listeners

`duty.register( name, fn, options )`

* **name** is a string for the queue name
* **fn** is the listener function to handle jobs
* **options** is an optional object of listener configuration options

This is a fully-blown example of a job that reads a file and counts the number of lines in it:

```javascript
duty.register( "count-lines", function ( data, done ) {
    var newlines = 0, total = null, loaded = 0;

    // read the file and count the number of newlines
    var readable = fs.createReadStream( data.filename )
        .once( "error", done )
        .on( "data", function ( data ) {
            loaded += data.length;
            newlines += data
                .toString()
                .split( "\n" )
                .length - 1;

            // optionally, emit progress updates to allow external code to
            // keep track of the internal job progress  
            this.emit( "progress", loaded, total )
        }.bind( this ) )
        .once( "end", function () {
            done( null, { newlines: newlines } );
        });

    // read the total file size, used for the progress tracking
    fs.stat( data.filename, function ( err, stats ) {
        if ( err ) return done( err );
        total = stats.size;
    })

    // an external error (or job cancelation) has been triggered
    // this is important if you have a long running job, and you want to allow
    // external code to call duty.cancel( job ) to terminate it.
    this.on( "error", function () {
        readable.destroy();
    })
})

// add tasks to this queue
duty( "count-lines", { filename: "somefile.txt" } );
```

##### Options

* **delay** [60000] number of milliseconds to wait after the queue has been emptied before trying to read more jobs from the database
* **timeout** [Infinity] number of milliseconds to allow for inactivity timeout, which is the time from the start of the job processing, until any update occurs (completion or progress). It's recommended in order to prevent cases where the `done` method doesn't get called, and the jobs remains a zombie forever.
* **concurrency** [1] number of parallel processes allowed

