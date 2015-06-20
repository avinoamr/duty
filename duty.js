var util = require( "util" );
var events = require( "events" );
var extend = require( "extend" );
var conn = require( "dbstream-memory" ).connect();

module.exports = duty;
duty.register = register;
duty.unregister = unregister;
duty.get = get;
duty.cancel = cancel;
duty.db = db;

function duty ( name, id, data ) {
    if ( arguments.length == 2 ) {
        data = id;
        id = Math.random().toString( 36 ).substr( 2 );
    }

    var job = new events.EventEmitter();
    job.name = name;
    job.id = id;
    job.status = "pending";
    job.data = data;
    
    var added_on = new Date().toISOString();
    var cursor = new conn.Cursor()
        .on( "error", job.emit.bind( job, "error" ) )
        .on( "finish", function () {
            job.added_on = added_on;
            job.emit( "add" );
        });

    process.nextTick( function () {
        cursor.end({ 
            id: id, 
            name: name,
            status: "pending", 
            added_on: added_on,
            data: data
        });
    })
    return job;
}

// register a listener 
var listeners = {};
function register ( name, fn, options ) {
    listeners[ name ] = fn;
    options = extend({ 
        delay: 60000, // 1-minute?
        ttl: Infinity,
        concurrency: 1
    }, options );

    runloop( name, fn, options );
}

function unregister( name ) {
    if ( arguments.length == 0 ) {
        listeners = {};
    } else {
        delete listeners[ name ];
    }
}

function runloop ( name, fn, options ) {

    // listener was unregistered or overridden
    if ( listeners[ name ] != fn ) return; 

    // run the next job in the queue
    next( name, function ( err, job ) {

        // no job found, try again after `delay` seconds
        if ( !job ) return setTimeout( function () {
            runloop( name, fn, options )
        }, options.delay );

        job = extend( new events.EventEmitter(), job );

        job.on( "progress", function ( loaded, total ) {
            resetTimeout();
            update({ 
                id: job.id, 
                loaded: loaded, 
                total: total 
            }, function ( err, found ) {
                extend( job, found );
                if ( err || job.status == "error" ) {
                    job.emit( "error", err || job.error );
                }
            });
        });

        job.on( "error", function ( err ) {
            clearTimeout( timeout );
            update({
                id: job.id,
                status: "error",
                error: err instanceof Error ? err.toString() : err,
                end_on: new Date().toISOString()
            }, function ( err, found ) {
                extend( job, found )
            });
        })

        job.on( "success", function ( result ) {
            clearTimeout( timeout );
            update({
                id: job.id,
                status: "success",
                result: result,
                end_on: new Date().toISOString()
            }, function ( err, found ) {
                extend( job, found );
                if ( err || job.status == "error" ) {
                    job.emit( "error", err || job.error );
                }
            } );
        })

        // start running it
        resetTimeout();
        try {
            fn.call( job, job.data, function ( err, result ) {
                if ( err ) {
                    job.emit( "error", err );
                } else {
                    job.emit( "success", result );
                }
                runloop( name, fn, options );
            });
        } catch ( err ) {
            job.emit( "error", err );
            runloop( name, fn, options );
        }

        // inactivity timeout
        var timeout;
        function resetTimeout() {
            clearTimeout( timeout );
            if ( options.ttl < Infinity && job.status == "running" ) {
                timeout = setTimeout( function () {
                    job.emit( "error", "Expired due to inactivity" );
                }, options.ttl )
            }
        }
    })
}

// claim and return the next available job in the queue
function next( name, done ) {
    var job;
    return new conn.Cursor()
        .find({ name: name, status: "pending" })
        .limit( 1 )
        .once( "data", function ( data ) { job = data } )
        .once( "error", done )
        .once( "end", function () {
            if ( !job ) return done();

            // claim ownership of this job to prevent concurrently
            // running the same job by multiple processes
            claim( job, function ( err, job ) {
                if ( err ) return done( err );

                // job is already claimed by concurrent process,
                // continue to the next job
                if ( job == null ) return next( name, done );

                // claimed successfuly, return it
                done( null, job )
            })
        })
}

// attempts to claim ownership on a job by tagging it with a claim id
// and optimistically verifying that no other process has claimed it 
// concurrently. This is required in order for the library to remain database
// agnostic and not reliable on any underlying locking mechanism
function claim( job, done ) {
    var claim = Math.random().toString( 36 ).substr( 2 );
    update({ 
        id: job.id, 
        status: "running",
        start_on: new Date().toISOString(),
        claim: claim
    }, function ( err ) {
        if ( err ) return done( err );
        get( job.id, function ( err, job ) {
            if ( err ) return done( err );

            // job is already claimed by concurrent process
            done( null, job.claim == claim ? job : null )
        })
    })
}

function get( job, done ) {
    var id = job.id || job, found;
    new conn.Cursor()
        .find({ id: id })
        .limit( 1 )
        .once( "error", done )
        .once( "end", function() { done( null, found ) } )
        .once( "data", function ( job ) {
            found = job;
        })
}

function update( job, done ) {
    var found = {};
    var updated_on = new Date().toISOString();
    new conn.Cursor()
        .find({ id: job.id })
        .once( "error", done )
        .once( "finish", done.bind( null, null, found ) )
        .once( "data", extend.bind( null, found ) )
        .once( "end", function () {
            this.end( extend( found, job, { updated_on: updated_on } ) )
        })
}

function cancel( job, done ) {
    update({ 
        id: job.id || job,
        status: "error",
        error: "Canceled"
    }, done )
}

// get or set the dbstream connection
function db( conn_ ) {
    if ( typeof conn_ == "undefined" ) {
        return conn;
    }
    return conn = conn_;
}





