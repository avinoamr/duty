var util = require( "util" );
var crypto = require( "crypto" );
var events = require( "events" );
var extend = require( "extend" );
var conn = require( "dbstream-memory" ).connect();

module.exports = duty;
duty.register = register;
duty.unregister = unregister;
duty.get = get;
duty.cancel = cancel;
duty.db = db;
duty.expire = expire;

function duty ( name, data, done ) {
    done || ( done = function () {} );
    var id = Math.random().toString( 36 ).substr( 2 );
    var job = {
        name: name,
        id: id,
        status: "pending",
        data: data,
        done: false,
    };
    
    var added_on = new Date().toISOString();
    var cursor = new conn.Cursor()
        .on( "error", done )
        .on( "finish", function () {
            job.added_on = added_on;
            done( null, job )
        });

    if ( data.id ) {
        var duplicate = false;
        cursor.on( "data", function ( job ) {
            var err = "Duplicate running job detected in Job #" + job.id;
            err = extend( new Error( err ), {
                code: "duplicate",
                dataid: data.id,
                jobid: job.id,
                status: job.status,
                description: "Cancel the running job first, before adding " +
                    "a new one with the same data.id"
            })
            
            this.removeAllListeners()
            done( err )
        })
        .on( "end", push )
        .find({ done: false, dataid: data.id })
    } else {
        push();
    }
    return job;

    function push() {
        cursor.end( extend( { added_on: added_on, dataid: data.id }, job ) );
    }
}

// register a listener 
var listeners = {};
function register ( name, fn, options ) {
    listeners[ name ] = fn;
    options = extend({ 
        delay: 60000, // 1-minute?
        timeout: Infinity,
        concurrency: 1
    }, options );

    if ( options.concurrency == Infinity || options.concurrency <= 0 ) {
        throw new Error( "Concurrency must be a finite positive number" );
    }

    for ( var i = 0 ; i < options.concurrency ; i += 1 ) {
        // space out the concurrent runloops to reduce the likelihood of 
        // claim conflicts
        setTimeout( function () {
            runloop( name, fn, options );
        }, 10 * i );
    }
}

function unregister( name ) {
    if ( arguments.length == 0 ) {
        listeners = {};
    } else {
        delete listeners[ name ];
    }
}

var running = {};
function runloop ( name, fn, options ) {

    // listener was unregistered or overridden
    if ( listeners[ name ] != fn ) return; 
    var timeout = ( options.timeout && options.timeout != Infinity )
        ? options.timeout : null;

    // run the next job in the queue
    next( name, function ( err, job ) {

        // no job found, try again after `delay` seconds
        if ( !job ) {
            return setTimeout( function () {
                runloop( name, fn, options )
            }, options.delay ).unref();
        }

        // create the job emitter and bind the event listeners used to control
        // the life-cycle of the job
        running[ job.id ] = job = extend( new events.EventEmitter(), job )
            .on( "progress", function ( loaded, total ) {
                if ( this.status != "running" ) return;
                var expires_on;
                if ( timeout ) {
                    expires_on = new Date( new Date().getTime() + timeout )
                        .toISOString();
                }

                update({ 
                    id: this.id, 
                    loaded: loaded, 
                    total: total,
                    expires_on: expires_on
                }, function ( err, found ) {
                    extend( this, found );
                    if ( err || this.status == "error" ) {
                        this.emit( "error", err || this.error );
                    }
                }.bind( this ) );
            })
            .once( "error", done )
            .once( "success", done.bind( job, undefined ) )

        // start running it
        job.emit( "progress", null, null )
        try {
            fn.call( job, job.data, done );
        } catch ( err ) {
            done( err );
        }

        function done ( err, result ) {
            if ( !running[ job.id ] ) return; // disregard multiple completions
            delete running[ job.id ];
            update({
                id: job.id,
                status: err ? "error" : "success",
                error: err instanceof Error ? err.toString() : ( err || undefined ),
                result: err ? undefined : result,
                done: true,
                end_on: new Date().toISOString()
            }, function ( err ) {
                if ( err ) job.emit( "error", err );
            })
            runloop( name, fn, options );
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

// find and remove jobs that were expired
function expire( done ) {
    var now = new Date();
    var expired = [];
    new conn.Cursor()
        .find({ status: "running" })
        .once( "error", done )
        .once( "finish", done )
        .on( "data", function ( job ) {
            if ( job.expires_on && new Date( job.expires_on ) < now ) {
                job.error = "Expired due to inactivity";
                job.updated_on = new Date().toISOString();
                job.status = "error";
                job.error = "Expired due to inactivity";
                this.write( job );
            }

            // if it's running - update it.
            if ( running[ job.id ] && job.status == "error" ) {
                running[ job.id ].emit( "error", job.error );
            }
        })
        .once( "end", function () {
            this.end();
        });
}

// clear expired jobs once a minute
setInterval( expire.bind( null, function ( err ) {
    console.error( "Duty Error: ", err.stack );
} ), 60000 ).unref(); // don't wait for it





