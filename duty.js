var util = require( "util" );
var events = require( "events" );
var extend = require( "extend" );
var conn = require( "dbstream-memory" ).connect();

module.exports = duty;
duty.register = register;
duty.unregister = unregister;
duty.get = get;
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
    runloop( name, fn, extend({ delay: 60000 }, options ) );
}

function unregister( name ) {
    if ( name == "*" ) {
        listeners = {};
    }

    delete listeners[ name ];
}

function runloop ( name, fn, options ) {
    if ( listeners[ name ] != fn ) return;
    next( name, function ( err, job ) {

        // no job found, try again after `delay` seconds
        if ( !job ) return setTimeout( function () {
            runloop( name, fn, options )
        }, options.delay );


        fn( job.data, function ( err, result ) {
            register( name, fn, options );
            // job.status = err ? "error" : "success";
        });
    })
}

// claim and return the next available job in the queue
function next( name, done ) {
    var job = {};
    return new conn.Cursor()
        .find({ name: name, status: "pending" })
        .limit( 1 )
        .once( "data", extend.bind( null, job ) )
        .once( "error", done )
        .once( "end", function () {
            if ( !job.id ) return done();
            update( { id: job.id, status: "init" }, done )
        })
}

function get( job, done ) {
    var id = job.id || job, found;
    new conn.Cursor()
        .find({ id: id })
        .limit( 1 )
        .once( "error", done )
        .once( "end", done.bind( null, null, found ) )
        .once( "data", function ( job ) {
            found = job;
        })
}

function update( job, done ) {
    var found = {};
    new conn.Cursor()
        .find({ id: job.id })
        .once( "error", done )
        .once( "finish", done.bind( null, null, found ) )
        .once( "data", extend.bind( null, found ) )
        .once( "end", function () {
            this.end( extend( found, job ) )
        })
}

// attempts to claim ownership on a job by tagging it with a claim id
// and optimistically verifying that no other process has claimed it 
// concurrently. This is required in order for the library to remain database
// agnostic and not reliable on any underlying locking mechanism
function claim( job ) {

}

// get or set the dbstream connection
function db( conn_ ) {
    if ( typeof conn_ == "undefined" ) {
        return conn;
    }
    return conn = conn_;
}





