var assert = require( "assert" );
var duty = require( "./duty" );

describe( "Duty", function () {

    it( "returns the added job id and pending status", function () {
        var job = duty( "test", { hello: "world" } )
        assert( job.id );
        assert.equal( job.status, "pending" );
    });

    it( "fires an 'add' event when the job is added", function ( done ) {
        var before = new Date();
        var job = duty( "test", {} )
        job.on( "add", function () {
            var added_on = new Date( this.added_on );
            assert( added_on >= before );
            assert( added_on <= after );
            done();
        })
        var after = new Date();
    });

    it( "fires an 'error' event when the job wasn't added", function ( done ) {
        var db = duty.db();
        var save = db.Cursor.prototype._save;
        db.Cursor.prototype._save = function ( data, cb ) {
            cb( new Error( "Something went wrong" ) );
        }
        
        duty( "test", { hello: "world" } )
            .on( "error", function ( err ) {
                db.Cursor.prototype._save = save;
                assert.equal( err.message, "Something went wrong" )
                done();
            });
    })

    it( "queues jobs until a listener is registered", function ( done ) {
        var count = 0, all = [
            { hello: "world" },
            { foo: "bar" },
            { alice: "bob" }
        ];
        duty( "test", all[ 0 ] );
        duty( "test", all[ 1 ] );
        duty( "test", all[ 2 ] );
        duty.register( "test", function ( data, cb ) {
            assert.deepEqual( data, all[ count++ ] );
            cb();
            if ( count == 3 ) done();
        })
    });

    it( "pushes new jobs to existing listeners", function ( done ) {
        var count = 0, all = [
            { hello: "world" },
            { foo: "bar" },
            { alice: "bob" }
        ];
        duty.register( "test", function ( data, cb ) {
            assert.deepEqual( data, all[ count++ ] );
            cb();
            if ( count == 3 ) done();
        }, { delay: 10 } );
        setTimeout( function () {
            duty( "test", all[ 0 ] );
            duty( "test", all[ 1 ] );
            duty( "test", all[ 2 ] );
        }, 10 );
    });

    it( "unregister listeners", function ( done ) {
        duty.register( "test", function ( data, cb ) {
            done( new Error( "Unregistered listener invoked" ) )
        }, { delay: 10 } );
        duty.unregister( "test" );
        duty( "test", {} );
        setTimeout( done, 30 );
    })

    it( "overrides registered listeners", function ( done ) {
        duty.register( "test", function ( data, cb ) {
            done( new Error( "Unregistered listener invoked" ) )
        }, { delay: 10 } );
        duty.register( "test", function ( data, cb ) {
            done();
        }, { delay: 10 } );
        duty( "test", {} );
    })

    // it( "get returns the job object", function ( done ) {
    //     duty( "test", { hello: "world" } )
    //         .on( "add", function () {
    //             duty.get( this.id, function ( job ) {
    //                 console.log( job );
    //             })
    //         })
    // })

    // remove all jobs before and after each test
    beforeEach( dropdb );
    afterEach( dropdb );
});

function dropdb( done ) {
    var jobs = [];
    var Cursor = duty.db().Cursor;
    new Cursor()
        .find({})
        .on( "error", done )
        .on( "finish", done )
        .on( "data", jobs.push.bind( jobs ) )
        .on( "end", function () {
            jobs.forEach( this.remove.bind( this ) );
            this.end();
        })
}