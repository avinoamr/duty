var assert = require( "assert" );
var duty = require( "./duty" );

describe( "Duty", function () {

    it( "returns the added job id and pending status", function ( done ) {
        var job = duty( "test", { hello: "world" }, function ( err, job ) {
            assert( job.id );
            assert.equal( job.status, "pending" );
            done( err );
        })
    });

    it( "returns the job is added", function ( done ) {
        var before = new Date();
        var job = duty( "test", {}, function ( err, job ) {
            var added_on = new Date( job.added_on );
            assert( added_on >= before );
            assert( added_on <= after );
            done();
        })
        var after = new Date();
    });

    it( "returns an error when job wasn't added", function ( done ) {
        var db = duty.db();
        var save = db.Cursor.prototype._save;
        db.Cursor.prototype._save = function ( data, cb ) {
            cb( new Error( "Something went wrong" ) );
        }
        
        duty( "test", { hello: "world" }, function ( err ) {
            db.Cursor.prototype._save = save;
            assert.equal( err.message, "Something went wrong" )
            done();
        })
    });

    it( "queues jobs until a listener is registered", function ( done ) {
        var count = 0, input = [], all = [
            { hello: "world" },
            { foo: "bar" },
            { alice: "bob" }
        ];
        duty( "test", all[ 0 ] );
        duty( "test", all[ 1 ] );
        duty( "test", all[ 2 ] );
        duty.register( "test", function ( data, cb ) {
            count += 1;
            input.push( data );
            cb();
        });
        setTimeout( function () {
            assert.deepEqual( input, all );
            assert.equal( count, 3 );
            done();
        }, 20 )
    });

    it( "pushes new jobs to existing listeners", function ( done ) {
        var count = 0, input = [], all = [
            { hello: "world" },
            { foo: "bar" },
            { alice: "bob" }
        ];
        duty.register( "test", function ( data, cb ) {
            input.push( data );
            count += 1;
            cb();
        }, { delay: 5 } );
        setTimeout( function () {
            duty( "test", all[ 0 ] );
            duty( "test", all[ 1 ] );
            duty( "test", all[ 2 ] );
        }, 5 );

        setTimeout( function () {
            assert.deepEqual( input, all );
            assert.equal( count, 3 );
            done();
        }, 20 )
    });

    it( "unregister listeners", function ( done ) {
        duty.register( "test", function ( data, cb ) {
            done( new Error( "Unregistered listener invoked" ) )
        }, { delay: 10 } );
        duty.unregister( "test" );
        duty( "test", {} );
        setTimeout( done, 30 );
    });

    it( "overrides registered listeners", function ( done ) {
        duty.register( "test", function ( data, cb ) {
            done( new Error( "Unregistered listener invoked" ) )
        }, { delay: 10 } );
        duty.register( "test", function ( data, cb ) {
            done();
        }, { delay: 10 } );
        duty( "test", {} );
    });

    it( "get returns the job object", function ( done ) {
        duty( "test", { hello: "world" }, function ( err, job ) {
            var id = job.id;
            duty.get( id, function ( err, job ) {
                assert.deepEqual( job.data, { hello: "world" } );
                assert.equal( job.id, id );
                done( err );
            })
        })
    });

    it( "stores job result", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            cb( null, { ok: 1 } );
        })
        setTimeout( function () {
            duty.get( job.id, function ( err, job ) {
                assert.deepEqual( job.result, { ok: 1 } );
                assert.equal( typeof job.error, "undefined" );
                assert.equal( job.status, "success" );
                assert( !isNaN( new Date( job.end_on ).getTime() ) )
                done( err );
            })
        }, 20 );
    });

    it( "stores job error", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            cb( "Something went wrong", { ok: 1 } );
        })
        setTimeout( function () {
            duty.get( job.id, function ( err, job ) {
                assert.deepEqual( job.error, "Something went wrong" );
                assert.equal( typeof job.result, "undefined" );
                assert.equal( job.status, "error" );
                assert( !isNaN( new Date( job.end_on ).getTime() ) )
                done( err );
            })
        }, 20 );
    });

    it( "doesn't modify completed jobs", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, done ) {
            this.emit( "error", "Something went wrong" );
            setTimeout( function () {
                done( null, "Successful" );
            }, 20 )
        });
        setTimeout( function () {
            duty.get( job, function ( err, job ) {
                assert.equal( job.status, "error" );
                assert.equal( job.error, "Something went wrong" );
                assert.equal( typeof job.result, "undefined" );
                done( err );
            })
        }, 40 )
    });

    it( "calling done multiple times doesn't modify the job", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, done ) {
            done( null, "Successful" );
            setTimeout( function () {
                done( "Something went wrong" );
            }, 20 );
        })

        setTimeout( function () {
            duty.get( job, function ( err, job ) {
                assert.equal( job.status, "success" );
                assert.equal( job.result, "Successful" );
                assert.equal( typeof job.error, "undefined" );
                done( err );
            })
        }, 40 )
    })

    it( "stores errors for syncly thrown exceptions", function ( done ) {
        var job = duty( "test", {} )
        duty.register( "test", function () {
            throw new Error( "Something went wrong" )
        });
        setTimeout( function () {
            duty.get( job.id, function ( err, job ) {
                assert.deepEqual( job.error, "Error: Something went wrong" );
                assert.equal( typeof job.result, "undefined" );
                assert.equal( job.status, "error" );
                assert( !isNaN( new Date( job.end_on ).getTime() ) )
                done( err );
            })
        }, 20 );
    });

    it( "updates the progress", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            this.emit( "progress", 10, 100 );
            setTimeout( cb, 10 );
        });

        setTimeout( function () {
            duty.get( job, function ( err, job ) {
                assert.equal( job.loaded, 10 );
                assert.equal( job.total, 100 );
                done( err )
            });
        }, 30 )
    });

    it( "cancels running jobs", function ( done ) {
        var job = duty( "test", {} );
        var count = 0, status;
        duty.register( "test", function ( data, cb ) {
            var interval = setInterval( function () {
                count += 1;
                this.emit( "progress" ); // force a job update
            }.bind( this ), 10 );

            // external error
            this.on( "error", function ( err ) {
                clearInterval( interval )
                assert.equal( err, "Canceled" );
                assert( count >= 1 && count <= 3, "1 <= " + count + " <= 3" );
                done();
            });
        });

        setTimeout( function () {
            duty.cancel( job, function ( err ) {
                if ( err ) done( err );
            });
        }, 20 );
    });

    it( "expires jobs after the inactivity timeout", function ( done ) {
        var everror;
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            this.on( "error", function ( err ) {
                everror = err;
            })
        }, { timeout: 20 } );

        setTimeout( function () {
            duty.expire( function () {
                duty.get( job, function ( err, job ) {
                    assert.equal( job.status, "error" );
                    assert.equal( job.error, "Expired due to inactivity" );
                    assert.equal( everror, "Expired due to inactivity" );
                    done( err );
                })
            });
            
        }, 30 )
    });

    it( "doesn't expire jobs when they complete on time", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            setTimeout( cb, 5 );
        }, { timeout: 20 } );

        setTimeout( function () {
            duty.get( job, function ( err, job ) {
                assert.equal( job.status, "success" );
                done( err );
            })
        }, 30 )
    });

    it( "runs listeners serially", function ( done ) {
        duty( "test", {});
        duty( "test", {});
        duty( "test", {});

        var concurrent = 0, results = [];
        duty.register( "test", function ( data, cb ) {
            concurrent += 1;
            results.push( concurrent )
            setTimeout( function () {
                concurrent -= 1;
                cb();
            }, 20 )
        }, { concurrency: 1 } );

        setTimeout( function () {
            assert.equal( results[ 0 ], 1 );
            assert.equal( results[ 1 ], 1 );
            assert.equal( results[ 2 ], 1 );
            done();
        }, 100 )
    });

    it( "runs listeners concurrently", function ( done ) {
        duty( "test", {});
        duty( "test", {});
        duty( "test", {});

        var concurrent = 0, results = [];
        duty.register( "test", function ( data, cb ) {
            concurrent += 1;
            results.push( concurrent )
            setTimeout( function () {
                concurrent -= 1;
                cb();
            }, 30 )
        }, { concurrency: 10 } );

        setTimeout( function () {
            assert.equal( results[ 0 ], 1 );
            assert.equal( results[ 1 ], 2 );
            assert.equal( results[ 2 ], 3 );
            done();
        },  100 )
    });

    it( "prevents duplicate processing of the same job", function ( done ) {
        var input = [];
        var job = duty( "test", {} );
        
        // start two listeners, while the second one will override the first,
        // both will still have access to the same job because the first 
        // listener will attempt to read at least one job before it's overridden
        // but will not have enough time to change its status
        var fn = function ( data, cb ) {
            input.push( data );
            cb();
        }

        duty.register( "test", fn, { concurrency: 10 } );
        duty.register( "test", fn, { concurrency: 10 } );

        setTimeout( function () {
            assert.equal( input.length, 1 );
            done();
        }, 100 )
    });

    it( "validates the concurrency", function () {
        assert.throws( function () {
            duty.register( "test", function () {}, { concurrency: Infinity } );
        });

        assert.throws( function () {
            duty.register( "test", function () {}, { concurrency: 0 } );
        });

        assert.throws( function () {
            duty.register( "test", function () {}, { concurrency: -10 } );
        });
    });

    it( "prevents duplicate running of the same data", function ( done ) {
        var job;
        duty.register( "test", function ( data, cb ) {
            job = this;
            setTimeout( cb, 100 );
        }, { delay: 20 })
        duty( "test", { id: 15, hello: "world" } );

        setTimeout( function () {
            duty( "test", { id: 15, hello: "world" }, function ( err ) {
                assert( err instanceof Error, err + " instanceof Error" );
                assert.equal( err.code, "duplicate" );
                assert.equal( err.jobid, job.id );
                assert.equal( err.dataid, 15 );
                assert.equal( err.status, "running" );
                done();
            });
        }, 10 )
    })

    it( "prevents duplicate pending of the same data", function ( done ) {
        var job = duty( "test", { id: 15, hello: "world" } );

        setTimeout( function () {
            duty( "test", { id: 15, hello: "world" }, function ( err ) {
                assert( err instanceof Error, err + " instanceof Error" );
                assert.equal( err.code, "duplicate" );
                assert.equal( err.jobid, job.id );
                assert.equal( err.dataid, 15 );
                assert.equal( err.status, "pending" );
                done();
            });
        }, 10 )
    })

    // remove all jobs before and after each test
    beforeEach( reset );
    afterEach( reset );
});

function reset( done ) {
    duty.unregister();
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