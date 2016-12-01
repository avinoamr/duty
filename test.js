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
            this.once( "error", done );
            count += 1;
            input.push( data );
            cb();

            if ( count === 3 ) {
                setTimeout( complete, 20 );
            }
        });

        function complete() {
            assert.deepEqual( input, all );
            assert.equal( count, 3 );
            done();
        }
    });

    it( "allows registering for a specific job", function ( done ) {
        var all = [
            { hello: "world", job: 0 },
            { foo: "bar", job: 1 },
            { alice: "bob", job: 2 }
        ];
        var job0 = duty( "test", all[ 0 ] );
        var job1 = duty( "test", all[ 1 ] );
        var job2 = duty( "test", all[ 2 ] );

        var foundJob;
        duty.register( "test", function ( data, cb ) {
            this.once( "error", done );
            foundJob = data.job;
            cb();
            setTimeout( complete, 20 );
        }, { id: job1.id } );

        function complete() {
            assert.equal( foundJob, job1.data.job );
            done();
        }
    });

    it( "pushes new jobs to existing listeners", function ( done ) {
        var count = 0, input = [], all = [
            { hello: "world" },
            { foo: "bar" },
            { alice: "bob" }
        ];
        duty.register( "test", function ( data, cb ) {
            this.once( "error", done );
            input.push( data );
            count += 1;
            cb();

            if ( count == 3 ) {
                setTimeout( complete, 20 );
            }
        }, { delay: 5 } );

        setTimeout( function () {
            duty( "test", all[ 0 ] );
            duty( "test", all[ 1 ] );
            duty( "test", all[ 2 ] );
        }, 5 );

        function complete() {
            assert.deepEqual( input, all );
            assert.equal( count, 3 );
            done();
        }
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
            this.once( "error", done );
            cb( null, { ok: 1 } );
            setTimeout( complete, 20 );
        })
        function complete() {
            duty.get( job.id, function ( err, job ) {
                assert.deepEqual( job.result, { ok: 1 } );
                assert.equal( typeof job.error, "undefined" );
                assert.equal( job.status, "success" );
                assert( !isNaN( new Date( job.end_on ).getTime() ) )
                done( err );
            })
        };
    });

    it( "stores job error", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            cb( "Something went wrong", { ok: 1 } );
            setTimeout( complete, 20 );
        })
        function complete() {
            duty.get( job.id, function ( err, job ) {
                assert.deepEqual( job.error, "Something went wrong" );
                assert.equal( typeof job.result, "undefined" );
                assert.equal( job.status, "error" );
                assert( !isNaN( new Date( job.end_on ).getTime() ) )
                done( err );
            })
        }
    });

    it( "stores job error as object", function ( done ) {
        var conn = duty.db();
        var options = { deepError: true };
        var error = new Error( "Something went wrong" );
        error.type = "test"
        
        duty.db( conn, options )
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            cb( error, { ok: 1 } );
            setTimeout( complete, 20 );
        })

        function complete() {
            duty.get( job.id, function ( err, job ) {
                assert( job.error )
                assert.equal( job.error.message, "Something went wrong" )
                assert.equal( job.error.type, "test" );
                assert( !job.error.stack )
                done( err )
            })
        }
    })

    it( "supports job retries", function ( done ) {
        var job = duty( "test", {} );
        var errorNext = true;

        duty.register( "test", function ( data, cb ) {
            if ( errorNext ) {
                cb( "Something went wrong" );
                errorNext = false;
            } else {
                cb( null, { ok: 1 } );
                setTimeout( complete, 20 )
            }

        }, { retries: 1, delay: 10 } )
        function complete() {
            duty.get( job.id, function ( err, job ) {
                assert.equal( job.lastError, "Something went wrong" )
                assert.equal( typeof job.error, "undefined" );
                assert.deepEqual( job.result, { ok: 1 } );
                assert.equal( job.status, "success" );
                assert( !isNaN( new Date( job.end_on ).getTime() ) )
                done( err );
            })
        }
    })

    it( "doesn't retry canceled jobs", function ( done ) {
        var job = duty( "test", {} );
        var count = 0, status, err;
        duty.register( "test", function ( data, cb ) {
            count += 1;

            this.once( "error", function ( _err ) {
                // external error
                clearInterval( interval )
                setTimeout( complete, 200 )
            });

            var interval = setInterval( function () {
                this.emit( "progress" ); // force a job update
            }.bind( this ), 10 );
        }, { retries: 1, delay: 10 } );

        setTimeout( function () {
            duty.cancel( job, function ( err ) {
                if ( err ) done( err );
            });
        }, 30 );

        function complete() {
            duty.get( job.id, function ( err, job ) {
                assert.equal( job.error, "Error: Canceled" );
                assert.equal( count, 1 );
                done( err );
            })
        }
    })

    it( "doesn't modify completed jobs", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            this.emit( "error", "Something went wrong" );
            setTimeout( function () {
                cb( null, "Successful" );
                setTimeout( complete, 20 );
            }, 20 )
        });

        function complete() {
            duty.get( job, function ( err, job ) {
                assert.equal( job.status, "error" );
                assert.equal( job.error, "Something went wrong" );
                assert.equal( typeof job.result, "undefined" );
                done( err );
            })
        }
    });

    it( "calling done multiple times doesn't modify the job", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            this.once( "error", function () {
                setTimeout( complete, 20 );
            });
            cb( null, "Successful" );
            setTimeout( function () {
                cb( "Something went wrong" );
            }, 20 );
        })

        function complete() {
            duty.get( job, function ( err, job ) {
                assert.equal( job.status, "success" );
                assert.equal( job.result, "Successful" );
                assert.equal( typeof job.error, "undefined" );
                done( err );
            })
        }
    })

    it( "stores errors for syncly thrown exceptions", function ( done ) {
        var job = duty( "test", {} )
        duty.register( "test", function () {
            this.once( "error", function () {
                setTimeout( complete, 20 );
            });
            throw new Error( "Something went wrong" )
        });

        function complete() {
            duty.get( job.id, function ( err, job ) {
                assert.deepEqual( job.error, "Error: Something went wrong" );
                assert.equal( typeof job.result, "undefined" );
                assert.equal( job.status, "error" );
                assert( !isNaN( new Date( job.end_on ).getTime() ) )
                done( err );
            })
        }
    });

    it( "all emitted errors get captured", function ( done ) {
        duty( "test", {} );
        duty.register( "test", function () {
            this.emit( "error", "Error 1" );
            setTimeout( function () {
                this.emit( "error", "Error 2" );
                setTimeout( done, 30 );
            }.bind( this ), 30 )
        })
    })

    it( "updates the progress", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            this.once( "error", done );
            this.emit( "progress", 10, 100 );
            setTimeout( function () {
                cb();
                setTimeout( complete, 20 );
            }, 10 );
        });

        function complete() {
            duty.get( job, function ( err, job ) {
                assert.equal( job.loaded, 10 );
                assert.equal( job.total, 100 );
                done( err )
            });
        }
    });

    it( "cancels running jobs", function ( done ) {
        var job = duty( "test", {} );
        var count = 0, status, err;
        duty.register( "test", function ( data, cb ) {
            this.once( "error", function ( _err ) {
                // external error
                clearInterval( interval )
                err = _err;
                setTimeout( complete, 20 )
            });

            count = 1;
            var interval = setInterval( function () {
                count += 1;
                this.emit( "progress" ); // force a job update
            }.bind( this ), 10 );
        });

        function complete () {
            assert.equal( err, "Error: Canceled" );
            assert( count >= 1 && count <= 3, "1 <= " + count + " <= 3" );
            done();
        }

        setTimeout( function () {
            duty.cancel( job, function ( err ) {
                if ( err ) done( err );
            });
        }, 30 );
    });

    it( "retries jobs after the inactivity timeout", function ( done ) {
        var everror;
        var job = duty( "test_expire", {} );
        duty.register( "test_expire", function ( data, cb ) {
            this.on( "error", function ( err ) {
                everror = err;
            })
            setTimeout( complete, 20 );
        }, { timeout: 20, retries: 1 } );

        function complete () {
            duty.expire( function() {
                process.nextTick( function() {
                    duty.get( job, function( err, job ) {
                        assert.equal( 'startAfter' in job, true );
                        assert.equal( job.retries > 0, true );

                        done( err );
                    });
                });
            });
        }
    });

    it( "doesn't expire jobs when they complete on time", function ( done ) {
        var job = duty( "test", {} );
        duty.register( "test", function ( data, cb ) {
            this.once( "error", done );
            setTimeout( cb, 5 );
            setTimeout( complete, 20 );
        }, { timeout: 20 } );

        function complete() {
            duty.get( job, function ( err, job ) {
                assert.equal( job.status, "success" );
                done( err );
            })
        }
    });

    it( "runs listeners serially", function ( done ) {
        duty( "test", {});
        duty( "test", {});
        duty( "test", {});

        var concurrent = 0, results = [], timeout;
        duty.register( "test", function ( data, cb ) {
            this.once( "error", done );
            concurrent += 1;
            results.push( concurrent )
            setTimeout( function () {
                clearTimeout( timeout );
                concurrent -= 1;
                cb();
                timeout = setTimeout( complete, 220 )
            }, 20 )

        }, { concurrency: 1 } );

        function complete() {
            assert.equal( results[ 0 ], 1 );
            assert.equal( results[ 1 ], 1 );
            assert.equal( results[ 2 ], 1 );
            done();
        }
    });

    it( "runs listeners concurrently", function ( done ) {
        duty( "test", {});
        duty( "test", {});
        duty( "test", {});

        var concurrent = 0, results = [], timeout;
        duty.register( "test", function ( data, cb ) {
            this.once( "error", done );
            concurrent += 1;
            results.push( concurrent )
            setTimeout( function () {
                clearTimeout( timeout );
                concurrent -= 1;
                cb();
                timeout = setTimeout( complete, 50 )
            }, 30 )
        }, { concurrency: 10 } );

        function complete() {
            assert.equal( results[ 0 ], 1 );
            assert.equal( results[ 1 ], 2 );
            assert.equal( results[ 2 ], 3 );
            done();
        }
    });

    it( "prevents duplicate processing of the same job", function ( done ) {
        // emulate a worst case scenario, where the update takes a long time,
        // and the reading is very fast (dbstream-memory reads in sync)
        var db = duty.db();
        var save = db.Cursor.prototype._save;
        db.Cursor.prototype._save = function () {
            if ( this.__saving ) return;
            this.__saving = true;
            var args = arguments, that = this;
            setTimeout( function () {
                that.__saving = false;
                save.apply( that, args )
            }, 50 )
        }


        var input = [];
        var job = duty( "test", {} );

        // start two listeners, while the second one will override the first,
        // both will still have access to the same job because the first
        // listener will attempt to read at least one job before it's overridden
        // but will not have enough time to change its status
        var timeout;
        var fn = function ( data, cb ) {
            this.once( "error", done );
            clearTimeout( timeout );
            input.push( data );
            cb();
            timeout = setTimeout( complete, 30 )
        }

        duty.register( "test", fn, { concurrency: 10 } );
        duty.register( "test", fn, { concurrency: 10 } );

        function complete() {
            db.Cursor.prototype._save = save;
            assert.equal( input.length, 1 );
            done();
        }
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
            this.once( "error", done );
            job = this;
            setTimeout( complete, 1 );
        }, { delay: 20 })
        duty( "test", { id: 15, hello: "world" } );

        function complete() {
            duty( "test", { id: 15, hello: "world" }, function ( err ) {
                assert( err instanceof Error, err + " instanceof Error" );
                assert.equal( err.code, "duplicate" );
                assert.equal( err.jobid, job.id );
                assert.equal( err.dataid, 15 );
                assert.equal( err.status, "running" );
                done();
            });
        }
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

// uncomment to test with dbstream-mongo
// var mongo = require( "dbstream-mongo" );
// var conn = mongo.connect( "mongodb://127.0.0.1/duty-test", { collection: "test" } );
// duty.db( conn );
function reset( done ) {
    duty.unregister();
    var conn = duty.db();
    // restore to the defautl db options
    duty.db( conn );
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
