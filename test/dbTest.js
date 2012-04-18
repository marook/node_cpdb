var assert = require('assert');
var dbTestUtils = require('./../test/dbTestUtils.js');

(function(){
    var test = 'testGetReturnsFormerlySettedValue';
    console.log('Running ' + test);
    dbTestUtils.getEmptyDB(test, function(db){
	    var ens = dbTestUtils.CallEnsurance();
	    var key = '___myWeirdName:/\\?&=___';

	    var testSuccess = ens.ensureCall(test + ' getter', function(entry){
		    assert.equal('hello world!', entry.value);
		});

	    (function(){
		var t = db.newTransaction();
	
		t.set(key, { value: 'hello world!' });

		t.commit(function(){
			// success
			var t = db.newTransaction(true);

			t.get(key, testSuccess, assert.fail);
		    }, assert.fail);
	    })();

	    ens.waitForCalls(10 * 1000);
	});
}());

(function(){
    var test = 'testTransactionalCommitAndRollbackBehaviour';
    console.log('Running ' + test);
    dbTestUtils.getEmptyDB(test, function(db){
	    var ens = dbTestUtils.CallEnsurance();
	    var key = 'key';
	    var value = 'value';

	    var testSuccess = ens.ensureCall(test + ' testSuccess', function(){});

	    function rollbackTest(success){
		var t = db.newTransaction();

		t.set(key, 'something awful');

		t.rollback();

		success();
	    }

	    function assertValue(success){
		db.newTransaction(true).get(key, function(data){
			assert.equal(value, data);
			
			success();
		    }, assert.fail);
	    }

	    (function(){
		var t = db.newTransaction();

		t.set(key, value);

		t.commit(function(){
			// success
			assertValue(function(){
				rollbackTest(function(){
					assertValue(testSuccess);
				    });
			    });
		    }, assert.fail);
	    }());

	    ens.waitForCalls(10 * 1000);
	});
}());

(function(){
    var test = 'testDuplicateCommitFails';
    console.log('Running ' + test);
    dbTestUtils.getEmptyDB(test, function(db){
	    var ens = dbTestUtils.CallEnsurance();

	    var testSuccess = ens.ensureCall(test + ' testSuccess', function(){});

	    (function(){
		var t = db.newTransaction();

		t.set('key', 'value');

		t.commit(function(){
			// success
			t.commit(assert.fail, testSuccess);
		    }, assert.fail);
	    }());

	    ens.waitForCalls(10 * 1000);
	});
}());

(function(){
    var test = "testEmptyCommitIsSuccess";
    console.log('Running ' + test);
    dbTestUtils.getEmptyDB(test, function(db){
	    var ens = dbTestUtils.CallEnsurance();
	    var testSuccess = ens.ensureCall(test + ' testSuccess', function(){});

	    (function(){
		var t = db.newTransaction();

		t.commit(testSuccess, assert.fail);
	    }());

	    ens.waitForCalls(10 * 1000);
	});
}());

(function(){
    var test = "testCanNotRetriveDeletedEntries";
    console.log('Running ' + test);
    dbTestUtils.getEmptyDB(test, function(db){
	    var ens = dbTestUtils.CallEnsurance();
	    var key = 'key';

	    var testSuccess = ens.ensureCall(test + ' testSuccess', function(){});

	    function prepareTestData(success){
		var t = db.newTransaction();

		t.set(key, 'hello world');

		t.commit(success, assert.fail);
	    }

	    prepareTestData(function(){
		    var t = db.newTransaction();

		    t.drop(key, function(){
			    // success
			    t.get(key, function(data){
				    assert.equal(undefined, data);

				    testSuccess();
				}, assert.fail);
			}, assert.fail);
		});

	    ens.waitForCalls(10 * 1000);
	});
}());

(function(){
    var test = 'testCreateRandomKey';
    console.log('Running ' + test);
    dbTestUtils.getEmptyDB(test, function(db){
	    var ens = dbTestUtils.CallEnsurance();
	    var keyPrefix = 'key';

	    var testSuccess = ens.ensureCall(test + ' testSuccess', function(){});

	    var t = db.newTransaction();
	    t.createRandomKey(keyPrefix, function(key, pKeyPrefix, keyUnique){
		    assert.equal(keyPrefix, pKeyPrefix);
		    assert.notEqual(keyPrefix, key);

		    testSuccess();
		}, assert.fail);

	    ens.waitForCalls(10 * 1000);
	});
}());

(function(){
    /*
     * this test makes sure that very long key names can be stored in the file
     * system.
     *
     * this unit test makes sure that issue #1 [1] is fixed.
     *
     * [1] https://github.com/marook/node_cpdb/issues/1
     */
    var test = 'testStoreLongKey';
    console.log('Running ' + test);
    dbTestUtils.getEmptyDB(test, function(db){
	    var ens = dbTestUtils.CallEnsurance();
	    var keyPart = '___myWeirdName:/\\?&=___';
	    var key = '';

	    for(var i = 0; i < 500; ++i){
		key += keyPart;
	    }

	    var testSuccess = ens.ensureCall(test + ' getter', function(entry){
		});

	    (function(){
		var t = db.newTransaction();
	
		t.set(key, { value: 'hello world!' });

		t.commit(testSuccess, assert.fail);
	    })();

	    ens.waitForCalls(10 * 1000);
	});
}());
