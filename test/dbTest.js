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
