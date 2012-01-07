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

		t.commit();
	    })();

	    (function(){
		var t = db.newTransaction(true);

		t.get(key, testSuccess, assert.fail);
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

	    (function(){
		var t = db.newTransaction();

		t.set(key, value);

		t.commit();
	    }());

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

	    assertValue(function(){
		    rollbackTest(function(){
			    assertValue(testSuccess);
			});
		});

	    ens.waitForCalls(10 * 1000);
	});
}());
