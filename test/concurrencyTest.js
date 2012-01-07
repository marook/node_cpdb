var dbTestUtils = require('./../test/dbTestUtils.js');
var assert = require('assert');

(function(){
    var test = 'testConcurrentOperations';
    console.log('Running ' + test);
    dbTestUtils.getEmptyDB(test, function(db){
	    var key = 'key';
	    var concurrentOperations = 100;

	    for(var i = 0; i < concurrentOperations; ++i){
		setTimeout(function(){
			var t = db.newTransaction();

			t.get(key, function(value){
				// success
				if(value === undefined){
				    value = 0;
				}
				else{
				    assert.equal('number', typeof(value));
				}

				value += 1;

				t.set(key, value);

				t.commit();
			    }, assert.fail);
		    }, Math.floor(50 * Math.random()));
	    }

	});
    
}());