var assert = require('assert');
var util = require('util');
var path = require('path');
var fs = require('fs');

function CallEnsurance(){

    var called = {};

    function ensureCall(name, f){
	var parentThis = this;

	called[name] = false;

	return function(){
	    called[name] = true;

	    f.apply(parentThis, arguments);
	};
    }

    function getMissingCall(){
	var name;

	for(name in called){
	    if(called[name] === true){
		continue;
	    }

	    return name;
	}

	return null;
    }

    function validate(){
	var name = getMissingCall();

	if(name === null){
	    return;
	}

	assert.fail('Did not call ' + name);
    }

    function waitForCalls(maxWait){
	var delay = 100;
	var start = new Date();

	function check(){
	    var now;
	    var name = getMissingCall();

	    if(name === null){
		return;
	    }

	    now = new Date();

	    if(now.getTime() - start.getTime() > maxWait){
		validate();

		return;
	    }

	    delay = delay * 2;
	    util.log('Waiting for calls... ' + String(delay / 1000) + 'sec');

	    scheduleCheck();
	}

	function scheduleCheck(){
	    setTimeout(check, delay);
	}

	scheduleCheck();
    }
    
    return {
	
	ensureCall: ensureCall,

	    validate: validate,

	    waitForCalls: waitForCalls

    };
}

function getDB(){
    return require('./../lib/node_cpdb.js');
}

function getEmptyDB(name, success){
    var index = (new Date()).getTime();

    function check(){
	var dbPath = path.join('target', 'db_' + name + '_' + String(index));
	path.exists(dbPath, function(exists){
		if(exists === false){
		    fs.mkdirSync(dbPath, 0755);

		    var libDB = getDB();

		    success(libDB.DB(dbPath));

		    return;
		}
	    });
    }

    check();
}

(function(){
    var test = 'testGetReturnsFormerlySettedValue';
    console.log('Running ' + test);
    getEmptyDB(test, function(db){
	    var ens = CallEnsurance();
	    var key = '___myWeirdName:/\\?&=___';

	    (function(){
		var t = db.newTransaction();
	
		t.set(key, { value: 'hello world!' });

		t.commit();
	    })();

	    (function(){
		var t = db.newTransaction(true);

		t.get(key, ens.ensureCall(test + ' getter', function(entry){
			    assert.equal('hello world!', entry.value);
			}),
		    assert.fail);
	    })();

	    ens.waitForCalls(10 * 1000);
	});
})();

// TODO unit test which validates transactional behavior
