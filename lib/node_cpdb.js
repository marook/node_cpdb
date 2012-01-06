/*
 * Copyright 2012 Markus Pielmeier
 *
 * This file is part of cpdb.
 *
 * cpdb is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * cpdb is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with cpdb.  If not, see <http://www.gnu.org/licenses/>.
 */

var DEBUG = false;

var fs = require('fs');
var path = require('path');

function validateSuccessError(success, error){
    if(success === undefined){
	throw new Error('Missing success');
    }
    
    if(error === undefined){
	throw new Error('Missing error');
    }
}


function StorageFileNameFactory() {
    
    var SLASH_PATTERN = new RegExp('/', 'g');
    var UNDERSCORE_PATTERN = /_/g;

    function keyToFileName(key){
	var b64 = new Buffer(key).toString('base64');

	return b64.replace(SLASH_PATTERN, '_') + '.json';
    }

    function fileNameToKey(fileName){
	// TODO
	throw new Error('Not yet implemented');
    }

    return {

	keyToFileName: keyToFileName,

	    fileNameToKey: fileNameToKey

    };
}

function DB(rootDir){

    var storageFileNameFactory = StorageFileNameFactory();

    function getPath(key){
	return path.join(rootDir, storageFileNameFactory.keyToFileName(key));
    }

    function loadEntry(key, success, error){
	if(DEBUG === true){
	    console.log('Loading entry with key ' + key);
	}

	var entryPath = getPath(key);

	validateSuccessError(success, error);

	fs.readFile(entryPath, 'utf-8', function(err, data){
		var entry;

		if(err){
		    if(err.code === 'ENOENT'){
			// no db file found => starting with empty db

			success(undefined);
			
			return;
		    }
			    
		    error(err);

		    return;
		}

		try{
		    entry = JSON.parse(data);
		}
		catch(e){
		    error('Can\'t parse \'' + entryPath + '\' because of ' + e);

		    return;
		}

		success(entry);
	    });
    }

    function saveEntry(key, entry, success, error){
	if(entry === null || entry === undefined){
	    throw new Error('entry must have a value');
	}

	validateSuccessError(success, error);

	if(DEBUG === true){
	    console.log('Saving entry with key ' + key);
	}

	fs.open(getPath(key), 'w', 0640, function(e, id){
		if(e){
		    error(e);

		    return;
		}

		fs.write(id, JSON.stringify(entry), null, 'utf-8', function(){
			fs.close(id);

			success();
		    });
	    });
    }

    function deleteEntry(key){
	if(DEBUG === true){
	    console.log('Deleting entry with key ' + key);
	}

	/*
	 * TODO deleting an entry should not break the transaction rules when no
	 * entry exists for the given key.
	 */
	fs.unlinkSync(getPath(key));
    }

    function getJoinedKey(keyPrefix, keySuffix){
	return keyPrefix + keySuffix;
    }

    function newTransaction(readOnly){
	if(readOnly === undefined){
	    readOnly = false;
	}

	var entries = {};

	var droppedKeys = {};

	function set(key, value){
	    delete droppedKeys[key];

	    entries[key] = value;
	}

	function get(key, success, error){
	    if(key in entries === true){
		success(entries[key]);

		return;
	    }
	    
	    // TODO prevent an entry from being loaded multiple times in parallel
	    loadEntry(key, function(entry){
		    if(entry !== undefined){
			entries[key] = entry;
		    }

		    success(entry);
		},
		error);
	}

	function drop(key, success, error){
	    if(key in entries === true){
		delete entries[key];
	    }

	    droppedKeys[key] = true;

	    success();
	}

	function createRandomKey(keyPrefix, success, error){
	    crypto.randomBytes(16, function(e, rBytes){
		    if(e){
			error(e);

			return;
		    }

		    
		    var keyUnique = new Buffer(rBytes).toString('base64');
		    var key = getJoinedKey(keyPrefix, keyUnique);

		    loadEntry(key, function(entry){
			    if(entry !== undefined){
				createUnique(keyPrefix, success, error);
			
				return;
			    }

			    success(key, keyPrefix, keyUnique);
			},
			error);
		});
	}

	function commit(){
	    var key;

	    for(key in entries){
		/*
		 * TODO make the commit operation synchronous by using
		 * renameSync in commit operation
		 */
		saveEntry(key, entries[key], function(){
			// success
			delete entries[key];

			// TODO delete all entries Object after commit
		    }, function(e){
			// error
			throw e;
		    });
	    }

	    for(key in droppedKeys){
		deleteEntry(key);
	    }

	    delete droppedKeys;
	}

	function rollback(){
	    // TODO
	}

	return {

	    db: db,

	    set: set,

		get: get,
		
		drop: drop,

		commit: commit,

		rollback: rollback,

		readOnly: readOnly,

		createRandomKey: createRandomKey

	};
    }

    var db = {

	getJoinedKey: getJoinedKey,

	newTransaction: newTransaction

    };

    return db;
}

exports.DB = DB;
