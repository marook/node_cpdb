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

function DB(rootDir, dbError){

    var storageFileNameFactory = StorageFileNameFactory();

    var storagePath = null;

    function mkdirs(path, mode, success, error){
	fs.mkdir(path, 0755, function(e){
		if(e && e.code !== 'EEXIST'){
		    error(e);

		    return;
		}

		success(path);
	    });
    }

    var getNextTransactionDir = function(){

	var nextTransactionIndex = 1;

	function getTransactionDir(success, error){
	    var transactionDir = path.join(rootDir, 'transactions');

	    mkdirs(transactionDir, 0755, success, error);
	}

	function getNextTransactionDir(success, error){
	    function a(transactionDir){
		var thisTransactionDir = path.join(transactionDir, nextTransactionIndex.toString(16));
		++nextTransactionIndex;

		fs.mkdir(thisTransactionDir, 0755, function(e){
			if(e){
			    if(e.code === 'EEXIST'){
				// try next transaction index

				/*
				 * we increase the transaction index to step
				 * faster over possibly old transactions from
				 * former program run.
				 */
				nextTransactionIndex += 100;

				a(transactionDir);
			    }
			    else{
				error(e);
			    }

			    return;
			}
			
			success(thisTransactionDir);
		    });
	    }

	    getTransactionDir(a, error);
	}

	return getNextTransactionDir;

    }();

    function deleteTransactionDir(transactionDir){
	try{
	    // TODO recursively delete transaction directories
	}
	catch(e){
	    /*
	     * we ignore errors while transaction directory deletion. removing
	     * these undeletable directories is job of the database maintenance.
	     */
	    console.log('Unable to delete transaction dir ' + transactionDir + ': ' + e);
	}
    }

    function getPath(storagePath, key){
	return path.join(storagePath, storageFileNameFactory.keyToFileName(key));
    }

    function getStoragePath(success, error){
	if(storagePath !== null){
	    success(storagePath);

	    return;
	}

	var sp = path.join(rootDir, 'storage');
	mkdirs(sp, 0755, function(){
		storagePath = sp;

		success(storagePath);
	    }, error);
    }

    function loadEntry(key, success, error){
	if(DEBUG === true){
	    console.log('Loading entry with key ' + key);
	}

	validateSuccessError(success, error);

	getStoragePath(function(storagePath){
		var entryPath = getPath(storagePath, key);

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
	    }, error);
    }

    function saveEntry(transactionDir, key, entry, success, error){
	if(entry === null || entry === undefined){
	    throw new Error('entry must have a value');
	}

	validateSuccessError(success, error);

	if(DEBUG === true){
	    console.log('Saving entry with key ' + key);
	}

	var entryFileName = path.join(transactionDir, storageFileNameFactory.keyToFileName(key));
	fs.open(entryFileName, 'w', 0640, function(e, id){
		if(e){
		    error(e);

		    return;
		}

		fs.write(id, JSON.stringify(entry), null, 'utf-8', function(){
			fs.close(id);

			success(key, entryFileName);
		    });
	    });
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

	function isEntriesEmpty(){
	    var key;
	    for(key in entries){
		return false;
	    }
	    
	    return true;
	}

	/**
	 * Makes sure that a transaction has not yet been commited or rolled
	 * back.
	 */
	function ensureTransactionIsActive(error){
	    if(entries === null){
		error(new Error('Transaction is no longer active.'));

		return false;
	    }

	    return true;
	}

	function set(key, value){
	    if(value === undefined){
		// entries should only be removed using the drop function
		throw new Error('value can\'t be undefined');
	    }

	    delete droppedKeys[key];

	    entries[key] = value;
	}

	function get(key, success, error){
	    if(droppedKeys[key] === true){
		success(undefined);

		return;
	    }

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
	    delete entries[key];

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

	function containsDuplicateKey(modifiedEntries, droppedKeys){
	    var i, key, keys = {};

	    for(i = 0; i < modifiedEntries.length; ++i){
		key = modifiedEntries[i].key;

		if(keys[key] === true){
		    return true;
		}

		keys[key] = true;
	    }

	    for(key in droppedKeys){
		if(keys[key] === true){
		    return true;
		}

		keys[key] = true;
	    }

	    return false;
	}

	function commitEntries(modifiedEntries, droppedKeys, success, error){
	    if(containsDuplicateKey(modifiedEntries, droppedKeys) === true){
		error(new Error('Duplicate entry detected'));

		return;
	    }

	    getStoragePath(function(storagePath){
		    var i, entryFile, entryStorageFileName, droppedKey;

		    if(DEBUG === true){
			console.log('Commiting transaction');
		    }

		    try{
			// move entry files from transaction to storage directory
			for(i = 0; i < modifiedEntries.length; ++i){
			    entryFile = modifiedEntries[i];

			    entryStorageFileName = getPath(storagePath, entryFile.key);

			    if(DEBUG === true){
				console.log('Commiting update entry with key ' + entryFile.key);
			    }

			    fs.renameSync(entryFile.entryFileName, entryStorageFileName);
			}

			// delete dropped entries
			for(droppedKey in droppedKeys){
			    entryStorageFileName = getPath(storagePath, droppedKey);

			    if(DEBUG === true){
				console.log('Commiting drop entry with key ' + droppedKey);
			    }
			    
			    /*
			     * TODO take care if some other transaction has
			     * already deleted the entry. maybe we should ignore
			     * these deletion errors simply?
			     */
			    fs.unlinkSync(entryStorageFileName);
			}
		    }
		    catch(e){
			if(DEBUG === true){
			    console.log('Commiting failed: ' + e);
			}

			/*
			 * now we've messed up our database storage... that's not so
			 * good
			 */
			if(dbError !== undefined){
			    dbError(e);
			}

			error(e);

			return;
		    }

		    if(DEBUG === true){
			console.log('Commited successfully');
		    }

		    success();
		}, error);
	}

	function commit(success, error){
	    var entryFiles = [];

	    if(readOnly === true){
		error(new Error('Can\'t commit a read only transaction'));

		return;
	    }

	    validateSuccessError(success, error);

	    if(ensureTransactionIsActive(error) === false){
		return;
	    }

	    function doCommit(error, cleanup){
		entries = null;
				
		commitEntries(entryFiles, droppedKeys, function(){
			if(cleanup){
			    cleanup();
			}
			
			droppedKeys = null;

			success();
		    }, error);
	    }

	    if(isEntriesEmpty() === true){
		doCommit(error);

		return;
	    }

	    getNextTransactionDir(function(transactionDir){
		    var key;

		    function error2(){
			deleteTransactionDir(transactionDir);

			error.apply(this, arguments);
		    }

		    for(key in entries){
			saveEntry(transactionDir, key, entries[key], function(key, entryFileName){
				// success
				entryFiles.push({
					key: key,
					    entryFileName: entryFileName
					    });

				delete entries[key];

				if(isEntriesEmpty() === false){
				    /*
				     * not all saveEntry calls have returned yet
				     */
				    return;
				}

				doCommit(error2, function(){
					deleteTransactionDir(transactionDir);
				    });
			    }, error2);
		    }
		}, error);
	}

	function rollback(){
	    /*
	     * not required yet... but I'll keep it in the API to implement some
	     * intelligent sync strategies in the future.
	     */

	    ensureTransactionIsActive(function(e){
		    throw e;
		});
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
