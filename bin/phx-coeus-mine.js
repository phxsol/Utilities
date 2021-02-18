"use strict";

const db_cfg = require('../config.js').database;
const fs = require('fs');
const program = require('commander');
const pkg = require('../package.json');
const crc = require('crc');
const dbase_oledb = require('edge-oledb');
const dbase_src_path = db_cfg.dbase_src_path;



var srcFile_dateLastMod = null, oledbOptions = null, phxModelsDocDB = null, xFerProcessDocDB = null, xFerProcLog = null, xFerProc = null, xFerTableDocDB = null;
var batchesXFerdSoFar = 0, recsAdded = 0, recsUpdated = 0, recsSkipped = 0, WTFdRecs = 0, recsAssumedDeleted = 0;
var isStillKosher_xFer = true;

var dbSafe_tableName, oleDB_tableName, fieldCount, totalRecs, fieldList;
var srcFile_fullPath, batchesToXFer;

// Default Values
var batch_size = 2*3*5*7*11*7;
var start_recno = 1;
var brake = 2;

program
	.command('mine [file]', 'Instruct Coeus to mine the data from a dBase file.')
    .version(pkg.version)
    .usage('[options] <file>')
    .option('-s, --start-recno [value]', 'Set the depth at which to start mining.')
    .option('-b, --brake [value]', 'Set the brake speed (Full Batch)< 0 -- 6 >(Single Record).')
    .action(function (f) {
        oleDB_tableName = f;
        srcFile_fullPath = dbase_src_path + oleDB_tableName + ".dbf";
        dbSafe_tableName = f.toLowerCase();

        if(program.brake) brake = program.brake;
        if(program.startRecno) start_recno = program.startRecno;
    })
    .parse(process.argv);



/**********************************************************************************************************************/
// Object Definitions //
/**********************************************************************************************************************/

function XFerProcLog(){
    this.fields = [];
    this.procHistory = [];
    this.recordCount = null;
    this.srcLastModified = null;
    this.lastXFerKosher = false;
}

function XFerProcess(timeStamp){
    this.consoleLog = [];
    this.exceptions = [];
    this.batches = [];
    this.srcLastModified = null;
    this.isKosher = false;
    this.recsAdded = null;
    this.recsUpdated = null;
    this.recsSkipped = null;
    this.WTFdRecs = null;
    this.recsAssumedDeleted = null;
    this.proc_start = timeStamp;
    this.proc_stop = null;
}

function XFerLogEntry(timeStamp, logMessage){
    this.logMessage = logMessage;
    this.timeStamp = timeStamp;
}
function XFerLogEntry(timeStamp, err, errMessage){
    this.err = err;
    this.errMessage = errMessage;
    this.timeStamp = timeStamp;
}



/**********************************************************************************************************************/
// Routines //
/**********************************************************************************************************************/

function proc_init(init_phase) {
    try {
        if(init_phase==-1){
            // Brake harder.
            start_recno = batch_size * batchesXFerdSoFar;
            brake++;
            if(brake<=6){
                proc_statusUpdate("Applying brake... level " + brake, true);
                proc_init(1);
            } else {
                // Flow isn't the issue... maximum brake applied.
                proc_statusUpdate("Maximum Braking Ineffective... exiting now.", true);
                proc_exit(1);
            }
        } else if(init_phase==1) {
            // Apply the brake.
            switch(brake){
                case -1:
                    /*  Nothing really, just a placeholder to show that -1 is expected,
                        and not to futz with the batch_size if a brake wasn't specified.*/
                    break;
                case 0:
                    batch_size = 2*3*5*7*11*7;
                    break;
                case 1:
                    batch_size = 2*3*5*7*11*7/2;
                    break;
                case 2:
                    batch_size = 2*3*5*7*11*7/2/3;
                    break;
                case 3:
                    batch_size = 2*3*5*7*11*7/2/3/5;
                    break;
                case 4:
                    batch_size = 2*3*5*7*11*7/2/3/5/7;
                    break;
                case 5:
                    batch_size = 2*3*5*7*11*7/2/3/5/7/11;
                    break;
                case 6:
                    batch_size = 2*3*5*7*11*7/2/3/5/7/11/7;
                    break;
            }
            if(start_recno)

            xFerProc = new XFerProcess(Date.now());
            if (!srcFile_dateLastMod) {
                var fileStats = fs.statSync(srcFile_fullPath);
                srcFile_dateLastMod = fileStats.mtime.getTime();
                xFerProc.srcLastModified = srcFile_dateLastMod;
            }

            var nano = require('nano')(db_cfg.nano_cfg);
            nano.db.get(dbSafe_tableName, function (err, body) {
                if (err) {
                    nano.db.create(dbSafe_tableName, function (err, body) {
                        if (err) {
                            proc_error(err, "xFerTableDocDB " + dbSafe_tableName + ": Exception thrown during creation.");
                            proc_exit(1);
                        }
                        else {
                            xFerTableDocDB = nano.use(dbSafe_tableName);
                            proc_init(2);
                        }
                    });
                } else {
                    proc_init(2);
                }
            });
        } else if(init_phase==2){
            var nano = require('nano')(db_cfg.nano_cfg);
            var xFerProcessDocDB = nano.use(db_cfg.xfer_proc);
            xFerProcessDocDB.get(oleDB_tableName, {revs_info: true}, function (err, data) {
                if (err) {
                    if (err.reason == "no_db_file") {
                        console.log(db_cfg.xfer_proc + " docDB hasn't been initialized.");
                        nano.db.create(db_cfg.xfer_proc, function (err, body) {
                            if (err) {
                                proc_error(err);
                                proc_exit(1);
                            }
                            console.log('Done! | ' + db_cfg.xfer_proc + 'docDB initialization is_phase_complete.');

                            xFerProcLog = new XFerProcLog();
                            xFerProcessDocDB.insert(xFerProcLog, oleDB_tableName, function (err, body) {
                                if (err) {
                                    proc_error(err);
                                    proc_exit(1);
                                } else {
                                    xFerProcLog._rev = body.rev;
                                }
                                proc_init(3);
                            });
                        });
                    } else {
                        xFerProcLog = new XFerProcLog();
                        xFerProcessDocDB.insert(xFerProcLog, oleDB_tableName, function (err, body) {
                            if (err) {
                                proc_error(err);
                                proc_exit(1);
                            } else {
                                xFerProcLog._rev = body.rev;
                            }
                            proc_init(3);
                        });
                    }
                } else {
                    xFerProcLog = data;
                    proc_init(3);
                }

            });
        } else if(init_phase==3) {
            if (xFerProcLog.lastXFerKosher && xFerProcLog.srcLastModified && xFerProcLog.srcLastModified == srcFile_dateLastMod) {
                console.log("This table remains unchanged... skipping xFer process.");
                xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), "This table remains unchanged... skipping xFer process."));
                proc_exit(0);
            } else {

                xFerProcLog.srcLastModified = srcFile_dateLastMod;


                oledbOptions = {
                    dsn: "Provider=VFPOLEDB; Data Source=" + dbase_src_path + "vet.dbc; Mode=ReadWrite|Share Deny None;",
                    query: "SELECT RECCOUNT() as reccount, FCOUNT() as fldcount FROM " + dbSafe_tableName + " WHERE RECNO()=1",
                    getFieldsOnly: true
                };

                dbase_oledb(oledbOptions, function (result) {
                    if (result.error) {
                        proc_error(result.error);
                    }

                    if (result.records && result.records.length == 0) {
                        console.log("This table contains 0 records.");
                        xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), "This table contains 0 records."));
                        proc_exit(0);
                    } else {

                        xFerProcLog.recordCount = totalRecs = parseInt(result.records[0]["reccount"]);
                        xFerProcLog.fieldCount = fieldCount = parseInt(result.records[0]["fldcount"]);
                        oledbOptions.query = "SELECT * FROM " + dbSafe_tableName + " WHERE RECNO()=1";
                        dbase_oledb(oledbOptions, function (result) {
                            if (result.error) {
                                proc_error(result.error);
                            }

                            fieldList = new Array();
                            for (var field in result.records[0]) {
                                var fldType = result.records[0][field];
                                fieldList.push([field, fldType]);
                            }
                            xFerProcLog.fields = fieldList;
                            var nano = require('nano')(db_cfg.nano_cfg);
                            phxModelsDocDB = nano.use(db_cfg.models);
                            phxModelsDocDB.get(dbSafe_tableName, {revs_info: true}, function (err, data) {
                                var phxModel = (err) ? {} : data;
                                phxModel.srcFields = [].concat(fieldList);
                                phxModelsDocDB.insert(phxModel, dbSafe_tableName, function (err, data) {
                                    if (!err) {

                                    }
                                });
                            });

                            batchesXFerdSoFar = 0;
                            batchesToXFer = (totalRecs - start_recno <= batch_size) ? 1 : (Math.ceil((totalRecs - start_recno) / batch_size));
                            xFerDataBatch(start_recno, start_recno + batch_size - 1);
                        });
                    }

                });
            }
        }
    } catch(err) {
        proc_error(err);
        proc_exit(1);
    }
}

function xFerDataBatch(pBatchFloor, pBatchCeiling) {
    try {
        var pBatchCeiling = (pBatchCeiling > totalRecs) ? totalRecs : pBatchCeiling;

        var batchQuery = "SELECT * FROM " + dbSafe_tableName + " WHERE RECNO()>=" + pBatchFloor + " AND RECNO()<=" + pBatchCeiling;
        oledbOptions.query = batchQuery;
        oledbOptions.getFieldsOnly = false;

        dbase_oledb(oledbOptions, function (result) {
            if (result.error) {
                proc_error(result.error);
                proc_init(-1);
            } else {
                var taggedRecords = result.records.map(tagRecords, pBatchFloor);
                var recNos = taggedRecords.map(mapRecNosList);
                var nano = require('nano')(db_cfg.nano_cfg);
                var xFerTableDocDB = nano.use(dbSafe_tableName);
                xFerTableDocDB.fetch({"keys": recNos}, function (err, data) {
                    if (err) {
                        proc_error(err);
                        proc_init(-1);
                    } else {
                        data = (!data.rows) ? JSON.parse(data) : data;
                        var emptySet = (data.rows.length == 0);

                        var recsToAdd = (!emptySet) ? taggedRecords.filter(filter_recsToAdd, data.rows) : [];
                        var recsToUpdate = (!emptySet) ? taggedRecords.filter(filter_recToUpdate, data.rows) : [];
                        var recsToSkip = (!emptySet) ? taggedRecords.filter(filter_recToSkip, data.rows) : [];
                        var wtfRecs = (!emptySet) ? taggedRecords.filter(filter_recWTFs, data.rows) : [];

                        recsAdded += recsToAdd.length;
                        recsUpdated += recsToUpdate.length;
                        recsSkipped += recsToSkip.length;
                        WTFdRecs += wtfRecs.length;
                        var assumed_deleted_count = data.rows.length - (pBatchCeiling - pBatchFloor + 1);
                        recsAssumedDeleted += assumed_deleted_count;

                        var req_batch_size = (pBatchCeiling - pBatchFloor + 1);
                        var isKosher = (wtfRecs.length == 0 && (recsToAdd.length + recsToUpdate.length + recsToSkip.length) == data.rows.length);
                        if (!isKosher){
                            isStillKosher_xFer = false;
                            var whyNoIsKosher = "Curious Batch... xFer Stats: [ " + data.rows.length + " of " + req_batch_size + " ] | WTFs: " + wtfRecs.length + " | Added: " + recsToAdd.length + " | Updated: " + recsToUpdate.length + " | Skipped: " + recsToSkip.length + " | @Deleted: " + assumed_deleted_count;
                            proc_statusUpdate(whyNoIsKosher, false);
                        } else {
                            var whyIsKosher = "Kosher Batch... xFer Stats: [ " + data.rows.length + " of " + req_batch_size + " ] | WTFs: " + wtfRecs.length + " | Added: " + recsToAdd.length + " | Updated: " + recsToUpdate.length + " | Skipped: " + recsToSkip.length + " | @Deleted: " + assumed_deleted_count;
                            proc_statusUpdate(whyIsKosher, false);
                        }

                        xFerTableDocDB.bulk({docs: recsToAdd}, function (err, data) {
                            if (err) {
                                proc_error(err);
                                proc_init(-1);
                            }
                            var emptySet = (data.length == 0);

                            var insertSuccs = (!emptySet) ? data.filter(tally_successes) : [];
                            var insertFails = (!emptySet) ? data.filter(tally_failures) :[];

                            xFerTableDocDB.bulk({docs: recsToUpdate}, function (err, data) {
                                if (err) {
                                    proc_error(err);
                                    proc_init(-1);
                                }
                                var emptySet = (data.length == 0);

                                var updateSuccs = (!emptySet) ? data.filter(tally_successes) : [];
                                var updateFails = (!emptySet) ? data.filter(tally_failures) : [];

                                xFerProc.batches.push({
                                    "timestamp": Date.now(),
                                    "first_rec_num": pBatchFloor,
                                    "last_rec_num": pBatchCeiling,
                                    "recsToAdd" : recsToAdd.length,
                                    "insertSuccs": insertSuccs.length,
                                    "insertFails": insertFails,
                                    "recsToUpdate": recsToUpdate.length,
                                    "updateSuccs": updateSuccs.length,
                                    "updateFails": updateFails,
                                    "recsToSkip": recsToSkip.length,
                                    "wtfRecs" : wtfRecs,
                                    "recsAssumedDeleted": recsAssumedDeleted,
                                    "brakeLevel": brake
                                });

                                batchesXFerdSoFar++;
                                if(pBatchCeiling < totalRecs)
                                {
                                    proc_statusUpdate("Batch [ " + batchesXFerdSoFar + " ] of [ " + batchesToXFer + " ] xFer'd.", true);
                                    xFerDataBatch(pBatchFloor + batch_size, pBatchCeiling + batch_size);
                                } else {
                                    console.log("All batches completed.");
                                    proc_exit(0);
                                }
                            });
                        });
                    }
                });
            }
        });
    } catch(err) {
        proc_error(err);
        proc_init(-1);
    }
}

function proc_statusUpdate(status, postNow){
    console.log(status);
    xFerProc = (!xFerProc) ? new XFerProcess(Date.now()) : xFerProc;
    xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), status));

    if(postNow) {
        var nano = require('nano')(db_cfg.nano_cfg);
        var xFerProcessDocDB = nano.use(db_cfg.xfer_proc);
        xFerProcessDocDB.get("inProc_xFerProc", {revs_info: true}, function (err, data) {
            var singleton_xFerProc = xFerProc;
            if (!err) {
                singleton_xFerProc._rev = data._rev;
            }
            xFerProcessDocDB.insert(singleton_xFerProc, "inProc_xFerProc", function (err, data) {
                if (err) {
                    console.error("proc_statusUpdate Exception");
                    console.error(err + " : " + err.message);
                    xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), "proc_statusUpdate Exception"));
                    xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), err, err.message));
                    xFerProc.exceptions.push(err);
                }
            });
        });
    }
}

function proc_error(err) {
    proc_error(err, null);
}

function proc_error(err, comment){
    console.log("}=> proc_error");
    if(comment) console.log(comment);
    console.error(err, err.message);
    xFerProc = (!xFerProc) ? new XFerProcess(Date.now()) : xFerProc;
    xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), err, err.message));
    var nano = require('nano')(db_cfg.nano_cfg);
    var xFerProcessDocDB = nano.use(db_cfg.xfer_proc);
    xFerProcessDocDB.get("inProc_xFerProc", {revs_info: true}, function(err, data){
        var singleton_xFerProc = xFerProc;
        if(!err){
            singleton_xFerProc._rev = data._rev;
        }
        xFerProcessDocDB.insert(singleton_xFerProc, "inProc_xFerProc", function(err, data){
            if(err){
                console.error("proc_error Exception");
                console.error(err + " : " + err.message);
                xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), "proc_error Exception"));
                xFerProc.consoleLog.push(new XFerLogEntry(Date.now(), err, err.message));
                xFerProc.exceptions.push(err);
            }
        });
    });
}

function proc_exit(exitCode) {
    xFerProcLog = (!xFerProcLog) ? new XFerProcLog() : xFerProcLog;
    xFerProc = (!xFerProc) ? new XFerProcess(Date.now()) : xFerProc;
    var nano = require('nano')(db_cfg.nano_cfg);
    var xFerProcessDocDB = nano.use(db_cfg.xfer_proc);
    xFerProcessDocDB.get("inProc_xFerProc", {revs_info: true}, function(err, data){
        if (!err) {
            xFerProc._rev = data._rev;
        }
        xFerProc.isKosher = isStillKosher_xFer;
        xFerProc.recsAdded = recsAdded;
        xFerProc.recsUpdated = recsUpdated;
        xFerProc.recsSkipped = recsSkipped;
        xFerProc.WTFdRecs = WTFdRecs;
        xFerProc.recsAssumedDeleted = recsAssumedDeleted;
        xFerProc.proc_stop = Date.now();
        xFerProcessDocDB.insert(xFerProc, "inProc_xFerProc", function (err, data) {

            xFerProcessDocDB.get(oleDB_tableName, {revs_info: true}, function(err, data){
                if(!err) {
                    xFerProcLog.procHistory = (data.procHistory) ? data.procHistory : [];
                    xFerProcLog._rev = data._rev;
                }
                xFerProcLog.procHistory.push(xFerProc);
                xFerProcLog.lastXFerKosher = isStillKosher_xFer;
                xFerProcessDocDB.insert(xFerProcLog, oleDB_tableName, function(err){
                    if(err){
                        console.log("Well, the xFerProcLog never made it to the safehouse... See you around Chuck.")
                        console.error(err + " : " + err.message);
                    }
                    process.exit(exitCode);
                })
            });
        });
    });
}



/**********************************************************************************************************************/
// Sub-Routines //
/**********************************************************************************************************************/

function tagRecords(item, index){
    item._id = (this + index).toString();
    item.phxHash = crc.crc32(JSON.stringify(item)).toString(32);
    return item;
}

function mapRecNosList(item){
    return item._id;
}

function filter_recsToAdd(element, index){
    if (this[index].error && this[index].error == "not_found" && !element.failure)  {
        return true;
    }
    if (this[index].value && this[index].value.deleted && !element.failure) {
        return true;
    }
}

function filter_recToUpdate(element, index) {
    if (this[index].doc && !element.failure) {
        var srcRec = element;
        if(srcRec.phxHash != this[index].doc.phxHash){
            element._rev = this[index].value.rev;
            return true;
        }
    }
}

function filter_recToSkip(element, index) {
    if (this[index].doc && !this[index].value.deleted) {
        var srcRec = element;
        return (srcRec.phxHash == this[index].doc.phxHash);
    }
}

function filter_recWTFs(element, index){
    if (element.failure) {
        return true;
    }
}

function tally_failures(item){
    return(item.error);
}

function tally_successes(item){
    return(!item.error);
}



/**********************************************************************************************************************/
// Main Script Entry //
/**********************************************************************************************************************/

proc_init(1);