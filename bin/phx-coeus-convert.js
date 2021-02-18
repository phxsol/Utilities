"use strict";

const program = require('commander');
const pkg = require('../package.json');
const db_cfg = require('./config.js').database;
const nano = require('nano')(db_cfg.nano_cfg);

const batch_size = 2*3*5*7*11*13/2;

var fieldNames = null;

var docDB, db_name, design_name, view_name;



/**********************************************************************************************************************/
// Object Definitions //
/**********************************************************************************************************************/



/**********************************************************************************************************************/
// Routines //
/**********************************************************************************************************************/

function proc_init() {
    try {
        program .command('convert [db] [design] [view] ', 'Emit data results .CSV file.')
            .version(pkg.version)
            .usage('[options] <db> <design> <view>')
            .action(function (db, design, view) {
                db_name = db.toLowerCase();
                design_name = design;
                view_name = view;
            })
            .parse(process.argv);

        docDB = nano.use(db_name);

        modelsDocDB = nano.use("phx_models");
        modelsDocDB.get(db_name, function(err,response){
            if(!err) {
                var xfieldNames = response.srcFields.map(pullFieldNamesFromModel);
                fieldNames = [].concat("key", xfieldNames);
                console.log("\"" + fieldNames.join("\",\"") + "\"");                
            }
			extractDataBatch(design_name, view_name, { load_limit: batch_size, skip: 0 });
        });
    } catch(err) {
        console.error(err);
        process.exit(1);
    }
}


function extractDataBatch(designDoc, viewName, params){
    docDB.view(designDoc, viewName, params, function(err, data) {
        if(!err){
            if(!fieldNames){
                var xfieldNames = Object.keys(data.rows[0].value);
                fieldNames = [].concat("key", xfieldNames);
                console.log("\"" + fieldNames.join("\",\"") + "\"");
            }

            var csvExports = data.rows.map(emitCSVRecords);
            if(params.skip + batch_size < data.total_rows){
                params.skip += batch_size;
                console.error("Batch Complete | Next: [ " + params.skip + " to " + (params.skip + batch_size) + " ]");
                extractDataBatch(designDoc, viewName, params);
            } else {
                console.error("Conversion Complete.")
            }
        } else {
            console.error("Extraction failure: failed between record " + params.skip + " and " + params.skip + batch_size);
            process.exit(1);
        }
    });
}



/**********************************************************************************************************************/
// Sub-Routines //
/**********************************************************************************************************************/

function emitCSVRecords(element, index, array){
    var values = [];

    for(var key in fieldNames){
        if(key==0){
            values.push(element.key);
        } else {
            var fieldName = fieldNames[key];
            if (element.value.hasOwnProperty(fieldName)) {
                values.push(encodeURI(element.value[fieldName]));
            } else {
                values.push("");
            }
        }
    }
    console.log("\"" + values.join("\",\"") + "\"");
}

function pullFieldNamesFromModel(element, index, array){
    return element[0];
}

/**********************************************************************************************************************/
// Main Script Entry //
/**********************************************************************************************************************/

proc_init();






/**********************************************************************************************************************/
// Deprecated Logic //
/**********************************************************************************************************************/