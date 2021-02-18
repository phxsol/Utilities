const db_cfg = require('../config.js').database;
const nano = require('nano')(db_cfg.nano_cfg);


function couchDB_tables(tableName){
    return(tableName.startsWith("_"));
}

function phx_tables(tableName){
    return(tableName.startsWith("phx_") && tableName != "phx_xfer");
}

function xFerd_tables(tableName){
    return(!tableName.startsWith("_") && !tableName.startsWith("phx_") || tableName == "phx_xfer");
}

function clean_tables(tableName){
    return(!tableName.startsWith("_"));
}

function proc_init(){
    nano.db.list(function(err, response){
       if(!err){
           var couchDB_tables = response.filter(function(x){return(x.startsWith("_"))});
           var phx_tables = response.filter(function(x){return(x.startsWith("phx_"))});
           var xFerd_tables = response.filter(function(x){return(!x.startsWith("_") && !x.startsWith("phx_"))});
		   var clean_tables = response.filter(function(x){return(!x.startsWith("_"))});
           console.log("== Table Discovery ==");
           console.log("");
           console.log("System DBs (KEEP): " + couchDB_tables.length);
           //console.log("Phoenix DBs (KEEP): " + phx_tables.length);
           
		   console.log("clean_tables (DESTROY): " + clean_tables.length);		   
		   for(var ndx in clean_tables){
               nano.db.destroy(clean_tables[ndx]);
           }
       }
    });
}


proc_init();