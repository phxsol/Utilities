/**********************************************************************************************************************/
// initialization //
/**********************************************************************************************************************/

"use strict";
let whenever_an_err = [], curiosity = {};
let config = (function(){

    let _curio, _phase;
    let _data_packages = require('../config.js').data_packages;
    let _curiosities = require('../config.js').curiosities;
    let _db_cfg = require('../config.js').database;

	// fetch the command-line parameters from node commander.
    let program = require('commander');
    let pkg = require('../package.json');
	program
		.command('model [curiosity]', 'Instruct Coeus to model one of his Curiosities.')
        .version(pkg.version)
        .usage('[options] <curiosity>')
        .option('-p, --phase [value]', 'Set the Phase at which to begin modelling the requested Curiosity.')
		.action(function (curiosity) {
			_curio = curiosity.toLowerCase();
			_phase = (program.phase) ? parseInt(program.phase) : 0;
		})
		.parse(process.argv);	
	
	return {
		curio: _curio,
		phase: _phase,
		data_packages: _data_packages,
		curiosities: _curiosities,
		db_cfg: _db_cfg
	}
	
})();


/**********************************************************************************************************************/
// routines //
/**********************************************************************************************************************/

// fetch() pulls a set from the docDB's fetchRevs results.  This self batches, so will scale to any currently foreseeable result sizes.
// => return_all determines whether to return the entire view results, or a single batch at a time... either way overall batch progress is tracked globally.
function fetch(perspective, return_all, callback){
    let manifest = [];
    let nano = require('nano')(config.db_cfg.nano_cfg);
    let source_db = nano.use(perspective.db);
    let batch_errors = 0;
    perspective.batch_ndx = 0;
    let view_params = { include_docs: perspective.include_docs, limit: perspective.batch_size, reduce: perspective.reduce };

    // This is the callback used by the initial and subsequent (batched) requests.
    let batch_callback = function (err, response) {
        if (err) {
            batch_errors++;
            whenever_an_err.push(err);
            console.error(err);
            process.exit(1);	// TODO: Handle and remove hard exit.
        } else {
            let total_rows = perspective.keys.length;
            manifest = manifest.concat(response.rows.map(perspective.cleaned_response_map));
            let fetched_so_far = perspective.batch_size * perspective.batch_ndx;
            let items_remaining = (perspective.load_limit) ? perspective.load_limit - fetched_so_far : total_rows - fetched_so_far;
            let less_than_a_batch = (items_remaining < perspective.batch_size);
            let items_this_go = (less_than_a_batch) ? items_remaining : perspective.batch_size;
            console.log("fetching " + perspective.db + " data %d source records  |  extracting %d more now --> %d to go this load... %d issues so far.", total_rows, items_this_go, items_remaining - items_this_go, batch_errors);
            let rows_to_skip = perspective.start + (++perspective.batch_ndx * perspective.batch_size);
            let is_last_batch = (rows_to_skip >= perspective.keys.length);

            if(return_all && !is_last_batch) {
                view_params = { include_docs: perspective.include_docs };
                source_db.fetchRevs({keys: perspective.keys}, view_params, batch_callback);
            } else {
                callback(manifest, is_last_batch);
            }
        }
    };

    // the first batch is requested here.
    source_db.fetchRevs({keys: perspective.keys}, view_params, batch_callback);
}

// extract() pulls the entirety of a docDB's view results - using the DesignDoc/View in couchDB.  this self batches, so will scale to any currently foreseeable result sizes.
// => return_all determines whether to return the entire view results, or a single batch at a time... either way overall batch progress is tracked globally.
function extract(perspective, return_all, callback){
	let manifest = [];
    let total_rows = 0;	// important value here... though i cannot remember why at the moment.
    let nano = require('nano')(config.db_cfg.nano_cfg);
    let source_db = nano.use(perspective.db);
    let batch_errors = 0;
    perspective.batch_ndx = 0;
    let view_params = { include_docs: perspective.include_docs, limit: perspective.batch_size, skip: perspective.start, reduce: perspective.reduce };
    if(perspective.keys) view_params.keys = perspective.keys;

	// This is the callback used by the initial and subsequent (batched) requests.
    let batch_callback = function (err, response) {
		if (err) {
			batch_errors++;
			whenever_an_err.push(err);
			console.error(err);
			process.exit(1);	// TODO: Handle and remove hard exit.
		} else {
			total_rows = response.total_rows;
			manifest = manifest.concat(response.rows.map(perspective.cleaned_response_map));
            let extracted_so_far = perspective.batch_size * perspective.batch_ndx;
            let items_remaining = (perspective.load_limit) ? perspective.load_limit - extracted_so_far : total_rows - extracted_so_far;
            let less_than_a_batch = (items_remaining < perspective.batch_size);
            let items_this_go = (less_than_a_batch) ? items_remaining : perspective.batch_size;
            console.log("extracting " + perspective.db + " data %d source records  |  extracting %d more now --> %d to go this load... %d issues so far.", total_rows, items_this_go, items_remaining - items_this_go, batch_errors);
            let rows_to_skip = perspective.start + (++perspective.batch_ndx * perspective.batch_size);
            let is_last_batch = (perspective.load_limit) ? (rows_to_skip >= perspective.start + perspective.load_limit || rows_to_skip >= total_rows) : (rows_to_skip >= total_rows);
			if(return_all && !is_last_batch) {
                view_params = { include_docs: perspective.include_docs, limit: perspective.batch_size, skip: rows_to_skip, reduce: perspective.reduce };
                if(perspective.keys) view_params.keys = perspective.keys;
                source_db.view(perspective.design, perspective.view, view_params, batch_callback);
			} else {
				callback(manifest, is_last_batch);
			}
		}
	};

	// the first batch is requested here.
    source_db.view(perspective.design, perspective.view, view_params, batch_callback);
}

// store() places the generated curiosity into the corresponding couchDB repository.  large sets are broken into batches automatically.
function store(){

    let batch_limit = 10;
    let batch = null;
	
	// this is the callback used by the initial and subsequent (batched) requests.
    let batch_callback = function (err, response) {
		if (err) {
			whenever_an_err.push(err);
			console.error(err);
		}
		if(response) console.log("batch stored: " + config.curio);
		for(let ndx in curiosity){
			if(curiosity.hasOwnProperty(ndx)){
				if(!batch) batch = [];
				batch.push(curiosity[ndx]);
				delete curiosity[ndx];
				if(batch.length == batch_limit) {
                    let nano = require('nano')(config.db_cfg.nano_cfg);
                    let modelDB = nano.use(config.curiosities[config.curio]);
					modelDB.bulk({docs:batch}, batch_callback);
					batch = null;
					break;
				}
			}
		}
		if(batch){
            let nano = require('nano')(config.db_cfg.nano_cfg);
            let modelDB = nano.use(config.curiosities[config.curio]);
			modelDB.bulk({docs:batch}, batch_callback);
			batch = null;
		}
	};

	batch_callback(null, null);
}



/**********************************************************************************************************************/
// sub-routines //
/**********************************************************************************************************************/

function inventor_manifest_response_map(inventor_doc){
	if(!inventor_doc.key) inventor_doc.key = "N/A";
	return {
		id: inventor_doc.id,
		part_num: inventor_doc.key,
		quotes: [],
		orders: []
	}
}

function quote_manifest_response_map(quote_doc){
	if(!quote_doc.key) quote_doc.key = "N/A";	// q1_part_num is the acting index... missing/corrupt field data is normalized to "N/A".

	return {
		id: quote_doc.id,
		quote_num: quote_doc.value,
		part_num: quote_doc.key
	}
}

function order_manifest_response_map(order_doc){
	if(!order_doc.key) order_doc.key = "N/A";	// or_part_num is the acting index... missing/corrupt field data is normalized to "N/A".

	return {
		id: order_doc.id,
		order_num: order_doc.value,
		part_num: order_doc.key
	}
}

function operate_manifest_response_map(ops_doc){
	if(!ops_doc.key) ops_doc.key = "N/A";	// op_quote_num is the acting index... missing/corrupt field data is normalized to "N/A".

	return {
		id: ops_doc.id,
		op_num: ops_doc.value,
		quote_num: ops_doc.key
	}
}

function aoperate_manifest_response_map(aops_doc){
	if(!aops_doc.key) aops_doc.key = "N/A";	// aop_order_num is the acting index... missing/corrupt field data is normalized to "N/A".

	return {
		id: aops_doc.id,
		aop_num: aops_doc.value,
		order_num: aops_doc.key
	}
}

function coeus_parts_response_map(parts_doc){
    return parts_doc.doc;
}

function aop_ids_from_parts_curiosity(parts_curiosity){
    let aop_ids = [];
    for(let order_ndx in parts_curiosity.value.orders){
        let order = parts_curiosity.value.orders[order_ndx];
        aop_ids = aop_ids.concat(order.aops);
    }
    return aop_ids;
}

function op_ids_from_parts_curiosity(parts_curiosity){
    let op_ids = [];
    for(let quote_ndx in parts_curiosity.value.quotes){
        let quote = parts_curiosity.value.quotes[quote_ndx];
        op_ids = op_ids.concat(quote.ops);
    }
    return op_ids;
}

function aop_response_map(aop_doc){
    return aop_doc.doc;
}

function op_response_map(op_doc){
    return op_doc.doc;
}

/**********************************************************************************************************************/
// script //
/**********************************************************************************************************************/

switch (config.curio) {
    case 'coeus_parts_curiosity':
        /* Phase 1a: Extraction */
        let fetch_part_manifests = function(){
            // fetch_inventor_manifest
            let fetch_inventor_manifest = new Promise(function(resolve, reject){
                try{
                    let data_perspective = config.data_packages['inventor_by_part_num'];
                    data_perspective.cleaned_response_map = inventor_manifest_response_map;
                    extract(data_perspective, true, function(manifest, b){
                        resolve(manifest);
                    });
                } catch(err) {
                    reject(err);
                }
            });
            console.log("fetching inventor_manifest...");
            fetch_inventor_manifest.then(function(val) {
                console.log("inventor_manifest extracted.");
                let inventor_manifest = val;
                for(let inv_ndx in inventor_manifest){
                    let inventor = inventor_manifest[inv_ndx];
                    curiosity[inventor.part_num] = inventor;
                }
                agenda.mark_progress('fetch_inventor_manifest');
            }).catch(function(reason){
                console.error("issue encountered while fetching inventor_manifest. | reason: " + reason);
            });

            // fetch_quote_manifest
            let quotes = {};
            let fetch_quote_manifest = new Promise(function(resolve, reject){
                try{
                    let data_perspective = config.data_packages['quote_by_part_num'];
                    data_perspective.cleaned_response_map = quote_manifest_response_map;
                    extract(data_perspective, true, function(manifest, b){
                        resolve(manifest);
                    });
                } catch(err) {
                    reject(err);
                }
            });
            console.log("fetching quote_manifest...");
            fetch_quote_manifest.then(function(val) {
                console.log("quote_manifest extracted.");
                let quote_manifest = val;
                for(let quote_ndx in quote_manifest){
                    let quote = quote_manifest[quote_ndx];
                    quotes[quote.quote_num] = quote;
                    quotes[quote.quote_num].ops = [];
                }
                agenda.mark_progress('fetch_quote_manifest');
            }).catch(function(reason){
                console.error("issue encountered while fetching quote_manifest. | reason: " + reason);
            });

            // fetch_order_manifest
            let orders = {};
            let fetch_order_manifest = new Promise(function(resolve,reject){
                try{
                    let data_perspective = config.data_packages['order_by_part_num'];
                    data_perspective.cleaned_response_map = order_manifest_response_map;
                    extract(data_perspective, true, function(manifest, b){
                        resolve(manifest);
                    });
                } catch(err) {
                    reject(err);
                }
            });
            console.log("fetching order_manifest...");
            fetch_order_manifest.then(function(val) {
                console.log("order_manifest extracted.");
                let order_manifest = val;
                for(let order_ndx in order_manifest){
                    let order = order_manifest[order_ndx];
                    orders[order.order_num] = order;
                    orders[order.order_num].aops = [];
                }
                agenda.mark_progress('fetch_order_manifest');
            }).catch(function(reason){
                console.error("issue encountered while fetching order_manifest. | reason: " + reason);
            });


            // fetch_operate_manifest
            let fetch_operate_manifest = new Promise(function(resolve,reject){
                try{
                    let data_perspective = config.data_packages['operate_by_quote_num'];
                    data_perspective.cleaned_response_map = operate_manifest_response_map;
                    extract(data_perspective, true, function(manifest, b){
                        resolve(manifest);
                    });
                } catch(err) {
                    reject(err);
                }
            });
            console.log("fetching operate_manifest...");
            fetch_operate_manifest.then(function(val) {
                console.log("operate_manifest extracted.");
                let operate_manifest = val;
                for(let op_ndx in operate_manifest){
                    let op = operate_manifest[op_ndx];
                    if(!quotes[op.quote_num]) quotes[op.quote_num] = { quote_num: op.quote_num, ops: [] };
                    quotes[op.quote_num].ops.push(op.id);
                }
                agenda.mark_progress('fetch_operate_manifest');
            }).catch(function(reason){
                console.error("issue encountered while fetching operate_manifest. | reason: " + reason);
            });


            // fetch_aoperate_manifest
            let fetch_aoperate_manifest = new Promise(function(resolve,reject){
                try{
                    let data_perspective = config.data_packages['aoperate_by_part_num'];
                    data_perspective.cleaned_response_map = aoperate_manifest_response_map;
                    extract(data_perspective, true, function(manifest, b){
                        resolve(manifest);
                    });
                } catch(err) {
                    reject(err);
                }
            });
            console.log("fetching aoperate_manifest...");
            fetch_aoperate_manifest.then(function(val) {
                console.log("aoperate_manifest extracted.");
                let aoperate_manifest = val;
                for(let aop_ndx in aoperate_manifest){
                    let aop = aoperate_manifest[aop_ndx];
                    if(!orders[aop.order_num]) orders[aop.order_num] = { order_num: aop.order_num, aops: [] };
                    orders[aop.order_num].aops.push(aop.id);
                }
                agenda.mark_progress('fetch_aoperate_manifest');
            }).catch(function(reason){
                console.error("issue encountered while fetching aoperate_manifest. | reason: " + reason);
            });

        };

        /* Phase 1b: Assembly */
        let assemble_curiosity = function(){
            try {
                console.log('assembling curiosity...');
                for (let quotes_ndx in quotes) {
                    if (quotes.hasOwnProperty(quotes_ndx)) {
                        let quote = quotes[quotes_ndx];
                        if (!curiosity[quote.part_num]) {
                            curiosity[quote.part_num] = {
                                part_num: quote.part_num,
                                quotes: []
                            }
                        } else if(!curiosity[quote.part_num].quotes){
                            curiosity[quote.part_num].quotes = [];
                        }
                        curiosity[quote.part_num].quotes.push(quote);
                    }
                }
                agenda.mark_progress('assemble_quotes_to_parts');
                for (let orders_ndx in orders) {
                    if (orders.hasOwnProperty(orders_ndx)) {
                        let order = orders[orders_ndx];
                        if (!curiosity[order.part_num]) {
                            curiosity[order.part_num] = {
                                part_num: order.part_num,
                                orders: []
                            }
                        } else if(!curiosity[order.part_num].orders){
                            curiosity[order.part_num].orders = [];
                        }
                        curiosity[order.part_num].orders.push(order);
                    }
                }
                agenda.mark_progress('assemble_orders_to_parts');
                store();
            } catch(err) {
                whenever_an_err.push(err);
                console.error(err);
            }
        };

        /* Phase 2: Performance Calculation */
        let calculate_performance = function(start_point){
            try {
                let fetch_parts_curiosity = new Promise(function(resolve,reject){
                    try{
                        let data_perspective = config.data_packages['coeus_parts_by_part_num'];
                        data_perspective.cleaned_response_map = coeus_parts_response_map;
                        data_perspective.start = (!start_point) ? 0 : start_point;
                        extract(data_perspective, true, function(manifest, b){
                            resolve(manifest);
                        });
                    } catch(err) {
                        reject(err);
                    }
                });
                console.log("pulling parts_curiosity...");
                fetch_parts_curiosity.then(function(val) {
                    console.log("parts_curiosity extracted.");
                    function* parts_iterator(parts){
                        for(let ndx in parts){
                            yield parts[ndx];
                        }
                    }
                    let parts = parts_iterator(val);
                    let read_threads = 0;
                    let ready_count = 0;
                    function analyze_part_performance(part_to_analyze, callback){
                        // Analyze Ops
                        let ops = {
                            ready: false,
                            analyzed: false,
                            manifest: []
                        };
                        let fetch_part_ops = new Promise(function(resolve,reject){
                            try{
                                let ops_ids = op_ids_from_parts_curiosity(part_to_analyze);
                                if(ops_ids.length == 0) resolve([]);
                                let data_perspective = config.data_packages['operate_all_docs'];
                                data_perspective.cleaned_response_map = op_response_map;
                                data_perspective.keys = ops_ids;
                                read_threads++;
                                fetch(data_perspective, true, function(manifest, b){
                                    resolve(manifest);
                                });
                            } catch(err) {
                                reject(err);
                            }
                        });
                        fetch_part_ops.then(function(val){
                            read_threads--;
                            part_to_analyze.value.ops = val;
                            ops.ready = true;
                            save_when_ready();

                            // TODO: Run Calcs on OPS

                        }).catch(function(reason){
                            console.error("issue encountered while fetching part_ops. | reason: " + reason);
                            ops.ready = true;
                        });

                        // Analyze Aops
                        let aops = {
                            ready: false,
                            analyzed: false,
                            manifest: []
                        };
                        let fetch_part_aops = new Promise(function(resolve,reject){
                            try{
                                let aops_ids = aop_ids_from_parts_curiosity(part_to_analyze);
                                if(aops_ids.length == 0) resolve([]);
                                let data_perspective = config.data_packages['aoperate_all_docs'];
                                data_perspective.cleaned_response_map = aop_response_map;
                                data_perspective.keys = aops_ids;
                                read_threads++;
                                fetch(data_perspective, true, function(manifest, b){
                                    resolve(manifest);
                                });
                            } catch(err) {
                                reject(err);
                            }
                        });
                        fetch_part_aops.then(function(val){
                            read_threads--;
                            part_to_analyze.value.aops = val;
                            aops.ready = true;
                            save_when_ready();

                            // TODO: Run Calcs on AOPS

                        }).catch(function(reason){
                            console.error("issue encountered while fetching part_aops. | reason: " + reason);
                            aops.ready = true;
                        });


                        function save_when_ready(){
                            if(aops.ready && ops.ready) {
                                ready_count++;
                                console.log("adding to curiosity: " + part_to_analyze.value._id);
                                curiosity[part_to_analyze.value._id] = part_to_analyze.value;
                                callback(part_to_analyze.done);
                            }
                        }
                    }

                    function continue_on(done){
                        let next_part = parts.next();
                        if(ready_count >= 25 || next_part.done || !next_part.value) {
                            console.log("Saving %d part curiosities.", ready_count);
                            ready_count = 0;
                            store();
                            if(next_part.done) {
                                console.log("***********  progressing to the next batch of parts. ************");
                                start_point = (!start_point) ? config.data_packages['coeus_parts_by_part_num'].load_limit : start_point + config.data_packages['coeus_parts_by_part_num'].load_limit;
                                setTimeout(calculate_performance(start_point), 15000);
                            }
                            else setTimeout(analyze_part_performance(next_part, continue_on), 5000);
                        }
                        else
                        {
                            let delay = 1000 * read_threads;
                            setTimeout(analyze_part_performance(next_part, continue_on), delay);
                        }
                    }

                    analyze_part_performance(parts.next(), continue_on);
                }).catch(function(reason){
                    console.error("issue encountered while fetching order_manifest. | reason: " + reason);
                });
            } catch(err) {
                whenever_an_err.push(err);
                console.error(err);
            }
        };

        /* Agenda & Progress Monitor */
        let agenda = {
            active_phase: config.phase,
            phases: [ fetch_part_manifests, assemble_curiosity, calculate_performance ],

            // Phase 0 Checkpoints
            fetch_inventor_manifest: false,
            fetch_quote_manifest: false,
            fetch_order_manifest: false,
            fetch_operate_manifest: false,
            fetch_aoperate_manifest: false,

            // Phase 1 Checkpoints
            assemble_quotes_to_parts: false,
            assemble_orders_to_parts: false,

            // Phase 2 Checkpoints
            calculate_op_criteria: false,
            calculate_aop_performance: false,

            mark_progress: function(checkpoint){
                this[checkpoint] = true;
            },
            is_phase_complete: function(){
                switch(this.active_phase){
                    case 0:
                        return (this.fetch_inventor_manifest && this.fetch_quote_manifest && this.fetch_order_manifest && this.fetch_operate_manifest && this.fetch_aoperate_manifest);
                        break;

                    case 1:
                        return (this.assemble_quotes_to_parts && this.assemble_orders_to_parts);
                        break;

                    case 2:
                        return (this.calculate_op_criteria && this.calculate_aop_performance);
                        break;
                    default:
                        console.error('please include the phase to check for completion.')
                }
            }
        };

        /* Agenda Driver: Periodically checks the progress of the current phase.  Upon completion, the next phase is kicked off... through to completion. */
        let agenda_driver = setInterval(function(){
            if(agenda.is_phase_complete()){
                if(++agenda.active_phase >= agenda.phases.length) clearInterval(agenda_driver);
                else agenda.phases[config.phase]();
            }
        }, 10000);

        /* Script begins here */
        agenda.phases[config.phase]();
		break;

	default:
		console.log("please provide a valid Curiosity for Coeus to model.");
		process.exit(0);
		break;
}