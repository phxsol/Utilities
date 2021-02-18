"use strict";

const program = require('commander');
const pkg = require('../package.json');
const db_cfg = require('../config.js').database;

var curio, whenever_an_err = [];

program
	.command('model [curiosity]', 'Model all of the "curiosities" designed by Coeus, or a single one by "name".')
	.version(pkg.version)
	.usage('[options] <curiosity>')
	.action(function (curiosity) {
		curio = curiosity.toLowerCase();
	})
	.parse(process.argv);



/**********************************************************************************************************************/
// Complex Definitions //
/**********************************************************************************************************************/

var DataPackages = {
	PartsManifest: {
		db: 'inventor',
		design: 'inventor',
		view: 'all_by_part_num',
		batch_size: 2 * 3 * 5 * 7 * 11 * 13,
		start: 0,
		load_limit: false,
		batch_ndx: 0,
		response_total: 0,
		options: { include_docs: false, limit: DataPackages.PartsManifest.batch_size, skip: DataPackages.PartsManifest.start, reduce: false },
		response_map: map_parts_manifest
	},
	QuotesManifest: {
		db: 'quote',
		design: 'quote',
		view: 'all_by_part_num',
		batch_size: 2 * 3 * 5 * 7 * 11 * 13,
		start: 0,
		load_limit: false,
		batch_ndx: 0,
		response_total: 0,
		response_map: map_quotes_manifest
	},
	OrdersManifest: {
		db: 'order',
		design: 'order',
		view: 'all_by_part_num',
		batch_size: 4000,
		start: 0,
		load_limit: false,
		batch_ndx: 0,
		response_total: 0,
		response_map: map_orders_manifest
	},
	PartsCuriosity: {
		db: 'coeus_parts',
		design: 'coeus_parts',
		view: 'all_by_part_num',
		batch_size: 2000,
		start: 0,
		load_limit: false,
		batch_ndx: 0,
		response_total: 0,
		response_map: map_docs_to_curiosity
	},
	OpsManifest: {
        db: 'operate',
        design: 'operate',
        view: 'all_by_quote_num',
		batch_size: 1000,
        start: 0,
		load_limit: false,
		batch_ndx: 0,
        response_total: 0,
		response_map: map_ops_manifest
	},
	AOpsManifest: {
        db: 'aoperate',
        design: 'aoperate',
        view: 'all_by_order_num',
		batch_size: 1000,
		start: 0,
		load_limit: 50000,
        batch_ndx: 0,
        response_total: 0,
		response_map: map_aops_manifest
	}
};

var QuotesManifest = [];
var PartsManifest = [];
var OrdersManifest = [];
var OpsManifest = [];
var AOpsManifest = [];
var Quotes_Untethered = [];
var Orders_Untethered = [];
var Ops_Untethered = [];
var AOps_Untethered = [];
var quote_to_part_index = {};
var order_to_part_index = {};
var ActiveCuriosity = {};

var CuriosityFactory = {
	Parts:{
		Assemble: function(phase, callback){
			switch(phase) {
				case 'parts_from_manifest':
					var part = PartsManifest.pop();
					while (part) {
						ActiveCuriosity[part.iv_part_num] = part;
						part = null;
						part = PartsManifest.pop();
					}
					PartsManifest = null;
					callback();
					break;

				case 'quotes_to_parts':
					var quote = QuotesManifest.pop();
					while(quote){
						if(ActiveCuriosity[quote.q1_part_num]) ActiveCuriosity[quote.q1_part_num].quotes.push(quote);
						else //Quotes_Untethered.push(quote);
						quote = null;
						quote = QuotesManifest.pop();
					}
					QuotesManifest = null;
					callback();
					break;

				case 'orders_to_parts':
					var order = OrdersManifest.pop();
					while(order){
						if(ActiveCuriosity[order.or_part_num]) ActiveCuriosity[order.or_part_num].orders.push(order);
						else //Orders_Untethered.push(order);
						order = null;
						order = OrdersManifest.pop();
					}
					OrdersManifest = null;
					callback();
					break;

				case 'part_indices':
					for(var invent_num in ActiveCuriosity){
						if(ActiveCuriosity.hasOwnProperty(invent_num)){
							var part = ActiveCuriosity[invent_num];
							for(var quote_ndx in part.quotes){
								var quote_num = part.quotes[quote_ndx].q1_quote_num;
								quote_to_part_index[quote_num] = part.invent_num;
							}
							for(var order_ndx in part.orders){
								var order_num = part.orders[order_ndx].order_num;
								order_to_part_index[order_num] = part.invent_num;
							}
						}
					}
					callback();
					break;

				case 'ops_to_parts':
                    var op = OpsManifest.pop();
					while(op){
                        if(quote_to_part_index[op.op_quote_num]){
							var part_num = quote_to_part_index[op.op_quote_num];
							ActiveCuriosity[part_num].ops.push(op);
						} //else Ops_Untethered.push(op);
						op = null;
                        op = OpsManifest.pop();
                    }
                    OpsManifest = null;
                    callback();
					break;

				case 'aops_to_parts':
					var aop = AOpsManifest.pop();
					while(aop){
						if(order_to_part_index[aop.aop_order_num]){
							var part_num = order_to_part_index[aop.aop_order_num];
							ActiveCuriosity[part_num].aops.push(aop);
						} //else AOps_Untethered.push(aop);
						aop = null;
						aop = AOpsManifest.pop();
					}
					AOpsManifest = null;
					callback();
					break;
			}
		}
	}
};



/**********************************************************************************************************************/
// Routines //
/**********************************************************************************************************************/

function proc_init(curio) {
	try {
		switch(curio){
			case 'curiosities':

				break;

			case 'parts':			
				var order_batch_callback = function(pOrdersManifest, is_last_batch){
					console.log("Assembling Part Orders");
					OrdersManifest = pOrdersManifest;
					CuriosityFactory.Parts.Assemble('orders_to_parts',function() {
						if(!is_last_batch){
							extract(DataPackages['OrdersManifest'], false, order_batch_callback);
						} else {
							console.log("Storing Parts Curiosity  -- Phase One --");
							store('parts');
						}
					});
				};			
			
				console.log("Extracting PartsManifest");
				extract(DataPackages['PartsManifest'], true, function(pPartsManifest, b){
					console.log("Assembling Parts");
					PartsManifest = pPartsManifest;
					CuriosityFactory.Parts.Assemble('parts_from_manifest',function(){
                        console.log("Extracting QuotesManifest");
						extract(DataPackages['QuotesManifest'], true, function(pQuotesManifest, b){
							console.log("Assembling Part Quotes");
							QuotesManifest = pQuotesManifest;
							CuriosityFactory.Parts.Assemble('quotes_to_parts',function() {
								console.log("Extracting OrdersManifest");
								extract(DataPackages['OrdersManifest'], false, order_batch_callback);
							});
						});
					});
				});
				break;

			case 'partops':				
				var op_batch_callback = function(pOpsManifest, is_last_batch){
					console.log("Assembling Part Ops");
					OpsManifest = pOpsManifest;
					CuriosityFactory.Parts.Assemble('ops_to_parts',function() {
						if(!is_last_batch){
							extract(DataPackages['OpsManifest'], false, op_batch_callback);
						} else {
							console.log("Storing Parts Curiosity  -- Phase Two --");
							store('parts');
						}
					});
				};

				console.log("Extracting Parts Curiosity  -- Phase Two --");
				extract(DataPackages['PartsCuriosity'], true, function(a, b){
					a = null;
					console.log("Assembling Part Indices");
					CuriosityFactory.Parts.Assemble('part_indices',function() {
						console.log("Extracting OpsManifest");
						extract(DataPackages['OpsManifest'], false, op_batch_callback);						
					});
				});
				break;

			case 'partaops':
				var aop_batch_callback = function(pAOpsManifest, is_last_batch){
					console.log("Assembling Part AOps");
					AOpsManifest = pAOpsManifest;
					CuriosityFactory.Parts.Assemble('aops_to_parts',function() {
						if(!is_last_batch){
							extract(DataPackages['AOpsManifest'], false, aop_batch_callback);
						} else {
							console.log("Storing Parts Curiosity  -- Phase Three --");
							store('parts');
						}
					});
				};

				console.log("Extracting Parts Curiosity  -- Phase Three --");
				extract(DataPackages['PartsCuriosity'], true, function(a, b){
					a = null;
					console.log("Assembling Part Indices");
					CuriosityFactory.Parts.Assemble('part_indices',function() {
						console.log("Extracting AOpsManifest");
						extract(DataPackages['AOpsManifest'], false, aop_batch_callback);
					});
				});
				break;
		}
	} catch (err) {
		console.error(err);
		process.exit(1);
	}
}

// extract() pulls the entirety of a docDB's view results - using the DesignDoc/View in couchDB.  This self batches, so will scale to any currently foreseeable result sizes.
// => return_all determines whether to return the entire view results, or a single batch at a time... Either way overall batch progress is tracked globally.
function extract(perspective, return_all, callback){
	var manifest = [];
	var total_rows = 0;	// important value here.
	var nano = require('nano')(db_cfg.nano_cfg);
	var SourceDB = nano.use(perspective.db);
	var batch_errors = 0;

	// This is the callback used by the initial and subsequent (batched) requests.
	var batch_callback = function (err, response) {
		if (err) {
			batch_errors++;
			whenever_an_err.push(err);
			console.error(err);
			process.exit(1);	// TODO: Handle and remove hard exit.
		} else {
			total_rows = response.total_rows;
			manifest = manifest.concat(response.rows.map(perspective.response_map));
			var extracted_so_far = perspective.batch_size * perspective.batch_ndx;
			var items_remaining = (perspective.load_limit) ? perspective.load_limit - extracted_so_far : total_rows - extracted_so_far;
			var less_than_a_batch = (items_remaining < perspective.batch_size);
			var items_this_go = (less_than_a_batch) ? items_remaining : perspective.batch_size;
			console.log("%d manifest items  |  Extracting %d more now --> %d items remain", total_rows, items_this_go, items_remaining - items_this_go);
			console.log("Batch Extraction Complete: " + batch_errors + " errors.");
			var rows_to_skip = perspective.start + (++perspective.batch_ndx * perspective.batch_size);
			var is_last_batch = (perspective.load_limit) ? (rows_to_skip >= perspective.start + perspective.load_limit || rows_to_skip >= total_rows) : (rows_to_skip >= total_rows);
			if(return_all && !is_last_batch) {
				perspective.options.skip = rows_to_skip;
				SourceDB.view(perspective.design, perspective.view, perspective.options, batch_callback);
			} else {
				callback(manifest, is_last_batch);
			}
		}
	};

	// First batch is requested here.
	//var options = { include_docs: false, limit: perspective.batch_size, skip: perspective.start, reduce: false };
	SourceDB.view(perspective.design, perspective.view, perspective.options, batch_callback);
}

// store() places the generated ActiveCuriosity into the corresponding couchDB repository.  Large sets are broken into batches automatically.
function store(curio){
	switch(curio) {
		case 'parts':
			var batch_limit = 10;
			var batch = null;
			
			// This is the callback used by the initial and subsequent (batched) requests.
			var batch_callback = function (err, response) {
				if (err) {
					whenever_an_err.push(err);
					console.error(err);
				}
				if(response) console.log("Parts batch stored.");
				for(var part_ndx in ActiveCuriosity){
					if(ActiveCuriosity.hasOwnProperty(part_ndx)){
						if(!batch) batch = [];
						batch.push(ActiveCuriosity[part_ndx]);
						delete ActiveCuriosity[part_ndx];
						if(batch.length == batch_limit) {
							var nano = require('nano')(db_cfg.nano_cfg);
							var modelDB = nano.use(DataPackages.PartsCuriosity.db);
							modelDB.bulk({docs:batch}, batch_callback);
							batch = null;
							break;
						}
					}
				}
				if(batch){
					var nano = require('nano')(db_cfg.nano_cfg);
					var modelDB = nano.use(DataPackages.PartsCuriosity.db);
					modelDB.bulk({docs:batch}, batch_callback);
					batch = null;
				}
			};

			batch_callback(null, null);
			break;
	}
}



/**********************************************************************************************************************/
// Sub-Routines //
/**********************************************************************************************************************/

function map_parts_manifest(inventor_doc){
	// Clean-up null and empty values
	if(!inventor_doc.doc.iv_part_num) inventor_doc.doc.iv_part_num = "N/A";
	inventor_doc.doc.quotes = [];
	inventor_doc.doc.orders = [];
	inventor_doc.doc.ops = [];
	inventor_doc.doc.aops = [];
	delete inventor_doc.doc._rev;
	delete inventor_doc.doc._id;
	delete inventor_doc.doc.rev;
	delete inventor_doc.doc.id;
	return inventor_doc.doc;
}

function map_quotes_manifest(quote_doc){
	if(!quote_doc.doc.q1_part_num) quote_doc.doc.q1_part_num = "N/A";
	delete quote_doc.doc._rev;
	delete quote_doc.doc._id;
	delete quote_doc.doc.rev;
	delete quote_doc.doc.id;
	return quote_doc.doc;
}

function map_orders_manifest(order_doc){
	if(!order_doc.doc.or_part_num) order_doc.doc.or_part_num = "N/A";
	delete order_doc.doc._rev;
	delete order_doc.doc._id;
	delete order_doc.doc.rev;
	delete order_doc.doc.id;
	return order_doc.doc;
}

function map_docs_to_curiosity(part_doc){
	ActiveCuriosity[part_doc.doc.iv_part_num] = part_doc.doc;
}

function map_ops_manifest (ops_doc){
	if(!ops_doc.doc.op_quote_num) ops_doc.doc.op_quote_num = "N/A";
	delete ops_doc.doc._rev;
	delete ops_doc.doc._id;
	delete ops_doc.doc.rev;
	delete ops_doc.doc.id;
	return ops_doc.doc;
}

function map_aops_manifest(aops_doc){
	if(!aops_doc.doc.aop_order_num) aops_doc.doc.aop_order_num = "N/A";
	delete aops_doc.doc._rev;
	delete aops_doc.doc._id;
	delete aops_doc.doc.rev;
	delete aops_doc.doc.id;
	return aops_doc.doc;
}



/**********************************************************************************************************************/
// Main Script Entry //
/**********************************************************************************************************************/

proc_init(curio);