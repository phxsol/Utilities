const agentkeepalive	= require('agentkeepalive');        // connection 'keep-alive' agent for nano

exports.database = {
    nano_cfg: {
        url: '[SECURE_URL_CREDENTIALS]',
        timeout: 300000,
        pool: {
            maxSockets: Infinity
        }
    },
    dbase_src_path: "D:/Workbench/Data Processing/Inbound/",
    phx_phyr: 'phx_phyr',
    models: 'phx_models',
    xfer_proc: "phx_xfer"
};

exports.curiosities = {
    coeus_parts_curiosity: 'coeus_parts'
};

exports.data_packages = {
	inventor_by_part_num: {
		db: 'inventor',
		design: 'inventor',
		view: 'all_by_part_num',
		batch_size: 10000,
		start: 0,
        reduce: false,
        include_docs: false,
		load_limit: false,
		batch_ndx: 0,
		response_total: 0
	},
	quote_by_part_num: {
		db: 'quote',
		design: 'quote',
		view: 'all_by_part_num',
		batch_size: 15000,
		start: 0,
        reduce: false,
        include_docs: false,
		load_limit: false,
		batch_ndx: 0,
		response_total: 0
	},
	order_by_part_num: {
		db: 'order',
		design: 'order',
		view: 'all_by_part_num',
		batch_size: 100000,
		start: 0,
        reduce: false,
        include_docs: false,
		load_limit: false,
		batch_ndx: 0,
		response_total: 0
	},
    operate_all_docs: {
        db: 'operate',
        design: false,
        view: '_all_docs',
        batch_size: 500,
        start: 0,
        reduce: false,
        include_docs: true,
        load_limit: false,
        batch_ndx: 0,
        response_total: 0
    },
	operate_by_quote_num: {
        db: 'operate',
        design: 'operate',
        view: 'all_by_quote_num',
		batch_size: 150000,
        start: 0,
        reduce: false,
        include_docs: false,
		load_limit: false,
		batch_ndx: 0,
        response_total: 0
	},
	aoperate_all_docs:{
        db: 'aoperate',
        design: false,
        view: '_all_docs',
        batch_size: 500,
        start: 0,
        reduce: false,
        include_docs: true,
        load_limit: false,
        batch_ndx: 0,
        response_total: 0
	},
	aoperate_by_part_num: {
        db: 'aoperate',
        design: 'aoperate',
        view: 'all_by_order_num',
		batch_size: 250000,
		start: 0,
		reduce: false,
		include_docs: false,
		load_limit: false,
        batch_ndx: 0,
        response_total: 0
	},
	coeus_parts_by_part_num: {
		db: 'coeus_parts',
		design: 'coeus_parts',
		view: 'all_by_part_num',
		batch_size: 125,
		start: 0,
        reduce: false,
        include_docs: true,
		load_limit: 125,
		batch_ndx: 0,
		response_total: 0
	}
};
