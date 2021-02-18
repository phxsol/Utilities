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
