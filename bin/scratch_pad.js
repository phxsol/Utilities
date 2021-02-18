var req = require('request');
var options = {
    url: 'http://phx:c002n68r7507@localhost:5984/aoperate/_find',
    json: true,
    body: {
        selector: {
            ao_order_num: {
                $eq: "55555-01"
            }
        }
    }
};

req.post(options, function (error, response, body) {
    if (!error && response.statusCode == 200) {
        console.log(body)
    }
});


//Load the request module

var req_options = {
    "url": "http://phx:c002n68r7507@localhost:5984/aoperate/_find",
    "headers":{
        "content-type": "application/json"
    },
    "timeout": "30000",
    "pool": {
        "maxSockets": "Infinity"
    },
    "selector": {
        "ao_order_num": {
            "$eq": "55554-01"
        }
    }
};



