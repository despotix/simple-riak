var request = require('request');

function RiakConnector(rootUrl) {
  this.rootUrl = rootUrl;
  this.params = {
    url : rootUrl,
    json: {}
  };
}

RiakConnector.prototype.url = function (url) {
  this.params.url = this.rootUrl + url;
  return this;
};

RiakConnector.prototype.method = function (method) {
  this.params.method = method;
  return this;
};

RiakConnector.prototype.json = function (json) {
  this.params.json = json;
  return this;
};

RiakConnector.prototype.header = function (name, val) {
  if (!val) {
    name = name.split(/\s*:\s*/);
    val = name[1];
    name = name[0];
  }
  if(!this.params.headers){
    this.params.headers = {};
  }
  this.params.headers[name] = val;
  return this;
};

RiakConnector.prototype.run = function run(callback) {
  var _this = this;

  request(this.params, function (error, response, body) {
    // todo proccess response statusCodes according to documentation
    //console.log(req);
    //console.log(response.statusCode);
    //console.log(response);

    _this.response = response;

    if (typeof body === 'string') {
      error = body;
      body = null;
    }

    if (callback) callback(error, body);
  });
};

module.exports = RiakConnector;