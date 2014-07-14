var RiakConnector = require('./RiakConnector');

function RiakHttp(rootUrl){
  this.connect = new RiakConnector(rootUrl);
}

RiakHttp.prototype.getBuckets = function getBuckets(cb){
  this.connect.url('/buckets?buckets=true').run(cb);
};

RiakHttp.prototype.getBucketProperties = function getBucketProperties(bucket, cb){
  this.connect.url('/buckets/'+bucket+'/props').run(cb);
};

RiakHttp.prototype.getKeys = function getKeys(bucket, cb){
  this.connect.url('/buckets/'+bucket+'/keys?keys=true').run(function(err, arr){
    cb(err, (arr||{}).keys);
  });
};

// it clears if there is no sublings
// otherwise use curl:
// curl -v -XDELETE http://localhost:8098/buckets/test/keys/{{our_key}}
RiakHttp.prototype.clearBucket = function clearBucket(bucket, cb){
  var _this = this;
  _this.getKeys(bucket, function(e, keys){
    if(e) return cb(e);

    var count = keys.length;
    var success = 0;
    for(var i in keys){
      _this.deleteObject(bucket, keys[i], function(err){
        if(!err) success++;
        if(--count==0){
          cb(null, success);
        }
      })
    }
  });
};

RiakHttp.prototype.fetchObject = function(bucket, key, cb){
  this.connect.url('/buckets/'+bucket+'/keys/'+key).run(cb);
};

RiakHttp.prototype.storeObject = function storeObject(bucket, obj, cb){
  var self = this;

  var conn = self.connect.url('/buckets/'+bucket+'/keys/?returnbody=true');
  conn.method('POST')
    .json(obj)
    .run(function(err){
      if(err) return cb(err);
      obj.id = conn.response.headers.location.split('/').splice(-1, 1)[0];
      var vclock = conn.response.headers['x-riak-vclock'];

      self.connect.url('/buckets/'+bucket+'/keys/'+obj.id+'?returnbody=true')
        .method('PUT')
        .json(obj)
        .header('X-Riak-Vclock', vclock)
        .run(cb);
    });
};

// mention that deleting is possible when it was stored via PUT
// also object deletes after some time passes
RiakHttp.prototype.deleteObject = function(bucket, key, cb){
  this.connect.url('/buckets/'+bucket+'/keys/'+key)
    .method('DELETE')
    .run(cb);
};

RiakHttp.prototype.updateObject = function(bucket, key, param, val, cb){
  var self = this;

  var conn = self.connect.url('/buckets/'+bucket+'/keys/'+key)
  conn.run(function(err, obj){
    if(err) return cb(err);

    var vclock = conn.response.headers['x-riak-vclock'];

    obj[param] = val;

    self.connect.url('/buckets/'+bucket+'/keys/'+obj.id+'?returnbody=true')
      .method('PUT')
      .json(obj)
      .header('X-Riak-Vclock', vclock)
      .run(cb);
  });
};

// http://docs.basho.com/riak/2.0.0beta1/dev/using/mapreduce/
RiakHttp.prototype.mapReduse = function(bucket, selectFuncAsStr, cb){
  //console.log( selectFuncAsStr );
  this.connect.url('/mapred')
    .method('POST')
    .json({
      "inputs": bucket,
      "query": [{
        "map": {
          "language":"javascript",
          "name":"Riak.mapValuesJson",
          "source": selectFuncAsStr || (function(riakObject) {
            var val = riakObject.values[0].data.match(/pizza/g);
            return [
              [riakObject.key, (val ? val.length : 0 )]
            ]}).toString()
        }
      }]
    })
    .run(cb);
};

RiakHttp.prototype.findByKeyValue = function findByKeyValue(bucket, key, val, cb){
  this.mapReduse(
    bucket,
    (function(riakObject){
      var obj = riakObject.values[0].data;
      obj = JSON.parse(obj);
      if(obj["$key"]=="$val"){
        return [ obj ];
      }

      return [];
    }).toString().replace(/\$key/, key).replace(/\$val/, val),
    cb
  );
};

module.exports = RiakHttp;