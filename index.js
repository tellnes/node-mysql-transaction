var MySQLClient = require('mysql').Client
  , util = require('util')
  , hashish = require('hashish')
  ;


function Transaction(client) {
  this.client = client;
}

function makeFinishCb(client, cb) {
  return function (err) {
    if (cb) { cb(err); }
    client._isTransaction = false;
    client._flush();
  };
}

Transaction.prototype.commit = function(cb) {
  this.query('COMMIT', makeFinishCb(this.client, cb));
};

Transaction.prototype.rollback = function(cb) {
  this.query('ROLLBACK', makeFinishCb(this.client, cb));
};

Transaction.prototype.query = function() {
  MySQLClient.prototype.query.apply(this.client, arguments);
};

['format', 'escape', 'ping', 'useDatabase', 'statistics'].forEach(function(key) {
  Transaction.prototype[key] = function() {
    this.client[key].apply(this.client, arguments);
  };
});

function Client() {
  MySQLClient.call(this);
  this._queryQueue = [];
  this._isTransaction = false;
}
util.inherits(Client, MySQLClient);
exports.Client = Client;

exports.createClient = function createClient(config) {
  var client = new Client();
  hashish.update(client, config || {});
  return client;
};


Client.prototype._flush = function() {
  if (this._isTransaction) { return; }
  var args = this._queryQueue.shift();
  if (!args) { return; }
  MySQLClient.prototype.query.apply(this, args);
  this._flush();
};

Client.prototype.transaction = function(cb) {
  var trans = new Transaction(this);
  this.query('START TRANSACTION', function(err) {
    cb(err, trans);
  });
  this._isTransaction = true;
};

Client.prototype.query = function() {
  if (this._isTransaction) {
    this._queryQueue.push(arguments);
  } else {
    MySQLClient.prototype.query.apply(this, arguments);
  }
};
