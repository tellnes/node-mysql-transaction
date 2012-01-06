var MySQLPool = require("mysql-pool").MySQLPool
  , util = require("util")
  ;


function Transaction(pool, client) {
  this.pool = pool;
  this.client = client;
  client._transaction = true;
}

function makeFinishCb(that, cb) {
  return function (err) {
    that.client._transaction = false;
    that.pool._poolAvail(that.client);
    cb && cb(err);
  };
}

Transaction.prototype.commit = function(cb) {
  this.client.query('COMMIT', makeFinishCb(this, cb));
};

Transaction.prototype.rollback = function(cb) {
  this.client.query('ROLLBACK', makeFinishCb(this, cb));
};

['query', 'format', 'escape', 'ping', 'useDatabase', 'statistics'].forEach(function(key) {
  Transaction.prototype[key] = function() {
    this.client[key].apply(this.client, arguments);
  };
});

MySQLTransaction = function(properties) {
  if(!(this instanceof MySQLTransaction)) {
    return new MySQLTransaction(properties);
  }

  this._poolAvail = MySQLPool.prototype._avail.bind(this);

  MySQLPool.call(this, properties);
}
util.inherits(MySQLTransaction, MySQLPool);
exports.MySQLTransaction = MySQLTransaction;

exports.createClient = MySQLTransaction.prototype.createClient = function createClient(options) {
  return new MySQLTransaction(options);
};


MySQLTransaction.prototype._avail = function _avail(client) {
  if (!client._transaction) {
    this._poolAvail(client);
  }
};

MySQLTransaction.prototype.transaction = function(cb) {
  var client = this._idleQueue.shift();
  if(!client) {
    pool._todoQueue.push({method:wrapperMethod, args:args});
    return pool;
  }

  var trans = new Transaction(this, client);
  client.query('START TRANSACTION', function(err) {
    cb(err, trans);
  });
};
