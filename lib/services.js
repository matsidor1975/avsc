/* jshint node: true */

// TODO: Improve serialization error messages.

'use strict';

/** This module implements Avro's IPC/RPC logic. */

var types = require('./types'),
    utils = require('./utils'),
    events = require('events'),
    stream = require('stream'),
    util = require('util');

// A few convenience imports.
var Tap = utils.Tap;
var Type = types.Type;
var debug = util.debuglog('avsc:services');
var f = util.format;

// Namespace used for all internal types declared here.
var NAMESPACE = 'org.apache.avro.ipc';

// Various useful types. We instantiate options once, to share the registry.
var typeOpts = {namespace: NAMESPACE};
var BOOLEAN_TYPE = Type.forSchema('boolean', typeOpts);
var MAP_BYTES_TYPE = Type.forSchema({type: 'map', values: 'bytes'}, typeOpts);
var STRING_TYPE = Type.forSchema('string', typeOpts);
var HANDSHAKE_REQUEST_TYPE = Type.forSchema({
  name: 'HandshakeRequest',
  type: 'record',
  fields: [
    {name: 'clientHash', type: {name: 'MD5', type: 'fixed', size: 16}},
    {name: 'clientProtocol', type: ['null', 'string'], 'default': null},
    {name: 'serverHash', type: 'MD5'},
    {name: 'meta', type: ['null', MAP_BYTES_TYPE], 'default': null}
  ]
}, typeOpts);
var HANDSHAKE_RESPONSE_TYPE = Type.forSchema({
  name: 'HandshakeResponse',
  type: 'record',
  fields: [
    {
      name: 'match',
      type: {
        name: 'HandshakeMatch',
        type: 'enum',
        symbols: ['BOTH', 'CLIENT', 'NONE']
      }
    },
    {name: 'serverProtocol', type: ['null', 'string'], 'default': null},
    {name: 'serverHash', type: ['null', 'MD5'], 'default': null},
    {name: 'meta', type: ['null', MAP_BYTES_TYPE], 'default': null}
  ]
}, typeOpts);
var PACKET_TYPE = Type.forSchema({
  name: 'Packet',
  type: 'record',
  fields: [
    {name: 'id', type: 'long'},
    {
      name: 'handshake',
      type: ['null', HANDSHAKE_REQUEST_TYPE, HANDSHAKE_RESPONSE_TYPE],
      'default': null
    },
    {name: 'headers', type: MAP_BYTES_TYPE, 'default': {}},
    {name: 'body', type: 'bytes', 'default': '\u0000\u0000'}
  ]
}, typeOpts);

var Packet = PACKET_TYPE.recordConstructor;

// Internal message, used to check protocol compatibility.
var PING_MESSAGE = new Message(
  '', // Empty name (invalid for other "normal" messages).
  Type.forSchema({name: 'PingRequest', type: 'record', fields: []}, typeOpts),
  Type.forSchema(['string'], typeOpts),
  Type.forSchema('null', typeOpts)
);

// Prefix used to differentiate between messages when sharing a stream. This
// length should be smaller than 16. The remainder is used for disambiguating
// between concurrent messages (the current value, 16, therefore supports ~64k
// concurrent messages).
var PREFIX_LENGTH = 16;

/** An Avro message, containing its request, response, etc. */
function Message(name, reqType, errType, resType, oneWay, doc) {
  this.name = name;
  if (!Type.isType(reqType, 'record')) {
    throw new Error('invalid request type');
  }
  this.requestType = reqType;
  if (
    !Type.isType(errType, 'union') ||
    !Type.isType(errType.types[0], 'string')
  ) {
    throw new Error('invalid error type');
  }
  this.errorType = errType;
  if (oneWay) {
    if (!Type.isType(resType, 'null') || errType.types.length > 1) {
      throw new Error('inapplicable one-way parameter');
    }
  }
  this.responseType = resType;
  this.oneWay = !!oneWay;
  this.doc = doc !== undefined ? '' + doc : undefined;
  Object.freeze(this);
}

Message.forSchema = function (name, schema, opts) {
  opts = opts || {};
  if (!types.isValidName(name)) {
    throw new Error(f('invalid message name: %s', name));
  }
  // We use a record with a placeholder name here (the user might have set
  // `noAnonymousTypes`, so we can't use an anonymous one). We remove it from
  // the registry afterwards to avoid exposing it outside.
  if (!Array.isArray(schema.request)) {
    throw new Error(f('invalid message request: %s', name));
  }
  var recordName = f('%s.%sRequest', NAMESPACE, utils.capitalize(name));
  var reqType = Type.forSchema({
    name: recordName,
    type: 'record',
    namespace: opts.namespace || '', // Don't leak request namespace.
    fields: schema.request
  }, opts);
  delete opts.registry[recordName];
  if (!schema.response) {
    throw new Error(f('invalid message response: %s', name));
  }
  var resType = Type.forSchema(schema.response, opts);
  if (schema.errors !== undefined && !Array.isArray(schema.errors)) {
    throw new Error(f('invalid message errors: %s', name));
  }
  var errType = Type.forSchema(['string'].concat(schema.errors || []), opts);
  var oneWay = !!schema['one-way'];
  return new Message(name, reqType, errType, resType, oneWay, schema.doc);
};

Message.prototype.schema = Type.prototype.schema;

Message.prototype._attrs = function (opts) {
  var reqSchema = this.requestType._attrs(opts);
  var schema = {
    request: reqSchema.fields,
    response: this.responseType._attrs(opts)
  };
  var msgDoc = this.doc;
  if (msgDoc !== undefined) {
    schema.doc = msgDoc;
  }
  var errSchema = this.errorType._attrs(opts);
  if (errSchema.length > 1) {
    schema.errors = errSchema.slice(1);
  }
  if (this.oneWay) {
    schema['one-way'] = true;
  }
  return schema;
};

/**
 * An Avro RPC service.
 *
 * This constructor shouldn't be called directly, but via the
 * `Service.forProtocol` method. This function performs little logic to better
 * support efficient copy.
 */
function Service(name, messages, types, ptcl) {
  if (typeof name != 'string') {
    // Let's be helpful in case this class is instantiated directly.
    return Service.forProtocol(name, messages);
  }

  this.name = name;
  this._messagesByName = messages || {};
  this.messages = Object.freeze(utils.objectValues(this._messagesByName));

  this._typesByName = types || {};
  this.types = Object.freeze(utils.objectValues(this._typesByName));

  this.protocol = ptcl;
  // We cache a string rather than a buffer to not retain an entire slab.
  this._hashStr = utils.getHash(JSON.stringify(ptcl)).toString('binary');
  this.doc = ptcl.doc ? '' + ptcl.doc : undefined;

  Object.freeze(this);
}

Service.Client = Client;
Service.Server = Server;
Service.HANDSHAKE_REQUEST_TYPE = HANDSHAKE_REQUEST_TYPE;
Service.HANDSHAKE_RESPONSE_TYPE = HANDSHAKE_RESPONSE_TYPE;
Service.PACKET_TYPE = PACKET_TYPE;

Service.compatible = function (clientSvc, serverSvc) {
  try {
    createReaders(clientSvc, serverSvc);
  } catch (err) {
    return false;
  }
  return true;
};

Service.forProtocol = function (ptcl, opts) {
  opts = opts || {};

  var name = ptcl.protocol;
  if (!name) {
    throw new Error('missing protocol name');
  }
  if (ptcl.namespace !== undefined) {
    opts.namespace = ptcl.namespace;
  } else {
    var match = /^(.*)\.[^.]+$/.exec(name);
    if (match) {
      opts.namespace = match[1];
    }
  }
  name = types.qualify(name, opts.namespace);

  if (ptcl.types) {
    ptcl.types.forEach(function (obj) { Type.forSchema(obj, opts); });
  }
  var msgs;
  if (ptcl.messages) {
    msgs = {};
    Object.keys(ptcl.messages).forEach(function (key) {
      msgs[key] = Message.forSchema(key, ptcl.messages[key], opts);
    });
  }

  return new Service(name, msgs, opts.registry, ptcl);
};

Service.isService = function (any) {
  // Not fool-proof but likely sufficient.
  return !!any && any.hasOwnProperty('_hashStr');
};

Service.prototype.createClient = function (opts) {
  var client = new Client(this, opts);
  if (opts && opts.server) {
    // Convenience in-memory client. This can be useful to make requests
    // relatively efficiently to an in-process server. Note that it is still
    // is less efficient than direct method calls (because of the
    // serialization, which does provide "type-safety" though).
    var inMemoryOpts = {objectMode: true, noPing: true};
    var clientChannel;
    var serverChannel = opts.server.createChannel(
      function (handler) {
        clientChannel = client.createChannel(handler, inMemoryOpts);
       },
      inMemoryOpts
    );
    destroyTogether([clientChannel, serverChannel]);
  } else if (opts && opts.transport) {
    // Convenience functionality for the common single channel use-case: we
    // add a single channel using default options to the client.
    process.nextTick(function () {
      // We delay this processing such that we can attach handlers to the client
      // before any channels get created.
      client.createChannel(opts.transport);
    });
  }
  return client;
};

Service.prototype.createServer = function (opts) {
  return new Server(this, opts);
};

Object.defineProperty(Service.prototype, 'hash', {
  enumerable: true,
  get: function () { return new Buffer(this._hashStr, 'binary'); }
});

Service.prototype.message = function (name) {
  return this._messagesByName[name];
};

Service.prototype.type = function (name) {
  return this._typesByName[name];
};

Service.prototype.inspect = function () {
  return f('<Service %j>', this.name);
};

/** Function to retrieve a remote service's protocol. */
function discoverProtocol(transport, opts, cb) {
  if (cb === undefined && typeof opts == 'function') {
    cb = opts;
    opts = undefined;
  }
  opts = opts || {};
  var svc = new Service({protocol: 'Empty'}, {namespace: NAMESPACE});
  svc.createClient()
    .createChannel(transport, {
      codec: opts.codec,
      connectionTimeout: opts.timeout,
      scope: opts.scope,
      endWritable: typeof transport == 'function' // Stateless transports only.
    }).once('handshake', function (hreq, hres) {
        cb(null, JSON.parse(hres.serverProtocol));
        this.destroy({noWait: true});
      })
      .on('error', function (err) {
        cb(err); // Likely timeout.
        this.destroy({noWait: true});
      });
}

/** Load-balanced message sender. */
function Client(svc, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  // We have to suffix all client properties to be safe, since the message
  // names aren't prefixed with clients (unlike servers).
  this._svc$ = svc;
  this._channels$ = []; // Active channels.
  this._fns$ = []; // Middleware functions.

  this._cache$ = {};
  this._defaultTimeout$ = utils.getOption(opts, 'defaultTimeout', 0);
  this._policy$ = opts.channelPolicy;
  this._strict$ = !!opts.strictTypes;
  this._tagTypes$ = opts.tagTypes || {};

  if (opts.remoteProtocols) {
    insertRemoteProtocols(this._cache$, opts.remoteProtocols, svc, true);
  }

  this._svc$.messages.forEach(function (msg) {
    this[msg.name] = this._createMessageHandler$(msg);
  }, this);
}
util.inherits(Client, events.EventEmitter);

Client.prototype.activeChannels = function () {
  return this._channels$.slice();
};

Client.prototype.createChannel = function (transport, opts) {
  var newCodec = opts && opts.codec;
  if (typeof newCodec == 'string') {
    switch (newCodec) {
      case 'netty':
        newCodec = function () { return nettyCodec(HANDSHAKE_RESPONSE_TYPE); };
        break;
      case 'frame':
        newCodec = function () { return frameCodec(HANDSHAKE_RESPONSE_TYPE); };
        break;
      default:
        throw new Error(f('unsupported client codec: %s', opts.codec));
    }
  }
  var channel;
  if (typeof transport == 'function') {
    var callbackFactory;
    if (!newCodec) {
      callbackFactory = transport;
    } else {
      callbackFactory = function (reqPkt, cb) {
        var codec = newCodec();
        var writable = transport.call(this, function (err, readable) {
          if (err) {
            cb(err);
            return;
          }
          onlyElement(readable.pipe(codec.decoder), function (err, resPkt) {
            if (err) {
              cb(err);
              return;
            }
            cb(null, resPkt);
          });
        });
        if (writable) {
          codec.encoder.pipe(writable);
          codec.encoder.end(reqPkt);
        } else {
          cb(new Error('no writable transport'));
        }
      };
    }
    channel = new StatelessClientChannel(this, callbackFactory, opts);
  } else {
    var readable, writable;
    if (isStream(transport)) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    var codec;
    if (newCodec) {
      codec = newCodec();
      readable = readable.pipe(codec.decoder);
      codec.encoder.pipe(writable);
      writable = codec.encoder;
    }
    channel = new StatefulClientChannel(this, readable, writable, opts);
    if (codec) {
      // Since we never expose the automatically created encoder and decoder,
      // we release them ourselves here when the channel ends. (Unlike for
      // stateless channels, it is conceivable for the underlying stream to be
      // reused afterwards).
      channel.once('eot', function () {
        readable.unpipe(codec.decoder);
        codec.encoder.unpipe(writable);
      });
      // We also forward any (trailing data) error.
      codec.decoder.once('error', function (err) {
        channel.emit('error', err);
      });
    }
  }
  var channels = this._channels$;
  channels.push(channel);
  channel.once('drain', function () {
    // Remove the channel from the list of active ones.
    channels.splice(channels.indexOf(this), 1);
  });
  this.emit('channel', channel);
  return channel;
};

Client.prototype.destroyChannels = function (opts) {
  this._channels$.forEach(function (channel) {
    channel.destroy(opts);
  });
};

Client.prototype.emitMessage = function (name, req, ctx, cb) {
  var msg = getExistingMessage(this._svc$, name);
  var wreq = new WrappedRequest(msg, {}, req);
  this._emitMessage$(wreq, ctx, cb);
};

Client.prototype.remoteProtocols = function () {
  return getRemoteProtocols(this._cache$, true);
};

Object.defineProperty(Client.prototype, 'service', {
  enumerable: true,
  get: function () { return this._svc$; }
});

Object.defineProperty(Client.prototype, 'tagTypes', {
  enumerable: true,
  get: function () { return this._tagTypes$; }
});

Client.prototype.call = function (/* fn, [args ...] */) {
  var l = arguments.length;
  if (l === 0) {
    throw new Error('expected a function');
  }
  var args = [];
  var i;
  for (i = 0; i < l; i++) {
    args.push(arguments[i]);
  }
  var fn = args[0];
  args[0] = this;
  fn.apply(undefined, args);
  return this;
};

Client.prototype.use = function (/* fn ... */) {
  var i, l;
  for (i = 0, l = arguments.length; i < l; i++) {
    this._fns$.push(arguments[i]);
  }
  return this;
};

Client.prototype._emitMessage$ = function (wreq, ctx, cb) {
  // Common logic between `client.emitMessage` and the "named" message methods.
  if (!cb && typeof ctx === 'function') {
    cb = ctx;
    ctx = undefined;
  }
  ctx = utils.copyOwnProperties(ctx, {message: wreq._msg});
  if (ctx.timeout === undefined) {
    ctx.timeout = this._defaultTimeout$;
  }
  if (ctx.deadline === undefined) {
    if (ctx.timeout > 0) {
      ctx.deadline = Date.now() + ctx.timeout;
    }
  }
  var self = this;
  var channels = this._channels$;
  var msg = wreq._msg;
  var numChannels = channels.length;
  var channel;
  if (ctx.channel) {
    channel = ctx.channel;
  } else if (this._policy$) {
    channel = this._policy$(this._channels$.slice());
    if (!channel) {
      debug('policy returned no channel, skipping call');
      return;
    }
  } else if (numChannels === 1) {
    // Common case, optimized away.
    channel = channels[0];
  } else {
    // Random selection, cheap and likely good enough for most use-cases.
    channel = channels[Math.floor(Math.random() * numChannels)];
  }
  if (!channel) {
    var err = new Error('no available channels');
    process.nextTick(function () {
      if (cb) {
        cb.call(ctx, err); // No channel in the context.
      } else {
        self.emit('error', err);
      }
    });
    return;
  }
  if (!~channels.indexOf(channel)) {
    process.nextTick(function () { cb(new Error('invalid channel')); });
    return;
  }
  ctx.channel = channel;
  channel._emit(wreq, new WrappedResponse(msg), ctx, function (err, wres) {
    var errType = ctx.message.errorType;
    if (err) {
      // System error, likely the message wasn't sent (or an error occurred
      // while decoding the response).
      if (self._strict$) {
        err = errType.clone(err.message, {wrapUnions: true});
      }
      done(err);
      return;
    }
    if (!wres) {
      // This is a one way message in a stateful channel, so we won't be
      // receiving a reply.
      done();
      return;
    }
    // Message transmission succeeded, we transmit the message data; massaging
    // any error strings into actual `Error` objects in non-strict mode.
    err = wres.error;
    if (!self._strict$) {
      // Try to coerce an eventual error into more idiomatic JavaScript types:
      // `undefined` becomes `null` and a remote string "system" error is
      // wrapped inside an actual `Error` object.
      if (err === undefined) {
        err = null;
      } else {
        if (Type.isType(errType, 'union:unwrapped')) {
          if (typeof err == 'string') {
            err = new Error(err);
          }
        } else if (err && err.string) { // Wrapped union.
          err = new Error(err.string);
        }
      }
    }
    done(err, wres.response);

    function done(err, res) {
      if (cb) {
        cb.call(ctx, err, res);
      } else if (err) {
        self.emit('error', err);
      }
    }
  });
};

Client.prototype._createMessageHandler$ = function (msg) {
  // jshint -W054
  var fields = msg.requestType.fields;
  var names = fields.map(function (f) { return f.name; });
  var body = 'return function ' + msg.name + '(';
  if (names.length) {
    body += names.join(', ') + ', ';
  }
  body += 'opts, cb) {\n';
  body += '  var req = {';
  body += names.map(function (n) { return n + ': ' + n; }).join(', ');
  body += '};\n';
  body += '  return this.emitMessage(\'' + msg.name + '\', req, opts, cb);\n';
  body += '};';
  return (new Function(body))();
};

/** Message receiver. */
function Server(svc, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this.service = svc;
  this._handlers = {};
  this._fns = []; // Middleware functions.
  this._channels = {}; // Active channels.
  this._nextChannelId = 1;

  this._cache = {};
  this._defaultHandler = opts.defaultHandler;
  this._sysErrFormatter = opts.systemErrorFormatter;
  this._silent = !!opts.silent;
  this._strict = !!opts.strictTypes;
  this.tagTypes = opts.tagTypes || {};

  if (opts.remoteProtocols) {
    insertRemoteProtocols(this._cache, opts.remoteProtocols, svc, false);
  }

  svc.messages.forEach(function (msg) {
    var name = msg.name;
    if (!opts.noCapitalize) {
      name = utils.capitalize(name);
    }
    this['on' + name] = this._createMessageHandler(msg);
  }, this);
}
util.inherits(Server, events.EventEmitter);

Server.prototype.activeChannels = function () {
  return utils.objectValues(this._channels);
};

Server.prototype.createChannel = function (transport, opts) {
  var newCodec = opts && opts.codec;
  if (typeof newCodec == 'string') {
    switch (newCodec) {
      case 'netty':
        newCodec = function () { return nettyCodec(HANDSHAKE_REQUEST_TYPE); };
        break;
      case 'frame':
        newCodec = function () { return frameCodec(HANDSHAKE_REQUEST_TYPE); };
        break;
      default:
        throw new Error(f('unsupported server codec: %s', opts.codec));
    }
  }
  var channel;
  if (typeof transport == 'function') {
    var callbackFactory;
    if (!newCodec) {
      callbackFactory = transport;
    } else {
      callbackFactory = function (fn) {
        var codec = newCodec();
        var encoder, pkt;
        var readable = transport.call(this, function (err, writable) {
          if (err) {
            debug('writable transport failed (%s)', err);
            channel.emit('error', err);
            return;
          }
          debug('got writable transport');
          codec.encoder.pipe(writable);
          encoder = codec.encoder;
          step();
        });
        debug('got readable transport');
        onlyElement(readable.pipe(codec.decoder), function (err, reqPkt) {
          if (err) {
            debug('decoder transport failed (%s)', err);
            channel.emit('error', err);
            return;
          }
          debug('got request packet');
          fn(reqPkt, function (err, resPkt) {
            if (err) {
              channel.emit('error', err);
              return;
            }
            debug('got response packet');
            pkt = resPkt;
            step();
          });
        });
        function step() {
          if (encoder && pkt) {
            encoder.end(pkt);
          }
        }
      };
    }
    channel = new StatelessServerChannel(this, callbackFactory, opts);
  } else {
    var readable, writable;
    if (isStream(transport)) {
      readable = writable = transport;
    } else {
      readable = transport.readable;
      writable = transport.writable;
    }
    var codec;
    if (newCodec) {
      codec = newCodec();
      readable = readable.pipe(codec.decoder);
      codec.encoder.pipe(writable);
      writable = codec.encoder;
    }
    channel = new StatefulServerChannel(this, readable, writable, opts);
    if (codec) {
      // Similar to client channels, since we never expose the encoder and
      // decoder, we must release them ourselves here.
      channel.once('eot', function () {
        readable.unpipe(codec.decoder);
        codec.encoder.unpipe(writable);
      });
      codec.decoder.once('error', function (err) {
        debug('decoder failed with an error (%s), destroying channel', err);
        channel.emit('error', err);
      });
    }
  }

  if (!this.listeners('error').length) {
    this.on('error', this._onError);
  }
  var channelId = this._nextChannelId++;
  var channels = this._channels;
  channels[channelId] = channel
    .once('eot', function () { delete channels[channelId]; });
  debug('added server channel');
  this.emit('channel', channel);
  return channel;
};

Server.prototype.destroyChannels = function (opts) {
  this.activeChannels().forEach(function (channel) {
    channel.destroy(opts);
  });
};

Server.prototype.onMessage = function (name, handler) {
  getExistingMessage(this.service, name); // Check message existence.
  this._handlers[name] = handler;
  return this;
};

Server.prototype.remoteProtocols = function () {
  return getRemoteProtocols(this._cache, false);
};

Server.prototype.call = Client.prototype.call;

Server.prototype.use = function (/* fn ... */) {
  var i, l;
  for (i = 0, l = arguments.length; i < l; i++) {
    this._fns.push(arguments[i]);
  }
  return this;
};

Server.prototype._createMessageHandler = function (msg) {
  // jshint -W054
  var name = msg.name;
  var fields = msg.requestType.fields;
  var numArgs = fields.length;
  var args = fields.length ?
    ', ' + fields.map(function (f) { return 'req.' + f.name; }).join(', ') :
    '';
  // We are careful to not lose the initial handler's number of arguments (or
  // more specifically whether it would have access to the callback or not).
  // This is useful to implement "smart promisification" logic downstream.
  var body = 'return function (handler) {\n';
  body += '  if (handler.length > ' + numArgs + ') {\n';
  body += '    return this.onMessage(\'' + name + '\', function (req, cb) {\n';
  body += '      return handler.call(this' + args + ', cb);\n';
  body += '    });\n';
  body += '  } else {\n';
  body += '    return this.onMessage(\'' + name + '\', function (req) {\n';
  body += '      return handler.call(this' + args + ');\n';
  body += '    });\n';
  body += '  }\n';
  body += '};\n';
  return (new Function(body))();
};

Server.prototype._onError = function (err) {
  /* istanbul ignore if */
  if (!this._silent && err.rpcCode !== 'UNKNOWN_PROTOCOL') {
    console.error();
    if (err.rpcCode) {
      console.error(err.rpcCode);
      if (err.cause) {
        console.error(err.cause);
      }
    } else {
      console.error('INTERNAL_SERVER_ERROR');
      console.error(err);
    }
  }
};

/** Base message emitter class. See below for the two available variants. */
function ClientChannel(client, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this.client = client;
  this._prefix = normalizedPrefix(opts.scope);

  var cache = client._cache$;
  var clientSvc = client._svc$;
  var hash = opts.serverHash;
  if (!hash) {
    hash = clientSvc.hash;
  }
  var adapter = cache[hash];
  if (!adapter) {
    // This might happen even if the server hash option was set if the cache
    // doesn't contain the corresponding adapter. In this case we fall back to
    // the client's protocol (as mandated by the spec).
    hash = clientSvc.hash;
    adapter = cache[hash] = new Adapter(clientSvc, clientSvc, hash);
  }
  this._adapter = adapter;

  this._registry = new Registry(this, PREFIX_LENGTH);
  this.pending = 0;
  this.destroyed = false;
  this.draining = false;
  this.once('_eot', function (pending, err) {
    // Since this listener is only run once, we will only forward an error if
    // it is present during the initial `destroy` call, which is OK.
    debug('client channel EOT');
    this.destroyed = true;
    this.emit('eot', pending, err);
  });
}
util.inherits(ClientChannel, events.EventEmitter);

ClientChannel.prototype.destroy = function (opts) {
  var noWait = opts && opts.noWait;
  debug('destroying client channel');
  if (!this.draining) {
    this.draining = true;
    this.emit('drain');
  }
  var registry = this._registry;
  var pending = this.pending;
  if (noWait) {
    registry.clear();
  }
  if (noWait || !pending) {
    this.emit('_eot', pending);
  } else {
    debug('client channel entering drain mode (%s pending)', pending);
  }
};

ClientChannel.prototype.ping = function (timeout, cb) {
  if (!cb && typeof timeout == 'function') {
    cb = timeout;
    timeout = this.client._defaultTimeout$;
  }
  var self = this;
  var wreq = new WrappedRequest(PING_MESSAGE);
  var wres = new WrappedResponse(PING_MESSAGE);
  this._emit(wreq, wres, {deadline: Date.now() + timeout}, function (err) {
    if (cb) {
      cb.call(self, err);
    } else if (err) {
      self.emit('error', err);
    }
  });
};

ClientChannel.prototype._createHandshakeRequest = function (adapter, noSvc) {
  var svc = this.client._svc$;
  var HandshakeRequest = HANDSHAKE_REQUEST_TYPE.recordConstructor;
  return new HandshakeRequest(
    svc.hash,
    noSvc ? null : JSON.stringify(svc.protocol),
    adapter._hash
  );
};

ClientChannel.prototype._emit = function (wreq, wres, ctx, cb) {
  var msg = wreq._msg;
  var self = this;
  this.pending++;
  process.nextTick(function () {
    if (!msg.name) {
      // Ping request, bypass middleware.
      onTransition(wreq, wres, onCompletion);
    } else {
      var fns = self.client._fns$;
      debug('starting client middleware chain (%s)', fns.length);
      chainMiddleware({
        fns: fns,
        ctx: ctx,
        wreq: wreq,
        wres: wres,
        channel: self,
        events: [
          'outgoingRequestPre', 'outgoingRequestPost',
          'incomingResponsePre', 'incomingResponsePost',
        ],
        onTransition: onTransition,
        onCompletion: onCompletion,
        onError: onError
      });
    }
  });

  function onTransition(wreq, wres, prev) {
    if (self.destroyed) {
      prev(new Error('channel destroyed'));
      return;
    }
    try {
      var pkt = wreq.toPacket(0, self.client.tagTypes); // ID populated below.
    } catch (err) {
      prev(err);
      return;
    }
    // Compute a (reduced) timeout for the remote part of the flow.
    var timeout = ctx.deadline ? ctx.deadline - Date.now() : 0;
    var id = self._registry.add(timeout, function (err, resPkt, adapter) {
      if (!err && !msg.oneWay) {
        try {
          adapter._decodeResponse(resPkt, wres, msg, self.client.tagTypes);
        } catch (cause) {
          err = cause;
        }
      }
      prev(err);
    });
    id |= self._prefix;
    pkt.id = id;
    debug('sending message %s', id);
    self._send(pkt, !!msg && msg.oneWay);
  }

  function onCompletion(err) {
    self.pending--;
    cb.call(ctx, err, wres);
    if (self.draining && !self.destroyed && !self.pending) {
      self.destroy();
    }
  }

  function onError(err) {
    // This will happen if a middleware callback is called multiple times. We
    // forward the error to the client rather than emit it on the channel since
    // middleware are a client-level abstraction, so better handled there.
    self.client.emit('error', err, self);
  }
};

ClientChannel.prototype._getAdapter = function (hres) {
  var hash = hres.serverHash;
  var cache = this.client._cache$;
  var adapter = cache[hash];
  if (adapter) {
    return adapter;
  }
  var ptcl = JSON.parse(hres.serverProtocol);
  var serverSvc = Service.forProtocol(ptcl);
  adapter = new Adapter(this.client._svc$, serverSvc, hash, true);
  return cache[hash] = adapter;
};

ClientChannel.prototype._matchesPrefix = function (id) {
  return matchesPrefix(id, this._prefix);
};

ClientChannel.prototype._send = utils.abstractFunction;

/**
 * Callback factory-based client channel.
 *
 * This channel doesn't keep a persistent connection to the server and requires
 * prepending a handshake to each message emitted. Usage examples include
 * talking to an HTTP server (where the factory returns an HTTP request).
 *
 * Since each message will use its own writable/readable stream pair, the
 * advantage of this channel is that it is able to keep track of which response
 * corresponds to each request without relying on transport ordering. In
 * particular, this means these channels are compatible with any server
 * implementation.
 */
function StatelessClientChannel(client, callbackFactory, opts) {
  ClientChannel.call(this, client, opts);
  this._callbackFactory = callbackFactory;

  var connectionTimeout = (opts && opts.connectionTimeout) | 0;
  if (!opts || !opts.noPing) {
    // Ping the server to check whether the remote protocol is compatible.
    // If not, this will throw an error on the channel.
    debug('emitting ping request');
    this.ping(connectionTimeout);
  }
}
util.inherits(StatelessClientChannel, ClientChannel);

StatelessClientChannel.prototype._send = function (baseReqPkt) {
  var cb = this._registry.get(baseReqPkt.id);
  var adapter = this._adapter;
  var self = this;
  process.nextTick(emit);
  return true;

  function emit(retry) {
    if (self.destroyed) {
      // The request's callback will already have been called.
      return;
    }
    var hreq = self._createHandshakeRequest(adapter, !retry);
    var reqPkt = withHandshake(baseReqPkt, hreq);
    self._callbackFactory.call(self, reqPkt, function (err, resPkt) {
      if (err) {
        cb(err);
        return;
      }
      debug('received response');
      // We don't check that the prefix matches since the ID likely hasn't been
      // propagated to the response (see default stateless codec).
      var hres = resPkt.handshake.unwrap();
      if (hres.serverHash) {
        try {
          adapter = self._getAdapter(hres);
        } catch (cause) {
          cb(cause);
          return;
        }
      }
      var match = hres.match;
      debug('handshake match: %s', match);
      self.emit('handshake', hreq, hres);
      if (match === 'NONE') {
        // Try again, including the full protocol this time.
        process.nextTick(function() { emit(true); });
      } else {
        // Change the default adapter.
        self._adapter = adapter;
        cb(null, resPkt, adapter);
      }
    });
  }
};

/**
 * Multiplexing client channel.
 *
 * These channels reuse the same streams (both readable and writable) for all
 * messages. This avoids a lot of overhead (e.g. creating new connections,
 * re-issuing handshakes) but requires the underlying transport to support
 * forwarding message IDs.
 */
function StatefulClientChannel(client, readable, writable, opts) {
  ClientChannel.call(this, client, opts);
  this._endWritable = !!utils.getOption(opts, 'endWritable', true);
  this._readable = readable;
  this._writable = writable;
  this._connected = !!(opts && opts.noPing);
  this._readable.on('end', onEnd);
  this._writable.on('finish', onFinish);

  var self = this;
  var timer = null;
  this.once('eot', function () {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    if (!self._connected) {
      // Clear any buffered calls (they are guaranteed to error out when
      // reaching the transition phase).
      self.emit('_ready');
    }
    // Remove references to this channel to avoid potential memory leaks.
    this._writable.removeListener('finish', onFinish);
    if (this._endWritable) {
      debug('ending transport');
      this._writable.end();
    }
    this._readable
      .removeListener('data', onPing)
      .removeListener('data', onMessage)
      .removeListener('end', onEnd);
  });

  var connectionTimeout = (opts && opts.connectionTimeout) | 0;
  var hreq; // For handshake events.
  if (this._connected) {
    this._readable.on('data', onMessage);
  } else {
    this._readable.on('data', onPing);
    process.nextTick(ping);
    if (connectionTimeout) {
      timer = setTimeout(function () {
        self.emit('error', new Error('ping timeout'));
      }, connectionTimeout);
    }
  }

  function ping(retry) {
    if (self.destroyed) {
      return;
    }
    hreq = self._createHandshakeRequest(self._adapter, !retry).wrap();
    // We can use a static ID here since we are guaranteed that this message is
    // the only one on the channel (for this scope at least).
    self._writable.write(new Packet(self._prefix, undefined, undefined, hreq));
  }

  function onPing(resPkt) {
    if (!self._matchesPrefix(resPkt.id)) {
      debug('discarding unscoped response %s (still connecting)', resPkt.id);
      return;
    }
    var hres = resPkt.handshake.unwrap();
    if (hres.serverHash) {
      try {
        self._adapter = self._getAdapter(hres);
      } catch (cause) {
        // This isn't a recoverable error.
        self.emit('error', cause);
        return;
      }
    }
    var match = hres.match;
    debug('handshake match: %s', match);
    self.emit('handshake', hreq, hres);
    if (match === 'NONE') {
      process.nextTick(function () { ping(true); });
    } else {
      debug('successfully connected');
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      self._readable.removeListener('data', onPing).on('data', onMessage);
      self._connected = true;
      self.emit('_ready');
      hreq = null; // Release reference.
    }
  }

  // Callback used after a connection has been established.
  function onMessage(reqPkt) {
    var id = reqPkt.id;
    if (!self._matchesPrefix(id)) {
      debug('discarding unscoped message %s', id);
      return;
    }
    var cb = self._registry.get(id);
    if (cb) {
      process.nextTick(function () {
        debug('received message %s', id);
        // Ensure that the initial callback gets called asynchronously, even
        // for completely synchronous transports (otherwise the number of
        // pending requests will sometimes be inconsistent between stateful and
        // stateless transports).
        cb(null, reqPkt, self._adapter);
      });
    }
  }

  function onEnd() { self.destroy({noWait: true}); }
  function onFinish() { self.destroy(); }
}
util.inherits(StatefulClientChannel, ClientChannel);

StatefulClientChannel.prototype._emit = function (wreq, wres, ctx, cb) {
  // Override this method to allow calling `_emit` even before the channel is
  // connected. Note that we don't perform this logic in `_send` since we want
  // to guarantee that `'handshake'` events are emitted before any
  // `'outgoingCallPre'` events.
  var self = this;
  if (wreq._msg.oneWay) {
    // We override wrapped responses to indicate that we're not getting a
    // response for one-way messages.
    wres = undefined;
  }
  if (this._connected || this.draining) {
    done();
  } else {
    debug('queuing request');
    this.once('_ready', done);
  }

  function done() {
    ClientChannel.prototype._emit.call(self, wreq, wres, ctx, cb);
  }
};

StatefulClientChannel.prototype._send = function (reqPkt, oneWay) {
  var id = reqPkt.id;
  if (oneWay) {
    var self = this;
    // Clear the callback, passing in an empty packet.
    process.nextTick(function () {
      self._registry.get(id)(null, new Packet(id), self._adapter);
    });
  }
  return this._writable.write(reqPkt);
};

/** The server-side emitter equivalent. */
function ServerChannel(server, opts) {
  opts = opts || {};
  events.EventEmitter.call(this);

  this.server = server;
  this._prefix = normalizedPrefix(opts.scope);

  var cache = server._cache;
  var svc = server.service;
  var hash = svc.hash;
  if (!cache[hash]) {
    // Add the channel's protocol to the cache if it isn't already there. This
    // will save a handshake the first time on channels with the same protocol.
    cache[hash] = new Adapter(svc, svc, hash);
  }
  this._adapter = null;

  this.destroyed = false;
  this.draining = false;
  this.pending = 0;
  this.once('_eot', function (pending, err) {
    debug('server channel EOT');
    this.emit('eot', pending, err);
  });
}
util.inherits(ServerChannel, events.EventEmitter);

ServerChannel.prototype.destroy = function (opts) {
  var noWait = opts && opts.noWait;
  debug('destroying server channel');
  if (!this.draining) {
    this.draining = true;
    this.emit('drain');
  }
  if (noWait || !this.pending) {
    this.destroyed = true;
    if (isError(noWait)) {
      debug('fatal server channel error: %s', noWait);
      this.emit('_eot', this.pending, noWait);
    } else {
      this.emit('_eot', this.pending);
    }
  }
};

ServerChannel.prototype._createHandshakeResponse = function (err, hreq) {
  var svc = this.server.service;
  var buf = svc.hash;
  var serverMatch = hreq && hreq.serverHash.equals(buf);
  var HandshakeResponse = HANDSHAKE_RESPONSE_TYPE.recordConstructor;
  return new HandshakeResponse(
    err ? 'NONE' : (serverMatch ? 'BOTH' : 'CLIENT'),
    serverMatch ? null : JSON.stringify(svc.protocol),
    serverMatch ? null : buf
  );
};

ServerChannel.prototype._getAdapter = function (hreq) {
  var hash = hreq.clientHash;
  var adapter = this.server._cache[hash];
  if (adapter) {
    return adapter;
  }
  if (!hreq.clientProtocol) {
    throw toRpcError('UNKNOWN_PROTOCOL');
  }
  var ptcl = JSON.parse(hreq.clientProtocol);
  var clientSvc = Service.forProtocol(ptcl);
  adapter = new Adapter(clientSvc, this.server.service, hash, true);
  return this.server._cache[hash] = adapter;
};

ServerChannel.prototype._matchesPrefix = function (id) {
  return matchesPrefix(id, this._prefix);
};

ServerChannel.prototype._receive = function (reqPkt, adapter, ctx, noRes, cb) {
  var self = this;
  var id = reqPkt.id;
  var wreq;
  try {
    wreq = adapter._decodeRequest(reqPkt, self.server.tagTypes);
  } catch (cause) {
    cb(self._systemErrorPacket(id, toRpcError('INVALID_REQUEST', cause)));
    return;
  }

  var msg = wreq._msg;
  var wres = noRes && msg.oneWay ? undefined : new WrappedResponse(msg);
  if (!msg.name) {
    // Ping message, we don't invoke middleware logic in this case.
    wres.response = null;
    cb(wres.toPacket(id, self.server.tagTypes), false);
    return;
  }

  ctx = utils.copyOwnProperties(ctx, {channel: this});
  var fns = this.server._fns;
  debug('starting server middleware chain (%s)', fns.length);
  if (wres) {
    self.pending++;
  }
  chainMiddleware({
    fns: fns,
    ctx: ctx,
    wreq: wreq,
    wres: wres,
    channel: self,
    events: [
      'incomingRequestPre', 'incomingRequestPost',
      'outgoingResponsePre', 'outgoingResponsePost',
    ],
    onTransition: onTransition,
    onCompletion: onCompletion,
    onError: onError
  });

  function onTransition(wreq, wres, prev) {
    var handler = self.server._handlers[msg.name];
    if (!handler) {
      // The underlying service hasn't implemented a handler.
      var defaultHandler = self.server._defaultHandler;
      if (defaultHandler) {
        // We call the default handler with arguments similar (slightly
        // simpler, there are no phases here) to middleware such that it can
        // easily access the message name (useful to implement proxies).
        defaultHandler.call(ctx, wreq, wres, prev);
      } else {
        var cause = new Error(f('no handler for %s', msg.name));
        prev(toRpcError('NOT_IMPLEMENTED', cause));
      }
      return;
    }

    var pending = !!wres;
    try {
      if (pending) {
        handler.call(ctx, wreq.request, function (err, res) {
          wres.error = err;
          wres.response = res;
          pending = false;
          prev();
        });
      } else {
        handler.call(ctx, wreq.request);
      }
    } catch (err) {
      // We catch synchronous failures (same as express) and return the
      // failure. Note that the server process can still crash if an error
      // is thrown after the handler returns but before the response is
      // sent (again, same as express). We are careful to only trigger the
      // response callback once, emitting the errors afterwards instead.
      if (pending) {
        prev(err);
      } else {
        onError(err);
      }
    }
  }

  function onCompletion(err) {
    self.pending--;
    var server = self.server;
    var resPkt;
    if (!err) {
      var resErr = wres.error;
      var isStrict = server._strict;
      if (!isStrict) {
        if (isError(resErr)) {
          // If the error type is wrapped, we must wrap the error too.
          wres.error = msg.errorType.clone(resErr.message, {wrapUnions: true});
        } else if (resErr === null) {
          // We also allow `null`'s as error in this mode, converting them to
          // the Avro-compatible `undefined`.
          resErr = wres.error = undefined;
        }
        if (
          resErr === undefined &&
          wres.response === undefined &&
          msg.responseType.isValid(null)
        ) {
          // Finally, for messages with `null` as acceptable response type, we
          // allow `undefined`; converting them to `null`. This allows users to
          // write a more natural `cb()` instead of `cb(null, null)`.
          wres.response = null;
        }
      }
      try {
        resPkt = wres.toPacket(id, server.tagTypes);
      } catch (cause) {
        debug('failed to serialize wrapped response (%s)', cause);
        err = cause;
      }
    }
    if (!resPkt) {
      resPkt = self._systemErrorPacket(id, err, wres.tags);
    } else if (resErr !== undefined) {
      server.emit('error', toRpcError('APPLICATION_ERROR', resErr));
    }
    cb(resPkt, msg.oneWay);
    if (self.draining && !self.pending) {
      self.destroy();
    }
  }

  function onError(err) {
    // Similar to the client equivalent, we redirect this error to the server
    // since middleware are defined at server-level.
    self.server.emit('error', err, self);
  }
};

/** Encode an error and headers into a packet. */
ServerChannel.prototype._systemErrorPacket = function (id, err, tags) {
  var server = this.server;
  server.emit('error', err, this);
  var errStr;
  if (server._sysErrFormatter) {
    // Format the error into a string to send over the wire.
    errStr = server._sysErrFormatter.call(this, err);
  } else if (err.rpcCode) {
    // By default, only forward the error's message when the RPC code is set
    // (i.e. when this isn't an internal server error).
    errStr = err.message;
  }
  var body = Buffer.concat([
    new Buffer([1, 0]), // Error flag and first union index.
    STRING_TYPE.toBuffer(errStr || 'internal server error')
  ]);
  var headers;
  if (tags) {
    try {
      headers = serializeTags(tags, server.tagTypes);
    } catch (err) {
      debug('unable to serialize tags (%s)', err);
    }
  }
  return new Packet(id, headers || {}, body);
};

/**
 * Server channel for stateless transport.
 *
 * This channel expect a handshake to precede each message.
 */
function StatelessServerChannel(server, callbackFactory, opts) {
  ServerChannel.call(this, server, opts);
  var self = this;
  callbackFactory.call(this, function (reqPkt, ctx, cb) {
    if (!cb && typeof ctx == 'function') {
      cb = ctx;
      ctx = undefined;
    }
    if (self.destroyed) {
      cb(new Error('destroyed'));
      return;
    }
    var hreq = reqPkt.handshake.unwrap();
    var err;
    try {
      var adapter = self._getAdapter(hreq);
    } catch (cause) {
      err = toRpcError('INVALID_HANDSHAKE_REQUEST', cause);
    }

    var hres = self._createHandshakeResponse(err, hreq);
    self.emit('handshake', hreq, hres);
    if (err) {
      done(self._systemErrorPacket(reqPkt.id, err));
    } else {
      self._receive(reqPkt, adapter, ctx, false, done);
    }

    function done(resPkt) {
      if (self.destroyed) {
        return;
      }
      resPkt.handshake = hres.wrap();
      cb(null, resPkt);
    }
  });
}
util.inherits(StatelessServerChannel, ServerChannel);

/**
 * Stateful transport listener.
 *
 * A handshake is done when the channel first receives a message, then all
 * messages are sent without.
 */
function StatefulServerChannel(server, readable, writable, opts) {
  ServerChannel.call(this, server, opts);
  this._adapter = undefined;
  this._endWritable = !!utils.getOption(opts, 'endWritable', true);
  this._writable = writable.on('finish', onFinish);
  this._readable = readable.on('data', onHandshake).on('end', onEnd);
  this
    .once('drain', function () {
      // Stop listening to incoming events.
      this._readable
        .removeListener('data', onHandshake)
        .removeListener('data', onRequest)
        .removeListener('end', onEnd);
    })
    .once('eot', function () {
      // Clean up any references to the channel on the underlying streams.
      this._writable.removeListener('finish', onFinish);
      if (this._endWritable) {
        this._writable.end();
      }
    });

  var self = this;

  function onHandshake(reqPkt) {
    debug('received pre-connection packet');
    var id = reqPkt.id;
    if (!self._matchesPrefix(id)) {
      return;
    }
    var hreq = reqPkt.handshake.unwrap();
    var err;
    try {
      self._adapter = self._getAdapter(hreq);
    } catch (cause) {
      err = toRpcError('INVALID_HANDSHAKE_REQUEST', cause);
    }
    var hres = self._createHandshakeResponse(err, hreq);
    self.emit('handshake', hreq, hres);
    if (err) {
      // Either the client's protocol was unknown or it isn't compatible.
      done(self._systemErrorPacket(id, err));
    } else {
      self._readable
        .removeListener('data', onHandshake)
        .on('data', onRequest);
      self._receive(reqPkt, self._adapter, {}, true, done);
    }

    function done(resPkt) {
      if (self.destroyed) {
        return;
      }
      resPkt.handshake = hres.wrap();
      self._writable.write(resPkt);
    }
  }

  function onRequest(reqPkt) {
    // These requests are not prefixed with handshakes.
    var id = reqPkt.id;
    if (!self._matchesPrefix(id)) {
      return;
    }
    self._receive(reqPkt, self._adapter, {}, true, function (resPkt, oneWay) {
      if (self.destroyed || oneWay) {
        return;
      }
      resPkt.id = id;
      self._writable.write(resPkt);
    });
  }

  function onEnd() { self.destroy(); }

  function onFinish() { self.destroy({noWait: true}); }
}
util.inherits(StatefulServerChannel, ServerChannel);

// Helpers.

function serializeTags(tags, tagTypes) {
  var headers = {};
  if (!tags) {
    return headers;
  }
  var keys = Object.keys(tags);
  var i, l, key, type;
  for (i = 0, l = keys.length; i < l; i++) {
    key = keys[i];
    type = tagTypes[key];
    if (!type) {
      // We throw an error here since tag values to avoid silently failing
      // (unlike remote headers, tags are under the same process' control).
      throw new Error(f('unknown tag: %s', key));
    }
    headers[key] = type.toBuffer(tags[key]);
  }
  return headers;
}

function deserializeTags(headers, tagTypes) {
  var tags = {};
  if (!headers) {
    return tags;
  }
  var keys = Object.keys(headers);
  var i, l, key, type;
  for (i = 0, l = keys.length; i < l; i++) {
    key = keys[i];
    type = tagTypes[key];
    if (type) {
      tags[key] = type.fromBuffer(headers[key]);
    } else {
      // Unlike the serialization case above, the client/server can't
      // necessarily control which headers its counterpart uses.
      debug('ignoring unknown tag %s', key);
    }
  }
  return tags;
}

/** Enhanced request, used inside forward middleware functions. */
function WrappedRequest(msg, tags, req) {
  this._msg = msg;
  this.tags = tags || {};
  this.request = req || {};
}

WrappedRequest.prototype.toPacket = function (id, tagTypes) {
  var msg = this._msg;
  var body = Buffer.concat([
    STRING_TYPE.toBuffer(msg.name),
    msg.requestType.toBuffer(this.request)
  ]);
  return new Packet(id, serializeTags(this.tags, tagTypes), body);
};
/** Enhanced response, used inside forward middleware functions. */
function WrappedResponse(msg, tags, err, res) {
  this._msg = msg;
  this.tags = tags || {};
  this.error = err;
  this.response = res;
}

WrappedResponse.prototype.toPacket = function (id, tagTypes) {
  var hasError = this.error !== undefined;
  var body = Buffer.concat([
    BOOLEAN_TYPE.toBuffer(hasError),
    hasError ?
      this._msg.errorType.toBuffer(this.error) :
      this._msg.responseType.toBuffer(this.response)
  ]);
  return new Packet(id, serializeTags(this.tags, tagTypes), body);
};

/**
 * Callback registry.
 *
 * Callbacks added must accept an error as first argument. This is used by
 * client channels to store pending calls. This class isn't exposed by the
 * public API.
 */
function Registry(ctx, prefixLength) {
  this._ctx = ctx; // Context for all callbacks.
  this._mask = ~0 >>> (prefixLength | 0); // 16 bits by default.
  this._id = 0; // Unique integer ID for each call.
  this._n = 0; // Number of pending calls.
  this._cbs = {};
}

Registry.prototype.get = function (id) { return this._cbs[id & this._mask]; };

Registry.prototype.add = function (timeout, fn) {
  this._id = (this._id + 1) & this._mask;

  var self = this;
  var id = this._id;
  var timer;
  if (timeout > 0) {
    timer = setTimeout(function () { cb(new Error('timeout')); }, timeout);
  }

  this._cbs[id] = cb;
  this._n++;
  return id;

  function cb() {
    if (!self._cbs[id]) {
      // The callback has already run.
      return;
    }
    delete self._cbs[id];
    self._n--;
    if (timer) {
      clearTimeout(timer);
    }
    fn.apply(self._ctx, arguments);
  }
};

Registry.prototype.clear = function () {
  Object.keys(this._cbs).forEach(function (id) {
    this._cbs[id](new Error('interrupted'));
  }, this);
};

/**
 * Service resolution helper.
 *
 * It is used both by client and server channels, to respectively decode errors
 * and responses, or requests.
 */
function Adapter(clientSvc, serverSvc, hash, isRemote) {
  this._clientSvc = clientSvc;
  this._serverSvc = serverSvc;
  this._hash = hash; // Convenience to access it when creating handshakes.
  this._isRemote = !!isRemote;
  this._readers = createReaders(clientSvc, serverSvc);
}

Adapter.prototype._decodeRequest = function (reqPkt, tagTypes) {
  var tap = new Tap(reqPkt.body);
  var name = STRING_TYPE._read(tap);
  var msg, req;
  if (name) {
    msg = this._serverSvc.message(name);
    req = this._readers[name + '?']._read(tap);
  } else {
    msg = PING_MESSAGE;
  }
  if (!tap.isValid()) {
    throw new Error(f('truncated %s request', name || 'ping$'));
  }
  var tags = deserializeTags(reqPkt.headers, tagTypes);
  return new WrappedRequest(msg, tags, req);
};

Adapter.prototype._decodeResponse = function (resPkt, wres, msg, tagTypes) {
  var tags = deserializeTags(resPkt.headers, tagTypes);
  utils.copyOwnProperties(tags, wres.tags, true);
  var tap = new Tap(resPkt.body);
  var isError = BOOLEAN_TYPE._read(tap);
  var name = msg.name;
  if (name) {
    var reader = this._readers[name + (isError ? '*' : '!')];
    msg = this._clientSvc.message(name);
    if (isError) {
      wres.error = reader._read(tap);
    } else {
      wres.response = reader._read(tap);
    }
    if (!tap.isValid()) {
      throw new Error(f('truncated %s response', name));
    }
  } else {
    msg = PING_MESSAGE;
  }
};

function packetForBuffer(id, buf, handshakeType) {
  var handshake, parts;
  if (handshakeType) {
    parts = readHead(handshakeType, buf);
    handshake = parts.head.wrap();
    buf = parts.tail;
  }
  parts = readHead(MAP_BYTES_TYPE, buf);
  return new Packet(id, parts.head, parts.tail, handshake);
}

function packetPayload(packet) {
  var bufs = [MAP_BYTES_TYPE.toBuffer(packet.headers), packet.body];
  if (packet.handshake) {
    bufs.unshift(packet.handshake.unwrap().toBuffer());
  }
  return Buffer.concat(bufs);
};

function withHandshake(packet, handshake) {
  return new Packet(packet.id, packet.headers, packet.body, handshake.wrap());
};

/** Standard "un-framing" stream. */
function FrameDecoder(handshakeType) {
  stream.Transform.call(this, {readableObjectMode: true});
  this._handshakeType = handshakeType;
  this._buf = new Buffer(0);
  this._bufs = [];

  this.on('finish', function () { this.push(null); });
}
util.inherits(FrameDecoder, stream.Transform);

FrameDecoder.prototype._transform = function (buf, encoding, cb) {
  buf = Buffer.concat([this._buf, buf]);
  var frameLength;
  while (
    buf.length >= 4 &&
    buf.length >= (frameLength = buf.readInt32BE(0)) + 4
  ) {
    if (frameLength) {
      this._bufs.push(buf.slice(4, frameLength + 4));
    } else {
      var bufs = this._bufs;
      this._bufs = [];
      var pkt;
      try {
        pkt = packetForBuffer(0, Buffer.concat(bufs), this._handshakeType);
      } catch (err) {
        this.emit('error', err);
        return;
      }
      this.push(pkt);
    }
    buf = buf.slice(frameLength + 4);
  }
  this._buf = buf;
  cb();
};

FrameDecoder.prototype._flush = function () {
  if (this._buf.length || this._bufs.length) {
    var bufs = this._bufs.slice();
    bufs.unshift(this._buf);
    var err = toRpcError('TRAILING_DATA');
    // Attach the data to help debugging (e.g. if the encoded bytes contain a
    // human-readable protocol like HTTP).
    err.trailingData = Buffer.concat(bufs).toString();
    this.emit('error', err);
  }
};

/** Standard framing stream. */
function FrameEncoder() {
  stream.Transform.call(this, {writableObjectMode: true});
  this.on('finish', function () { this.push(null); });
}
util.inherits(FrameEncoder, stream.Transform);

FrameEncoder.prototype._transform = function (pkt, encoding, cb) {
  try {
    var buf = packetPayload(pkt);
  } catch (err) {
    cb(err);
    return;
  }
  var length = buf.length;
  this.push(intBuffer(length));
  if (length) {
    this.push(buf);
    this.push(intBuffer(0));
  }
  cb();
};

function frameCodec(decoderHandshakeType) {
  return {
    decoder: new FrameDecoder(decoderHandshakeType),
    encoder: new FrameEncoder()
  };
}

/** Netty-compatible decoding stream. */
function NettyDecoder(handshakeType) {
  stream.Transform.call(this, {readableObjectMode: true});
  this._handshakeType = handshakeType;
  this._id = undefined;
  this._frameCount = 0;
  this._buf = new Buffer(0);
  this._bufs = [];

  this.on('finish', function () { this.push(null); });
}
util.inherits(NettyDecoder, stream.Transform);

NettyDecoder.prototype._transform = function (buf, encoding, cb) {
  buf = Buffer.concat([this._buf, buf]);

  while (true) {
    if (this._id === undefined) {
      if (buf.length < 8) {
        this._buf = buf;
        cb();
        return;
      }
      this._id = buf.readInt32BE(0);
      this._frameCount = buf.readInt32BE(4);
      buf = buf.slice(8);
    }

    var frameLength;
    while (
      this._frameCount &&
      buf.length >= 4 &&
      buf.length >= (frameLength = buf.readInt32BE(0)) + 4
    ) {
      this._frameCount--;
      this._bufs.push(buf.slice(4, frameLength + 4));
      buf = buf.slice(frameLength + 4);
    }

    if (this._frameCount) {
      this._buf = buf;
      cb();
      return;
    } else {
      var pkt;
      try {
        pkt = this._decodePacket(this._id, Buffer.concat(this._bufs));
      } catch (err) {
        this.emit('error', err);
        return;
      }
      this._bufs = [];
      this._id = undefined;
      this.push(pkt);
    }
  }
};

NettyDecoder.prototype._decodePacket = function (id, buf) {
  var pkt;
  try {
    pkt = packetForBuffer(id, buf, this._handshakeType);
  } catch (err) { // Probably connected.
    debug('unable to decode packet with %s', this._handshakeType);
    pkt = packetForBuffer(id, buf);
    this._handshakeType = null;
  }
  debug('packet decoded');
  return pkt;
};

NettyDecoder.prototype._flush = FrameDecoder.prototype._flush;

/** Netty-compatible encoding stream. */
function NettyEncoder() {
  stream.Transform.call(this, {writableObjectMode: true});
  this.on('finish', function () { this.push(null); });
}
util.inherits(NettyEncoder, stream.Transform);

NettyEncoder.prototype._transform = function (pkt, encoding, cb) {
  try {
    var bufs = [packetPayload(pkt)];
  } catch (err) {
    cb(err);
    return;
  }
  var l = bufs.length;
  var buf;
  // Header: [ ID, number of frames ]
  buf = new Buffer(8);
  buf.writeInt32BE(pkt.id, 0);
  buf.writeInt32BE(l, 4);
  this.push(buf);
  // Frames, each: [ length, bytes ]
  var i;
  for (i = 0; i < l; i++) {
    buf = bufs[i];
    this.push(intBuffer(buf.length));
    this.push(buf);
  }
  cb();
};

function nettyCodec(decoderHandshakeType) {
  return {
    decoder: new NettyDecoder(decoderHandshakeType),
    encoder: new NettyEncoder()
  };
}

/**
 * Returns a buffer containing an integer's big-endian representation.
 *
 * @param n {Number} Integer.
 */
function intBuffer(n) {
  var buf = new Buffer(4);
  buf.writeInt32BE(n);
  return buf;
}

/**
 * Decode a type used as prefix inside a buffer.
 *
 * @param type {Type} The type of the prefix.
 * @param buf {Buffer} Encoded bytes.
 *
 * This function will return an object `{head, tail}` where head contains the
 * decoded value and tail the rest of the buffer. An error will be thrown if
 * the prefix cannot be decoded.
 */
function readHead(type, buf) {
  var tap = new Tap(buf);
  var head = type._read(tap);
  if (!tap.isValid()) {
    throw new Error(f('truncated %s', type));
  }
  return {head: head, tail: tap.buf.slice(tap.pos)};
}

/**
 * Generate a decoder, optimizing the case where reader and writer are equal.
 *
 * @param rtype {Type} Reader's type.
 * @param wtype {Type} Writer's type.
 */
function createReader(rtype, wtype) {
  return rtype.equals(wtype) ? rtype : rtype.createResolver(wtype);
}

/**
 * Generate all readers for a given protocol combination.
 *
 * @param clientSvc {Service} Client service.
 * @param serverSvc {Service} Client service.
 */
function createReaders(clientSvc, serverSvc) {
  var obj = {};
  clientSvc.messages.forEach(function (c) {
    var n = c.name;
    var s = serverSvc.message(n);
    try {
      if (!s) {
        throw new Error(f('missing server message: %s', n));
      }
      if (s.oneWay !== c.oneWay) {
        throw new Error(f('inconsistent one-way message: %s', n));
      }
      obj[n + '?'] = createReader(s.requestType, c.requestType);
      obj[n + '*'] = createReader(c.errorType, s.errorType);
      obj[n + '!'] = createReader(c.responseType, s.responseType);
    } catch (cause) {
      throw toRpcError('INCOMPATIBLE_PROTOCOL', cause);
    }
  });
  return obj;
}

/**
 * Populate a cache from a list of protocols.
 *
 * @param cache {Object} Cache of adapters.
 * @param svc {Service} The local service (either client or server).
 * @param ptcls {Array} Array of protocols to insert.
 * @param isClient {Boolean} Whether the local service is a client's or
 * server's.
 */
function insertRemoteProtocols(cache, ptcls, svc, isClient) {
  Object.keys(ptcls).forEach(function (hash) {
    var ptcl = ptcls[hash];
    var clientSvc, serverSvc;
    if (isClient) {
      clientSvc = svc;
      serverSvc = Service.forProtocol(ptcl);
    } else {
      clientSvc = Service.forProtocol(ptcl);
      serverSvc = svc;
    }
    cache[hash] = new Adapter(clientSvc, serverSvc, hash, true);
  });
}

/**
 * Extract remote protocols from a cache
 *
 * @param cache {Object} Cache of adapters.
 * @param isClient {Boolean} Whether the remote protocols extracted should be
 * the servers' or clients'.
 */
function getRemoteProtocols(cache, isClient) {
  var ptcls = {};
  Object.keys(cache).forEach(function (hs) {
    var adapter = cache[hs];
    if (adapter._isRemote) {
      var svc = isClient ? adapter._serverSvc : adapter._clientSvc;
      ptcls[hs] = svc.protocol;
    }
  });
  return ptcls;
}

/** Destroy all channels when at least one is destroyed. */
function destroyTogether(channels, opts) {
  channels.forEach(function (channel) { channel.once('eot', onEot); });
  function onEot() {
    channels.forEach(function (channel) { channel.destroy(opts); });
  }
}

/**
 * Check whether something is an `Error`.
 *
 * @param any {Object} Any object.
 */
function isError(any) {
  // Also not ideal, but avoids brittle `instanceof` checks.
  return !!any && Object.prototype.toString.call(any) === '[object Error]';
}

/**
 * Create an error.
 *
 * @param msg {String} Error message.
 * @param cause {Error} The cause of the error. It is available as `cause`
 * field on the outer error.
 */
function toError(msg, cause) {
  var err = new Error(msg);
  err.cause = cause;
  return err;
}

/**
 * Mark an error.
 *
 * @param rpcCode {String} Code representing the failure.
 * @param cause {Error} The cause of the error. It is available as `cause`
 * field on the outer error.
 *
 * This is used to keep the argument of channels' `'error'` event errors.
 */
function toRpcError(rpcCode, cause) {
  var err = toError(rpcCode.toLowerCase().replace(/_/g, ' '), cause);
  err.rpcCode = (cause && cause.rpcCode) ? cause.rpcCode : rpcCode;
  return err;
}

/**
 * Compute a prefix of fixed length from a string.
 *
 * @param scope {String} Namespace to be hashed.
 */
function normalizedPrefix(scope) {
  return scope ?
    utils.getHash(scope).readInt16BE(0) << (32 - PREFIX_LENGTH) :
    0;
}

/**
 * Check whether an ID matches the prefix.
 *
 * @param id {Integer} Number to check.
 * @param prefix {Integer} Already shifted prefix.
 */
function matchesPrefix(id, prefix) {
  return ((id ^ prefix) >> (32 - PREFIX_LENGTH)) === 0;
}

/**
 * Check whether something is a stream.
 *
 * @param any {Object} Any object.
 */
function isStream(any) {
  // This is a hacky way of checking that the transport is a stream-like
  // object. We unfortunately can't use `instanceof Stream` checks since
  // some libraries (e.g. websocket-stream) return streams which don't
  // inherit from it.
  return !!(any && any.pipe);
}

/** Extract the only element from a stream. */
function onlyElement(readable, cb) {
  var data = [];
  readable
    .on('error', function (err) {
      data = undefined;
      cb(err);
    })
    .on('data', function (any) { data.push(any); })
    .on('end', function () {
      if (data === undefined) {
        return; // There was an error.
      }
      switch (data.length) {
        case 0:
          cb(new Error('no data'));
          return;
        case 1:
          cb(null, data[0]);
          return;
        default:
          cb(new Error('too much data'));
      }
    });
}

/**
 * Get a message, asserting that it exists.
 *
 * @param svc {Service} The protocol to look into.
 * @param name {String} The message's name.
 */
function getExistingMessage(svc, name) {
  var msg = svc.message(name);
  if (!msg) {
    throw new Error(f('unknown message: %s', name));
  }
  return msg;
}

/**
 * Middleware logic.
 *
 * This is used both in clients and servers to intercept call handling (e.g. to
 * populate headers, do access control).
 *
 * @param params {Object} The following parameters:
 *  + fns {Array} Array of middleware functions.
 *  + ctx {Object} Context used to call the middleware functions, onTransition,
 *    and onCompletion.
 *  + wreq {WrappedRequest}
 *  + wres {WrappedResponse}
 *  + channel: {Channel} The channel used to emit lifecycle events.
 *  + events: {Array} Array of 4 event names for lifecycle events.
 *  + onTransition {Function} End of forward phase callback. It accepts an
 *    eventual error as single argument. This will be used for the backward
 *    phase. This function is guaranteed to be called at most once.
 *  + onCompletion {Function} Final handler, it takes an error as unique
 *    argument. This function is guaranteed to be only at most once.
 *  + onError {Function} Error handler, called if an intermediate callback is
 *    called multiple times.
 */
function chainMiddleware(params) {
  var args = [params.wreq, params.wres];
  var deadline = params.ctx.deadline;
  var cbs = [];
  var cause; // Back-propagated error.
  tryEmit(params.events[0]);
  if (cause) {
    // An error happened when emitting the first event, skip everything.
    params.onCompletion.call(params.ctx, cause);
  } else {
    forward(0);
  }

  function tryEmit(name) {
    var args = [name, params.wreq, params.wres, params.ctx, cause];
    try {
      events.EventEmitter.prototype.emit.apply(params.channel, args);
    } catch (err) {
      cause = err;
    }
  }

  function forward(pos) {
    var isDone = false;
    if (deadline !== undefined && Date.now() >= deadline) {
      cause = new Error('deadline exceeded');
      backward();
      return;
    }
    var wres = params.wres; // Defined for non-one-way messages.
    if (wres && (wres.error !== undefined || wres.response !== undefined)) {
      // Shortcut the rest of the chain, we already have a response.
      backward();
      return;
    }
    if (pos < params.fns.length) {
      params.fns[pos].apply(params.ctx, args.concat(function (err, cb) {
        if (isDone) {
          params.onError(toError('duplicate forward middleware call', err));
          return;
        }
        isDone = true;
        if (err) {
          // Stop the forward phase, bypass the handler, and start the backward
          // phase. Note that we ignore any callback argument in this case.
          cause = err;
          backward();
          return;
        }
        if (cb) {
          cbs.push(cb);
        }
        forward(++pos);
      }));
    } else {
      tryEmit(params.events[1]);
      if (cause) {
        backward();
        return;
      }
      // Done with the middleware forward functions, call the handler.
      params.onTransition.apply(params.ctx, args.concat(function (err) {
        if (isDone) {
          params.onError(toError('duplicate handler call', err));
          return;
        }
        isDone = true;
        cause = err;
        if (wres) {
          // Only trigger backward middleware if we actually send a response
          // back.
          tryEmit(params.events[2]);
          process.nextTick(backward);
        } else if (cause) {
          params.onError(cause);
        }
      }));
    }
  }

  function backward() {
    var cb = cbs.pop();
    if (cb) {
      var isDone = false;
      cb.call(params.ctx, cause, function (err) {
        if (isDone) {
          params.onError(toError('duplicate backward middleware call', err));
          return;
        }
        // Substitute the error.
        cause = err;
        isDone = true;
        backward();
      });
    } else {
      // Done with all middleware calls.
      tryEmit(params.events[3]);
      params.onCompletion.call(params.ctx, cause);
    }
  }
}


module.exports = {
  Adapter: Adapter,
  Message: Message,
  Packet: Packet,
  Registry: Registry,
  Service: Service,
  discoverProtocol: discoverProtocol,
  streams: {
    FrameDecoder: FrameDecoder,
    FrameEncoder: FrameEncoder,
    NettyDecoder: NettyDecoder,
    NettyEncoder: NettyEncoder
  }
};
