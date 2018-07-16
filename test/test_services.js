/* jshint node: true, mocha: true */

'use strict';

var types = require('../lib/types'),
    services = require('../lib/services'),
    assert = require('assert'),
    stream = require('stream'),
    util = require('util');

var Packet = services.Packet;
var Service = services.Service;

suite('services', function () {

  suite('Service', function () {

    test('get name, types, and protocol', function () {
      var p = {
        namespace: 'foo',
        protocol: 'HelloWorld',
        types: [
          {
            name: 'Greeting',
            type: 'record',
            fields: [{name: 'message', type: 'string'}]
          },
          {
            name: 'Curse',
            type: 'error',
            fields: [{name: 'message', type: 'string'}]
          }
        ],
        messages: {
          hello: {
            doc: 'say hello',
            request: [{name: 'greeting', type: 'Greeting'}],
            response: 'Greeting',
            errors: ['Curse']
          },
          hi: {
          request: [{name: 'hey', type: 'string'}],
          response: 'null',
          'one-way': true
          }
        }
      };
      var s = Service.forProtocol(p);
      assert.equal(s.name, 'foo.HelloWorld');
      assert.equal(s.type('foo.Greeting').typeName, 'record');
      assert.equal(s.type('string').typeName, 'string');
      assert.equal(s.types.length, 4);
      assert.deepEqual(s.protocol, p);
    });

    test('missing name', function () {
      assert.throws(function () {
        Service.forProtocol({namespace: 'com.acme', messages: {}});
      });
    });

    test('missing type', function () {
      assert.throws(function () {
        Service.forProtocol({
          namespace: 'com.acme',
          protocol: 'HelloWorld',
          messages: {
            hello: {
              request: [{name: 'greeting', type: 'Greeting'}],
              response: 'Greeting'
            }
          }
        });
      });
    });

    test('multiple references to namespaced types', function () {
      // This test is a useful sanity check for hoisting implementations.
      var n = 0;
      var s = Service.forProtocol({
        protocol: 'Hello',
        namespace: 'ping',
        types: [
          {
            name: 'Ping',
            type: 'record',
            fields: []
          },
          {
            name: 'Pong',
            type: 'record',
            fields: [{name: 'ping', type: 'Ping'}]
          },
          {
            name: 'Pung',
            type: 'record',
            fields: [{name: 'ping', type: 'Ping'}]
          }
        ]
      }, {typeHook: hook});
      assert.equal(s.type('ping.Ping').typeName, 'record');
      assert.equal(s.type('ping.Pong').typeName, 'record');
      assert.equal(s.type('ping.Pung').typeName, 'record');
      assert.equal(n, 5);

      function hook() { n++; }
    });

    test('special character in name', function () {
      assert.throws(function () {
        Service.forProtocol({
          protocol: 'Ping',
          messages: {
            'ping/1': {
              request: [],
              response: 'string'
            }
          }
        });
      }, /invalid message name/);
    });

    test('get messages', function () {
      var svc;
      svc = Service.forProtocol({protocol: 'Empty'});
      assert.deepEqual(svc.messages, {});
      svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {
          ping: {
            request: [],
            response: 'string'
          }
        }
      });
      var messages = svc.messages;
      assert.deepEqual(messages, [svc.message('ping')]);
    });

    test('protocol', function () {
      var protocol = {
        protocol: 'Hello',
        messages: {
          ping: {request: [], response: 'boolean', doc: ''},
          pong: {request: [], response: 'null', 'one-way': true}
        },
        doc: 'Hey'
      };
      var svc = Service.forProtocol(protocol);
      assert.deepEqual(svc.protocol, protocol);
    });

    test('get documentation', function () {
      var svc = Service.forProtocol({protocol: 'Hello', doc: 'Hey'});
      assert.equal(svc.doc, 'Hey');
    });

    test('isService', function () {
      var svc = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert(Service.isService(svc));
      assert(!Service.isService(undefined));
      assert(!Service.isService({protocol: 'bar'}));
    });

    test('inspect', function () {
      var svc = Service.forProtocol({
        namespace: 'hello',
        protocol: 'World',
      });
      assert.equal(svc.inspect(), '<Service "hello.World">');
    });

    test('using constructor', function () {
      var svc = new services.Service({protocol: 'Empty'});
      assert.equal(svc.name, 'Empty');
      assert.deepEqual(svc.messages, []);
    });

    test('namespacing', function () {
      var svc;

      svc = newService('foo.Foo', '');
      assert.equal(svc.name, 'foo.Foo');
      assert(svc.type('Bar'));
      assert(svc.type('Baz'));

      svc = newService('foo.Foo');
      assert.equal(svc.name, 'foo.Foo');
      assert(svc.type('foo.Bar'));
      assert(svc.type('Baz'));

      svc = newService('Foo', 'bar');
      assert.equal(svc.name, 'bar.Foo');
      assert(svc.type('bar.Bar'));
      assert(svc.type('Baz'));

      svc = newService('Foo', 'bar', {namespace: 'opt'});
      assert.equal(svc.name, 'bar.Foo');
      assert(svc.type('bar.Bar'));
      assert(svc.type('Baz'));

      svc = newService('Foo', undefined, {namespace: 'opt'});
      assert.equal(svc.name, 'opt.Foo');
      assert(svc.type('opt.Bar'));
      assert(svc.type('Baz'));

      svc = newService('.Foo', undefined, {namespace: 'opt'});
      assert.equal(svc.name, 'Foo');
      assert(svc.type('Bar'));
      assert(svc.type('Baz'));

      function newService(name, namespace, opts) {
        return new services.Service({
          protocol: name,
          namespace: namespace,
          types: [
            {type: 'record', name: 'Bar', fields: []},
            {type: 'record', name: '.Baz', fields: []}
          ]
        }, opts);
      }
    });

    test('createClient transport option', function (done) {
      var svc = Service.forProtocol({protocol: 'Empty'});
      svc.createClient({transport: new stream.PassThrough({objectMode: true})})
        .on('channel', function () { done(); });
    });

    test('createListener strict', function () {
      var svc = Service.forProtocol({protocol: 'Empty'});
      assert.throws(function () {
        svc.createListener(new stream.PassThrough(), {strictErrors: true});
      });
    });

    test('compatible', function () {
      var emptySvc = Service.forProtocol({protocol: 'Empty'});
      var pingSvc = Service.forProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'boolean'},
          pong: {request: [], response: 'int'},
          pung: {request: [], response: 'null', 'one-way': true}
        }
      });
      var pongSvc = Service.forProtocol({
        protocol: 'Pong',
        messages: {
          pong: {request: [], response: 'long'}
        }
      });
      var pungSvc = Service.forProtocol({
        protocol: 'Pong',
        messages: {pung: {request: [], response: 'null'}}
      });
      assert(Service.compatible(emptySvc, pingSvc));
      assert(!Service.compatible(pingSvc, emptySvc));
      assert(!Service.compatible(pingSvc, pongSvc));
      assert(Service.compatible(pongSvc, pingSvc));
      assert(!Service.compatible(pungSvc, pingSvc));
    });
  });

  suite('Message', function () {

    var Message = services.Message;

    test('empty errors', function () {
      var m = Message.forSchema('Hi', {
        request: [{name: 'greeting', type: 'string'}],
        response: 'int'
      });
      assert.deepEqual(m.errorType.toString(), '["string"]');
    });

    test('non-array request', function () {
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: 'string',
          response: 'int'
        });
      }, /invalid \w* request/);
    });

    test('missing response', function () {
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}]
        });
      }, /invalid \w* response/);
    });

    test('non-array errors', function () {
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'int',
          errors: 'int'
        });
      }, /invalid \w* error/);
    });

    test('invalid one-way', function () {
      // Non-null response.
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'string',
          'one-way': true
        });
      }, /inapplicable/);
      // Non-empty errors.
      assert.throws(function () {
        Message.forSchema('Hi', {
          request: [{name: 'greeting', type: 'string'}],
          response: 'null',
          errors: ['int'],
          'one-way': true
        });
      }, /inapplicable/);
    });

    test('getters', function () {
      var s1 = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null'
      };
      var s2 = {
        request: [],
        'one-way': true,
        response: 'null',
        doc: 'a random int'
      };
      var m = Message.forSchema('Ping', s1);
      assert.equal(m.name, 'Ping');
      assert.equal(m.requestType.fields[0].name, 'ping');
      assert.equal(m.responseType.branchName, 'null');
      assert.strictEqual(m.oneWay, false);
      assert.deepEqual(m.schema(), s1);
      assert.deepEqual(Message.forSchema('Pong', s2).schema(), s2);
    });

    test('get documentation', function () {
      var schema = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null',
        doc: 'Pong'
      };
      var m = Message.forSchema('Ping', schema);
      assert.equal(m.doc, 'Pong');
    });

    test('invalid types', function () {
      assert.throws(function () {
        new Message('intRequest', types.Type.forSchema('int'));
      }, /invalid request type/);
      assert.throws(function () {
        new Message(
          'intError',
          types.Type.forSchema({type: 'record', fields: []}),
          types.Type.forSchema('int')
        );
      }, /invalid error type/);
    });

    test('schema multiple errors', function () {
      var s = {
        request: [{name: 'ping', type: 'string'}],
        response: 'null',
        errors: ['int', 'bytes']
      };
      var m = Message.forSchema('Ping', s);
      assert.deepEqual(m.schema(), s);
    });
  });

  suite('FrameDecoder & FrameEncoder', function () {

    var FrameDecoder = services.streams.FrameDecoder;
    var FrameEncoder = services.streams.FrameEncoder;

    test('roundtrip', function (done) {
      var pktType = types.Type.forSchema({
        type: 'record',
        name: 'Packet',
        fields: [
          {name: 'headers', type: {type: 'map', values: 'bytes'}},
          {name: 'body', type: 'bytes'}
        ]
      });
      var n = 200;
      var src = [];
      while (n--) {
        var val = pktType.random();
        src.push(new Packet(0, val.headers, val.body));
      }
      var dst = [];
      var encoder = new FrameEncoder();
      var decoder = new FrameDecoder();
      createReadableStream(src)
        .pipe(encoder)
        .pipe(decoder)
        .pipe(createWritableStream(dst))
        .on('finish', function () {
          assert.deepEqual(dst, src);
          done();
        });
    });
  });

  suite('NettyDecoder & NettyEncoder', function () {

    var NettyDecoder = services.streams.NettyDecoder;
    var NettyEncoder = services.streams.NettyEncoder;

    test('roundtrip', function (done) {
      var pktType = types.Type.forSchema({
        type: 'record',
        name: 'Packet',
        fields: [
          {name: 'id', type: 'int'},
          {name: 'headers', type: {type: 'map', values: 'bytes'}},
          {name: 'body', type: 'bytes'}
        ]
      });
      var n = 200;
      var src = [];
      while (n--) {
        var val = pktType.random();
        src.push(new Packet(val.id, val.headers, val.body));
      }
      var dst = [];
      var encoder = new NettyEncoder();
      var decoder = new NettyDecoder();
      createReadableStream(src)
        .pipe(encoder)
        .pipe(decoder)
        .pipe(createWritableStream(dst))
        .on('finish', function () {
          assert.deepEqual(dst, src);
          done();
        });
    });
  });

  suite('Adapter', function () {

    var Adapter = services.Adapter;

    test('truncated request & response', function () {
      var s = Service.forProtocol({
        protocol: 'Echo',
        messages: {
          echo: {request: [{name: 's', type: 'string'}], response: 'string'}
        }
      });
      var a = new Adapter(s, s);
      assert.throws(function () {
        a._decodeRequest({body: new Buffer([24])}, {});
      }, /truncated/);
      assert.throws(function () {
        a._decodeResponse(
          {body: new Buffer([48]), headers: {}},
          {tags: {}},
          s.message('echo'),
          {}
        );
      }, /truncated/);
    });
  });

  suite('Registry', function () {

    var Registry = services.Registry;

    test('get', function (done) {
      var ctx = {one: 1};
      var reg = new Registry(ctx);
      var id = reg.add(200, function (err, two) {
        assert.strictEqual(this, ctx);
        assert.strictEqual(err, null);
        assert.equal(two, 2);
        assert.strictEqual(reg.get(id), undefined);
        done();
      });
      setTimeout(function () { reg.get(id)(null, 2); }, 50);
    });

    test('timeout', function (done) {
      var ctx = {one: 1};
      var reg = new Registry(ctx);
      var id = reg.add(10, function (err) {
        assert.strictEqual(this, ctx);
        assert(/timeout/.test(err));
        assert.strictEqual(reg.get(id), undefined);
        done();
      });
    });

    test('no timeout', function (done) {
      var ctx = {one: 1};
      var reg = new Registry(ctx);
      var id = reg.add(-1, function (err, two) {
        assert.strictEqual(this, ctx);
        assert.strictEqual(err, null);
        assert.equal(two, 2);
        assert.strictEqual(reg.get(id), undefined);
        done();
      });
      reg.get(id)(null, 2);
    });

    test('clear', function (done) {
      var ctx = {one: 1};
      var reg = new Registry(ctx);
      var n = 0;
      reg.add(20, fn);
      reg.add(20, fn);
      reg.clear();

      function fn(err) {
        assert(/interrupted/.test(err), err);
        if (++n == 2) {
          done();
        }
      }
    });

    test('mask', function (done) {
      var ctx = {one: 1};
      var n = 0;
      var reg = new Registry(ctx, 31);
      assert.equal(reg.add(10, fn), 1);
      assert.equal(reg.add(10, fn), 0);
      reg.get(4)(null);
      reg.get(3)(null);

      function fn(err) {
        assert.strictEqual(err, null);
        if (++n == 2) {
          done();
        }
      }
    });
  });

  suite('StatefulClientChannel', function () {

    test('connection timeout', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = createPassthroughTransports(true)[0];
      svc.createClient().createChannel(transport, {connectionTimeout: 10})
        .on('error', function (err) {
          assert(/timeout/.test(err), err);
          assert.strictEqual(this.client.service, svc);
          this.destroy();
        })
        .on('eot', function () { done(); });
    });

    test('ping', function (done) {
      var svc = Service.forProtocol({protocol: 'Ping' });
      var transport = {
        readable: new stream.PassThrough({objectMode: true}),
        writable: new stream.PassThrough({objectMode: true})
      };
      var channel = svc.createClient()
        .createChannel(transport, {noPing: true, objectMode: true})
        .on('eot', function () { done(); });
      channel.ping(10, function (err) {
        assert(/timeout/.test(err), err);
        channel.destroy();
      });
    });

    test('readable ended', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports();
      svc.createClient()
        .createChannel(transports[0], {codec: 'netty'})
        .on('eot', function () { done(); });
      transports[0].readable.push(null);
    });

    test('writable finished', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      var transports = createPassthroughTransports(true);
      svc.createClient()
        .createChannel(transports[0])
        .on('eot', function () { done(); });
      transports[0].writable.end();
    });

    test('keep writable open', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      var transports = createPassthroughTransports(true);
      svc.createClient()
        .createChannel(transports[0], {endWritable: false})
        .on('eot', function () {
          transports[0].writable.write({}); // Doesn't fail.
          done();
        })
        .destroy();
    });

    test('discover service', function (done) {
      // Check that we can interrupt a handshake part-way, so that we can ping
      // a remote server for its service, but still reuse the same connection
      // for a later transmission.
      var svc1 = Service.forProtocol({protocol: 'Empty'});
      var svc2 = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });

      var transports = createPassthroughTransports();
      var server2 = svc2.createServer()
        .onPing(function (cb) { cb(null, true); });
      var chn1 = server2.createChannel(transports[0], {codec: 'netty'});
      assert.strictEqual(chn1.server.service, svc2);
      svc1.createClient()
        .createChannel(transports[1], {codec: 'netty', endWritable: false})
        .on('handshake', function (hreq, hres) {
          this.destroy();
          assert.equal(hres.serverProtocol, JSON.stringify(svc2.protocol));
        })
        .on('eot', function () {
          // The transports are still available for a connection.
          var client = svc2.createClient();
          var chn2 = client.createChannel(transports[1], {codec: 'netty'});
          client.ping(function (err, res) {
            assert.strictEqual(err, null);
            assert.strictEqual(res, true);
            chn2.on('eot', function () { done(); }).destroy();
          });
        });
    });

    test('trailing decoder', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports();
      svc.createClient()
        .createChannel(transports[0], {codec: 'netty', noPing: true})
        .on('error', function (err) {
          assert(/trailing/.test(err), err);
          done();
        });
      transports[0].readable.end(new Buffer([48]));
    });
  });

  suite('StatelessClientChannel', function () {

    test('factory error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var client = svc.createClient({strictTypes: true});
      var chn = client.createChannel(function (pkt, cb) {
        cb(new Error('foobar'));
      }, {noPing: true});
      client.ping(function (err) {
        assert(/foobar/.test(err.string), err);
        assert(!chn.destroyed);
        done();
      });
    });

    test('factory error no writable', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var client = svc.createClient();
      var chn = client.createChannel(
        function () {},
        {codec: 'frame', noPing: true}
      );
      client.ping(function (err) {
        assert(/no writable transport/.test(err), err);
        assert(!chn.destroyed);
        done();
      });
    });

    test('trailing data', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var client = svc.createClient();
      var readable = new stream.PassThrough();
      client.createChannel(function (cb) {
        cb(null, readable);
        return new stream.PassThrough();
      }, {codec: 'frame', noPing: true});
      client.ping(function (err) {
        assert(/trailing data/.test(err), err);
        done();
      });
      readable.end(new Buffer([48]));
    });

    test('frame encoder error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      }, {wrapUnions: true});
      var client = svc.createClient({strictTypes: true});
      var chn = client.createChannel(function (cb) {
        return new stream.PassThrough()
          .on('finish', function () { cb(new Error('foobar')); });
      }, {codec: 'frame', noPing: true});
      client.ping(function (err) {
        assert(/foobar/.test(err.string));
        assert(!chn.destroyed);
        done();
      });
    });

    test('interrupt writable', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'null'}}
      });
      // Fake handshake response.
      var hres = Service.HANDSHAKE_RESPONSE_TYPE.clone({
        match: 'NONE',
        serverService: JSON.stringify(svc.protocol),
        serverHash: svc.hash
      });
      var numHandshakes = 0;
      var client = svc.createClient();
      client.createChannel(function (reqPkt, cb) {
        cb(null, {handshake: hres.wrap()});
      }).on('handshake', function (hreq, actualHres) {
        numHandshakes++;
        assert.deepEqual(actualHres, hres);
        this.destroy({noWait: true});
      }).on('error', function (err) {
        assert(/interrupted/.test(err), err);
      });
      setTimeout(function () {
        // Only a single handshake should have occurred.
        assert.equal(numHandshakes, 1);
        done();
      }, 50);
    });
  });

  suite('StatefulServerChannel', function () {

    test('readable ended', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = new stream.PassThrough();
      svc.createServer().createChannel(transport).on('eot', function () {
        assert(this.destroyed);
        done();
      });
      transport.push(null);
    });

    test('readable trailing data', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = new stream.PassThrough();
      svc.createServer().createChannel(transport, {codec: 'netty'})
        .on('error', function (err) {
          assert(/trailing/.test(err), err);
          done();
        });
      transport.end(new Buffer([48]));
    });

    test('writable finished', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      // We must use object mode here since ending the encoding stream won't
      // end the underlying writable stream.
      var transports = createPassthroughTransports(true);
      svc.createServer().createChannel(transports[0])
        .on('eot', function () { done(); });
      transports[0].writable.end();
    });
  });

  suite('StatelessServerChannel', function () {

    test('trailing frame data', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean', errors: ['int']}}
      });
      var transports = createPassthroughTransports();
      svc.createServer({silent: true}).createChannel(function (fn) {
        fn(null, transports[0].writable);
        return transports[1].readable;
      }, {codec: 'frame'}).on('error', function (err) {
        assert(/trailing/.test(err), err);
        done();
      });
      transports[1].readable.end(new Buffer([48]));
    });
  });

  suite('Client', function () {

    test('no available channels', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var client = svc.createClient()
        .on('error', function (err) {
          assert(/no available channels/.test(err), err);
          done();
        });
      assert.strictEqual(client.service, svc);
      // With callback.
      client.ping(function (err) {
        assert(/no available channels/.test(err), err);
        assert.strictEqual(this.channel, undefined);
        // Without (triggering the error above).
        client.ping();
      });
    });

    test('destroy emitters', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };
      var client = svc.createClient();
      client.createChannel(transport)
        .on('eot', function () {
          done();
        });
      client.destroyChannels({noWait: true});
    });

    test('custom policy', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports(true);
      var client = svc.createClient({channelPolicy: policy});
      var channels = [
        client.createChannel(transports[0]),
        client.createChannel(transports[0])
      ];
      client.ping();

      function policy(channels_) {
        assert.deepEqual(channels_, channels);
        done();
      }
    });

    test('emit with channel', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transports = createPassthroughTransports(true);
      // This policy is invalid, calls without explicit channels will fail.
      var client = svc.createClient({channelPolicy: function () { return 1;}});
      var channel = client.createChannel(transports[0]);
      svc.createServer()
        .onPing(function (cb) { cb(null, true); })
        .createChannel(transports[1]);
      client.ping(function (err) {
        assert(/invalid channel/.test(err), err);
        client.ping({channel: channel}, function (err, res) {
          assert(!err, err);
          assert.strictEqual(res, true);
          done();
        });
      });
    });

    test('remote protocols existing', function () {
      var ptcl1 = Service.forProtocol({protocol: 'Empty1'});
      var ptcl2 = Service.forProtocol({protocol: 'Empty2'});
      var remotePtcls = {abc: ptcl2.protocol};
      var client = ptcl1.createClient({
        remoteFingerprint: 'abc',
        remoteProtocols: remotePtcls
      });
      assert.deepEqual(client.remoteProtocols(), remotePtcls);
    });

    test('missing message', function () {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'int'}}
      });
      var client = svc.createClient();
      assert.throws(
        function () { client.emitMessage('foo'); },
        /unknown message/
      );
    });

    test('custom policy', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var transport = {
        readable: new stream.PassThrough(),
        writable: new stream.PassThrough()
      };

      var client = svc.createClient({channelPolicy: policy});
      var channels = [
        client.createChannel(transport, {noPing: true}),
        client.createChannel(transport, {noPing: true})
      ];
      client.ping();

      function policy(channels_) {
        assert.deepEqual(channels_, channels);
        done();
      }
    });

    test('remote protocols existing', function () {
      var ptcl1 = Service.forProtocol({protocol: 'Empty1'});
      var ptcl2 = Service.forProtocol({protocol: 'Empty2'});
      var remotePtcls = {abc: ptcl2.protocol};
      var client = ptcl1.createClient({
        remoteFingerprint: 'abc',
        remoteProtocols: remotePtcls
      });
      assert.deepEqual(client.remoteProtocols(), remotePtcls);
    });

    test('invalid response', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'int'}}
      });
      var transports = createPassthroughTransports(true);
      var client = svc.createClient();
      client.createChannel(transports[0], {noPing: true});
      client.ping(function (err) {
        assert(/truncated/.test(err), err);
        done();
      });
      setTimeout(function () {
        // "Send" an invalid payload (negative union offset). We wait to allow
        // the callback for the above message to be registered.
        transports[0].readable.write({id: 1, body: new Buffer([45])});
      }, 0);
    });
  });

  suite('Server', function () {

    test('get channels', function (done) {
      var svc = Service.forProtocol({protocol: 'Empty1'});
      var server = svc.createServer();
      var channels = [
        server.createChannel(createPassthroughTransports(true)[0]),
        server.createChannel(createPassthroughTransports(true)[0])
      ];
      assert.deepEqual(server.activeChannels(), channels);
      channels[0]
        .on('eot', function () {
          assert.deepEqual(server.activeChannels(), [channels[1]]);
          done();
        })
        .destroy();
    });

    test('remote protocols', function () {
      var svc1 = Service.forProtocol({protocol: 'Empty1'});
      var svc2 = Service.forProtocol({protocol: 'Empty2'});
      var remotePtcls = {abc: svc2.protocol};
      var server = svc1.createServer({remoteProtocols: remotePtcls});
      assert.deepEqual(server.remoteProtocols(), remotePtcls);
    });

    test('no capitalization', function () {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'boolean'}}
      });
      var server = svc.createServer({noCapitalize: true});
      assert(!server.onPing);
      assert(typeof server.onping == 'function');
    });

    test('stateful transport reuse', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'int'}}
      });
      var transports = createPassthroughTransports(true);
      var server = svc.createServer()
        .onPing(function (cb) {
          cb(null, 1);
        });
      var channel = server.createChannel(transports[0], {endWritable: false});
      var client = svc.createClient()
        .once('channel', function () {
          this.ping(function (err, n) {
            // At this point the handshake has succeeded.
            // Check that the response is as expected.
            assert(!err, err);
            assert.equal(n, 1);
            channel.destroy(); // Destroy the server's channel (instant).
            this.channel.destroy(); // Destroy the client's channel (instant).
            // We can now reuse the transports.
            server.createChannel(transports[0]);
            client
              .once('channel', function () {
                this.ping(function (err, n) {
                  assert(!err, err);
                  assert.equal(n, 1);
                  done();
                });
              })
              .createChannel(transports[1]);
          });
        });
      // Create the first channel.
      client.createChannel(transports[1], {endWritable: false});
    });

    test('interrupted', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {ping: {request: [], response: 'int'}}
      });
      var server = svc.createServer()
        .onPing(function (cb) {
          this.channel.destroy({noWait: true});
          cb(null, 1); // Still call the callback to make sure it is ignored.
        });
      svc.createClient({server: server})
         .ping({timeout: 10}, function (err) {
            assert(/timeout/.test(err), err);
            done();
          });
    });
  });

  suite('clients & servers', function () {

    test('create client with server', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Echo',
        messages: {
          echo: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .onEcho(function (n, cb) { cb(null, n); });
      svc.createClient({server: server})
        .echo(123, function (err, n) {
          assert(!err, err);
          assert.equal(n, 123);
          done();
        });
    });

    test('middleware deadline', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Echo',
        messages: {
          echo: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var pending = 1;
      var server = svc.createServer();
      svc.createClient({server: server})
        .use(function (wreq, wres, next) {
          setTimeout(function () {
            next(null, function (err, prev) {
              pending--;
              prev(err);
            });
          }, 75);
        })
        .use(function (wreq, wres, next) {
          assert(false); // Shouldn't be called.
          next();
        })
        .echo(123, {timeout: 50}, function (err) {
          assert(/deadline exceeded/.test(err), err);
          assert(!pending);
          done();
        });
    });

    test('frame codec', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Echo',
        messages: {
          echo: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .onEcho(function (n, cb) { cb(null, n); });
      var client = svc.createClient();
      client.createChannel(function (cb) {
        var transports = createPassthroughTransports();
        server.createChannel(function (cb) {
          cb(null, transports[0].writable);
          return transports[0].readable;
        }, {codec: 'frame'});
        cb(null, transports[1].readable);
        return transports[1].writable;
      }, {codec: 'frame'});
      client
        .echo(123, function (err, n) {
          assert(!err, err);
          assert.equal(n, 123);
          done();
        });
    });

    test('create client with server destroy channels', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Echo',
        messages: {
          echo: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer();
      var client = svc.createClient({server: server});
      client.activeChannels()[0].on('eot', function () { done(); });
      server.destroyChannels({noWait: true});
    });

    test('client context call options', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .onNeg(function (n, cb) { cb(null, -n); });
      var ctx = {id: 123};
      var client = svc.createClient({server: server});
      var channel = client.activeChannels()[0];
      channel.on('outgoingRequestPre', function (wreq, wres, ctx) {
        ctx.id = 123;
      });
      client.neg(1, ctx, function (err, n) {
        assert(!err, err);
        assert.equal(n, -1);
        assert.equal(this.id, 123);
        done();
      });
    });

    test('server call constant context', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var numCalls = 0;
      var server = svc.createServer()
        .use(function (wreq, wres, next) {
          // Check that middleware have the right context.
          this.id = 123;
          assert.equal(numCalls++, 0);
          next(null, function (err, prev) {
            assert(!err, err);
            assert.equal(this.id, 456);
            assert.equal(numCalls++, 2);
            prev(err);
          });
        })
        .onNeg(function (n, cb) {
          assert.equal(this.id, 123);
          this.id = 456;
          assert.equal(numCalls++, 1);
          cb(null, -n);
        });
      svc.createClient({server: server})
        .neg(1, function (err, n) {
          assert(!err, err);
          assert.equal(n, -1);
          assert.equal(numCalls, 3);
          done();
        });
    });

    test('server call context options', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .on('channel', function (channel) {
          channel.on('incomingRequestPre', function (wreq, wres, ctx) {
            ctx.one = 1;
          });
        })
        .use(function (wreq, wres, next) {
          assert.deepEqual(this.one, 1);
          next();
        })
        .onNeg(function (n, cb) {
          assert.deepEqual(this.one, 1);
          cb(null, -n);
        });
      svc.createClient({server: server})
        .neg(1, function (err, n) {
          assert(!err, err);
          assert.equal(n, -1);
          done();
        });
    });

    test('server default handler', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'},
          abs: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer({defaultHandler: defaultHandler})
        .onNeg(function (n, cb) { cb(null, -n); });

      svc.createClient({server: server})
        .neg(1, function (err, n) {
          assert(!err, err);
          assert.equal(n, -1);
          this.channel.client.abs(5, function (err, n) {
            assert(!err, err);
            assert.equal(n, 10);
            done();
          });
        });

      function defaultHandler(wreq, wres, cb) {
        // jshint -W040
        wres.response = 10;
        cb();
      }
    });

    test('client middleware bypass', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .onNeg(function (n, cb) { cb(null, -n); });
      var isCalled = false;
      svc.createClient({server: server})
        .use(function (wreq, wres, next) {
          wres.response = -3;
          next();
        })
        .use(function (wreq, wres, next) {
          isCalled = true;
          next();
        })
        .neg(1, function (err, n) {
          assert(!err, err);
          assert.equal(n, -3);
          assert(!isCalled);
          done();
        });
    });

    test('client middleware override error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .onNeg(function (n, cb) { cb(null, -n); });
      svc.createClient({server: server})
        .use(function (wreq, wres, next) {
          next(null, function (err, prev) {
            assert(/bar/.test(err), err);
            prev(null); // Substitute `null` as error.
          });
        })
        .use(function (wreq, wres, next) {
          next(null, function (err, prev) {
            assert(!err, err);
            assert.equal(wres.response, -2);
            prev(new Error('bar'));
          });
        })
        .neg(2, function (err) {
          assert.strictEqual(err, null);
          done();
        });
    });

    test('server middleware bypass', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var handlerCalled = false;
      var errorTriggered = false;
      var server = svc.createServer()
        .on('error', function (err) {
          assert(/foobar/.test(err.cause), err);
          errorTriggered = true;
        })
        .use(function (wreq, wres, next) {
          wres.error = 'foobar';
          next();
        })
        .onNeg(function (n, cb) {
          handlerCalled = true;
          cb(null, -n);
        });
      svc.createClient({server: server})
        .neg(1, function (err) {
          assert(/foobar/.test(err), err);
          assert(!handlerCalled);
          setTimeout(function () {
            assert(errorTriggered);
            done();
          }, 0);
        });
    });

    test('dynamic middleware', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var server = svc.createServer()
        .call(function (server) {
          server
            .on('channel', function (channel) {
              channel.on('incomingRequestPre', function (wreq, wres, ctx) {
                ctx.foo = 'bar';
              });
            })
            .use(function (wreq, wres, next) {
              wreq.request.n = 3;
              next();
            });
        })
        .onNeg(function (n, cb) {
          assert.equal(this.foo, 'bar');
          cb(null, -n);
        });
      svc.createClient({server: server})
        .call(function (client) {
          client.activeChannels()[0]
            .on('outgoingRequestPre', function (wreq, wres, ctx) {
              ctx.two += 1;
            });
          client.use(function (wreq, wres, next) { next(); });
        })
        .neg(1, {two: 2}, function (err, n) {
          assert(!err, err);
          assert.equal(n, -3);
          assert.equal(this.two, 3);
          done();
        });
    });

    test('client non-strict error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {
            request: [{name: 'n', type: 'int'}],
            response: 'int',
            errors: ['int']
          }
        }
      }, {wrapUnions: true});
      var server = svc.createServer({silent: true})
        .onNeg(function (n, cb) { cb(n ? {int: n} : new Error('foo')); });
      var client = svc.createClient({server: server});
      client.neg(1, function (err) {
          assert.deepEqual(err, {int: 1});
          client.neg(0, function (err) {
            assert(/foo/.test(err), err);
            done();
          });
        });
    });

    test('server non-strict error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Math',
        messages: {
          neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
        }
      });
      var errorTriggered = false;
      var server = svc.createServer()
        .on('error', function () {
          errorTriggered = true;
        })
        .onNeg(function (n, cb) {
          cb(null, -n);
        });
      svc.createClient({server: server})
        .neg(1, function (err, n) {
          assert(!err, err);
          assert.equal(n, -1);
          setTimeout(function () {
            assert(!errorTriggered);
            done();
          }, 0);
        });
    });

    test('server one-way middleware error', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Push',
        messages: {
          push: {
            request: [{name: 'n', type: 'int'}],
            response: 'null',
            'one-way': true
          }
        }
      });
      var server = svc.createServer()
        .on('error', function (err) {
          assert(/foobar/.test(err), err);
          done();
        })
        .use(function (wreq, wres, next) {
          next(new Error('foobar'));
        });
      svc.createClient({server: server}).push(1);
    });

    test('stateful connected one-way message', function (done) {
      var svc = Service.forProtocol({
        protocol: 'Ping',
        messages: {
          ping: {request: [], response: 'null', 'one-way': true}
        }
      });
      var transports = createPassthroughTransports(true);
      var client = svc.createClient();
      client.createChannel(transports[0]);
      var server = svc.createServer();
      server.createChannel(transports[1]);
      server.onPing(function (cb) {
        assert.strictEqual(cb, undefined);
        done();
      });
      client.ping();
    });

    suite('stateful', function () {

      run(function (clientPtcl, serverPtcl, opts, cb) {
        if (!cb) {
          cb = opts;
          opts = undefined;
        }
        var codec = null;
        opts = opts || {};
        opts.codec = codec;
        opts.silent = true;
        var transports = createPassthroughTransports(true);
        var client = clientPtcl.createClient(opts);
        client.createChannel(transports[0], {codec: codec});
        var server = serverPtcl.createServer(opts);
        server.createChannel(transports[1], opts);
        cb(client, server);
      });

    });

    suite('stateless', function () {

      run(function (clientPtcl, serverPtcl, opts, cb) {
        if (!cb) {
          cb = opts;
          opts = undefined;
        }
        opts = opts || {};
        opts.silent = true;
        var client = clientPtcl.createClient(opts);
        var server = serverPtcl.createServer(opts);
        server.createChannel(function (fn) { client.createChannel(fn); });
        cb(client, server);
      });

    });

    function run(setupFn) {

      test('primitive types', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            negateFirst: {
              request: [{name: 'ns', type: {type: 'array', items: 'int'}}],
              response: 'long'
            }
          }
        });
        setupFn(svc, svc, function (client, server) {
          server
            .onNegateFirst(function (ns, cb) {
              assert.strictEqual(this.channel.server, server);
              cb(null, -ns[0]);
            });
          var channel = client.activeChannels()[0];
          channel.on('eot', function () {
              done();
            })
            .once('handshake', function (hreq, hres) {
              // Allow the initial ping to complete.
              assert.equal(hres.match, 'BOTH');
              process.nextTick(function () {
                client.negateFirst([20], function (err, res) {
                  assert.equal(this.channel, channel);
                  assert.strictEqual(err, null);
                  assert.equal(res, -20);
                  client.negateFirst([-10, 'ni'],  function (err) {
                    assert(/invalid "int"/.test(err), err);
                    this.channel.destroy();
                  });
                });
              });
            });
        });
      });


      test('invalid strict error', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            sqrt: {
              request: [{name: 'n', type: 'float'}],
              response: 'float'
            }
          }
        });
        setupFn(svc, svc, {strictTypes: true}, function (client, server) {
          server.onSqrt(function (n, cb) {
            if (n === -1) {
              cb(new Error('no i')); // Invalid error (should be a string).
            } else if (n < 0) {
              throw new Error('negative');
            } else {
              cb(undefined, Math.sqrt(n));
            }
          });
          client.sqrt(-1, function (err) {
            assert(/internal server error/.test(err), err);
            client.sqrt(-2, function (err) {
              assert(/internal server error/.test(err), err);
              client.sqrt(100, function (err, res) {
                // The server still doesn't die (we can make a new request).
                assert.strictEqual(err, undefined);
                assert(Math.abs(res - 10) < 1e-5);
                done();
              });
            });
          });
        });
      });

      test('non-strict response', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Ping',
          messages: {
            ping: {request: [], response: 'null'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          server.onPing(function (cb) { cb(); });
          client.ping(function (err) {
            assert(!err, err);
            done();
          });
        });
      });

      test('invalid strict response', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Ping',
          messages: {
            ping: {request: [], response: 'null'}
          }
        });
        setupFn(svc, svc, {strictTypes: true}, function (client, server) {
          server.onPing(function (cb) { cb(); });
          client.ping(function (err) {
            assert(/internal server error/.test(err), err);
            done();
          });
        });
      });

      test('server duplicate handler call', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          var pending = 2;
          server
            .onNeg(function (n, cb) {
              cb(null, -n);
              cb(null, -n);
            })
            .on('error', function (err) {
              assert(/duplicate handler/.test(err), err);
              step();
            });
          // The client should still get a response.
          client.neg(2, function (err, res) {
            assert.equal(res, -2);
            step();
          });

          function step() {
            if (--pending === 0) {
              done();
            }
          }
        });
      });

      test('server tags', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          server
            .use(function (wreq, wres, next) {
              wres.tags.foo = 'abc';
              next();
            })
            .onNeg(function (n, cb) { cb(null, -n); });
          var t = types.Type.forSchema('string');
          server.tagTypes.foo = client.tagTypes.foo = t;
          client
            .use(function (wreq, wres, next) {
              next(null, function (err, prev) {
                assert.equal(wres.tags.foo, 'abc');
                prev(err);
              });
            })
            .neg(2, function (err, res) {
              assert.strictEqual(err, null);
              assert.equal(res, -2);
              done();
            });
        });
      });

      test('client middleware', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          server.onNeg(function (n, cb) { cb(null, -n); });
          var buf = new Buffer([0, 1]);
          var isDone = false;
          var channel = client.activeChannels()[0];
          client.tagTypes.buf = types.Type.forSchema('bytes');
          client
            .use(function (wreq, wres, next) {
              // No callback.
              assert.strictEqual(this.channel, channel);
              assert.deepEqual(wreq.tags, {});
              wreq.tags.buf = buf;
              assert.deepEqual(wreq.request, {n: 2});
              next();
            })
            .use(function (wreq, wres, next) {
              // Callback here.
              assert.deepEqual(wreq.tags, {buf: buf});
              wreq.request.n = 3;
              next(null, function (err, prev) {
                assert(!err, err);
                assert(!wres.error, wres.error);
                assert.strictEqual(this.channel, channel);
                assert.deepEqual(wres.response, -3);
                isDone = true;
                prev();
              });
            })
            .neg(2, function (err, res) {
              assert.strictEqual(err, null);
              assert.equal(res, -3);
              assert(isDone);
              done();
            });
        });
      });

      test('client middleware forward error', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          server.onNeg(function (n, cb) { cb(null, -n); });
          var fwdErr = new Error('forward!');
          var bwdErr = new Error('backward!');
          var called = false;
          client
            .use(function (wreq, wres, next) {
              next(null, function (err, prev) {
                assert.strictEqual(err, fwdErr);
                assert(!called);
                prev(bwdErr); // Substitute the error.
              });
            })
            .use(function (wreq, wres, next) {
              next(fwdErr, function (err, prev) {
                called = true;
                prev();
              });
            })
            .neg(2, function (err) {
              assert.strictEqual(err, bwdErr);
              done();
            });
        });
      });

      test('client middleware duplicate forward calls', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          server.onNeg(function (n, cb) { cb(null, -n); });
          var chn = client.activeChannels()[0];
          client
            .on('error', function (err, chn_) {
              assert(/duplicate forward middleware/.test(err), err);
              assert.strictEqual(chn_, chn);
              setTimeout(function () { done(); }, 0);
            });
          client
            .use(function (wreq, wres, next) {
              next();
              next();
            })
            .neg(2, function (err, res) {
              assert.equal(res, -2);
            });
        });
      });

      test('server middleware', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          var isDone = false;
          var buf = new Buffer([0, 1]);
          // The server's channel won't be ready right away in the case of
          // stateless transports.
          var t = types.Type.forSchema('bytes');
          client.tagTypes.buf = server.tagTypes.buf = t;
          var channel;
          server
            .use(function (wreq, wres, next) {
              channel = this.channel;
              assert.strictEqual(channel.server, server);
              assert.deepEqual(wreq.request, {n: 2});
              next(null, function (err, prev) {
                assert.strictEqual(this.channel, channel);
                wres.tags.buf = buf;
                prev();
              });
            })
            .onNeg(function (n, cb) { cb(null, -n); });
          client
            .use(function (wreq, wres, next) {
              next(null, function (err, prev) {
                assert.deepEqual(wres.tags, {buf: buf});
                isDone = true;
                prev();
              });
            })
            .neg(2, function (err, res) {
              assert.strictEqual(err, null);
              assert.equal(res, -2);
              assert(isDone);
              done();
            });
        });
      });

      test('server middleware duplicate backward calls', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          server
            .use(function (wreq, wres, next) {
              // Attach error handler to channel.
              server.on('error', function (err, chn) {
                assert(/duplicate backward middleware/.test(err), err);
                assert.strictEqual(chn.server. server);
                setTimeout(function () { done(); }, 0);
              });
              next(null, function (err, prev) {
                prev();
                prev();
              });
            })
            .onNeg(function (n, cb) { cb(null, -n); });
          client
            .neg(2, function (err, res) {
              assert.strictEqual(err, null);
              assert.equal(res, -2);
            });
        });
      });

      test('error formatter', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        var opts = {systemErrorFormatter: formatter};
        var barErr = new Error('baribababa');
        setupFn(svc, svc, opts, function (client, server) {
          server
            .onNeg(function () { throw barErr; });
          client
            .neg(2, function (err) {
              assert(/FOO/.test(err));
              done();
            });
        });

        function formatter(err) {
          assert.strictEqual(err, barErr);
          return 'FOO';
        }
      });

      test('remote protocols', function (done) {
        var clientPtcl = {
          protocol: 'Math1',
          foo: 'bar', // Custom attribute.
          doc: 'hi',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'long'}
          }
        };
        var serverPtcl = {
          protocol: 'Math2',
          doc: 'hey',
          bar: 'foo', // Custom attribute.
          messages: {
            neg: {request: [{name: 'n', type: 'long'}], response: 'int'}
          }
        };
        var clientSvc = Service.forProtocol(clientPtcl);
        var serverSvc = Service.forProtocol(serverPtcl);
        setupFn(clientSvc, serverSvc, function (client, server) {
          server
            .onNeg(function (n, cb) { cb(null, -n); });
          client
            .neg(2, function (err, res) {
              assert(!err, err);
              assert.equal(res, -2);
              var remotePtcl;
              // Client.
              remotePtcl = {};
              remotePtcl[serverSvc.hash] = serverPtcl;
              assert.deepEqual(client.remoteProtocols(), remotePtcl);
              // Server.
              remotePtcl = {};
              remotePtcl[clientSvc.hash] = clientPtcl;
              assert.deepEqual(server.remoteProtocols(), remotePtcl);
              done();
            });
        });
      });

      test('client remote error no callback', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Ping',
          messages: {ping: {request: [], response: 'int'}}
        });
        setupFn(svc, svc, function (client) {
          client
            .on('error', function (err) {
              assert(/not implemented/.test(err), err);
              done();
            })
            .ping();
        });
      });

      test('server error after handler', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Math',
          messages: {
            neg: {request: [{name: 'n', type: 'int'}], response: 'int'}
          }
        });
        setupFn(svc, svc, function (client, server) {
          var numErrors = 0;
          server
            .on('error', function (err) {
              numErrors++;
              assert(/bar/.test(err), err);
            })
            .onNeg(function (n, cb) {
              cb(null, -n);
              throw new Error('bar');
            });
          client.neg(2, function (err, n) {
            assert(!err, err);
            assert.equal(n, -2);
            setTimeout(function () {
              assert.equal(numErrors, 1);
              done();
            }, 0);
          });
        });
      });

      test('client channel destroy no wait', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Delay',
          messages: {
            wait: {
              request: [{name: 'ms', type: 'int'}],
              response: 'string'
            }
          }
        });
        var interrupted = 0;
        setupFn(svc, svc, function (client, server) {
          server.onWait(function (ms, cb) {
            setTimeout(function () { cb(null, 'ok'); }, ms);
          });
          var channel = client.activeChannels()[0];
          channel.on('eot', function (pending) {
            assert.equal(pending, 2);
            setTimeout(function () {
              assert.equal(interrupted, 2);
              done();
            }, 5);
          });
          client.wait(75, interruptedCb);
          client.wait(50, interruptedCb);
          client.wait(10, function (err, res) {
            assert.equal(res, 'ok');
            channel.destroy({noWait: true});
          });

          function interruptedCb(err) {
            assert(/interrupted/.test(err), err);
            interrupted++;
          }
        });
      });

      test('out of order requests', function (done) {
        var svc = Service.forProtocol({
          protocol: 'Delay',
          messages: {
            w: {
              request: [
                {name: 'ms', type: 'float'},
                {name: 'id', type: 'string'}
              ],
              response: 'string'
            }
          }
        });
        var ids = [];
        setupFn(svc, svc, function (client, server) {
          server.onW(function (delay, id, cb) {
            if (delay < 0) {
              cb('delay must be non-negative');
              return;
            }
            setTimeout(function () { cb(null, id); }, delay);
          });
          var channel = client.activeChannels()[0];
          channel.on('eot', function (pending) {
            assert.equal(pending, 0);
            assert.deepEqual(ids, [undefined, 'b', 'a']);
            done();
          }).once('handshake', function (hreq, hres) {
            assert.equal(hres.match, 'BOTH');
            process.nextTick(function () {
              client.w(500, 'a', function (err, res) {
                assert.strictEqual(err, null);
                ids.push(res);
              });
              client.w(10, 'b', function (err, res) {
                assert.strictEqual(err, null);
                ids.push(res);
                channel.destroy();
              });
              client.w(-10, 'c', function (err, res) {
                assert(/non-negative/.test(err));
                ids.push(res);
              });
            });
          });
        });
      });
    }
  });

  suite('discover attributes', function () {

    var discoverProtocol = services.discoverProtocol;

    test('stateful ok', function (done) {
      var schema = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      var svc = Service.forProtocol(schema);
      var server = svc.createServer()
        .onUpper(function (str, cb) {
          cb(null, str.toUpperCase());
        });
      var transports = createPassthroughTransports(true);
      server.createChannel(transports[1]);
      discoverProtocol(transports[0], function (err, actualAttrs) {
        assert.strictEqual(err, null);
        assert.deepEqual(actualAttrs, schema);
        // Check that the transport is still usable.
        var client = svc.createClient();
        client.createChannel(transports[0])
          .on('eot', function() {
            done();
          });
        client.upper('foo', function (err, res) {
          assert.strictEqual(err, null);
          assert.equal(res, 'FOO');
          this.channel.destroy();
        });
      });
    });

    test('stateful wrong scope', function (done) {
      var schema = {
        protocol: 'Case',
        messages: {
          upper: {
            request: [{name: 'str', type: 'string'}],
            response: 'string'
          }
        }
      };
      var svc = Service.forProtocol(schema);
      var scope = 'bar';
      var transports = createPassthroughTransports(true);
      svc.createServer({silent: true})
        .createChannel(transports[1], {scope: scope});
      discoverProtocol(transports[0], {timeout: 5}, function (err) {
        assert(/timeout/.test(err), err);
        // Check that the transport is still usable.
        var client = svc.createClient();
        var chn = client.createChannel(transports[0], {scope: scope})
          .on('eot', function() { done(); });
        client.upper('foo', function (err) {
          assert(/not implemented/.test(err), err);
          chn.destroy();
        });
      });
    });
  });
});

// Helpers.

function createPassthroughTransports(objectMode) {
  var pt1 = stream.PassThrough({objectMode: objectMode});
  var pt2 = stream.PassThrough({objectMode: objectMode});
  return [{readable: pt1, writable: pt2}, {readable: pt2, writable: pt1}];
}

// Simplified stream constructor API isn't available in earlier node versions.

function createReadableStream(bufs) {
  var n = 0;
  function Stream() { stream.Readable.call(this, {objectMode: true}); }
  util.inherits(Stream, stream.Readable);
  Stream.prototype._read = function () {
    this.push(bufs[n++] || null);
  };
  var readable = new Stream();
  return readable;
}

function createWritableStream(bufs) {
  function Stream() { stream.Writable.call(this, {objectMode: true}); }
  util.inherits(Stream, stream.Writable);
  Stream.prototype._write = function (buf, encoding, cb) {
    bufs.push(buf);
    cb();
  };
  return new Stream();
}
