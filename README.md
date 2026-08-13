# Delphi client for NATS, the cloud native messaging system.


![Delphi For NATS](https://user-images.githubusercontent.com/4686497/177811743-be0f8bfc-5672-4c3d-84e3-7b39122c9d3f.png)


[Delphi](https://www.embarcadero.com/products/delphi) **native** implementation of a [NATS](https://nats.io) Client Library, supporting both **Core NATS** — publish/subscribe, queue groups, request/reply, headers — and **JetStream**: stream and consumer management, publish with acknowledgement, pull and push consumption, Key/Value and Object Store.

![Repo Created At](https://img.shields.io/github/created-at/paolo-rossi/nats.delphi)
[![License MIT][License-Image]][License-Url]
![Commit Activity](https://img.shields.io/github/commit-activity/m/paolo-rossi/nats.delphi)
![GitHub Contributors](https://img.shields.io/github/contributors/paolo-rossi/nats.delphi)


[License-Url]: https://opensource.org/licenses/MIT
[License-Image]: https://img.shields.io/badge/License-MIT-blue.svg

## Documentation

| | |
|---|---|
| **[Docs/Core-API.md](Docs/Core-API.md)** | Full Core NATS reference — every method, its signature and an example |
| **[Docs/JetStream-API.md](Docs/JetStream-API.md)** | Full JetStream reference — streams, consumers, acks, Key/Value, Object Store |

The rest of this page is the tour.

## Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Connecting](#connecting)
- [Publishing](#publishing)
- [Subscribing](#subscribing)
- [Request / Reply](#request--reply)
- [Headers](#headers)
- [JetStream](#jetstream)
- [Threading](#threading)
- [Errors and disconnects](#errors-and-disconnects)
- [Timeouts and keep-alive](#timeouts-and-keep-alive)
- [Pluggable sockets](#pluggable-sockets)
- [Demos](#demos)
- [Tests](#tests)
- [Status and limitations](#status-and-limitations)

## Requirements

- RAD Studio 12 Athens or 13. Developed and tested against **13**, Win32.
- **Indy** (ships with RAD Studio) for the default socket adapter.
- **[Neon](https://github.com/paolo-rossi/delphi-neon)** for JSON serialization, expected in `Libs\Neon`.
- **DUnitX** to build and run the test suite.

The runtime package requires `rtl`, `IndySystem`, `IndyCore` and `Neon`.

## Installation

Add both `Source` and `Libs\Neon\Source` to your project's search path, then `uses` what you need:

```pascal
uses
  Nats.Consts,
  Nats.Classes,
  Nats.Entities,
  Nats.Connection,
  Nats.Socket.Indy;   // registers the default socket - see "Pluggable sockets"
```

For JetStream, add whichever of these you need — they sit strictly *above* core,
so a program that only wants core NATS never links any of them:

```pascal
uses
  Nats.JetStream.Client,        // TJetStreamContext: management, publish, consume
  Nats.JetStream.Entities,      // the API records
  Nats.JetStream.Message,       // IJetStreamMsg
  Nats.JetStream.KV,            // Key/Value
  Nats.JetStream.ObjectStore;   // Object Store
```

`Nats.Socket.Indy` must be linked into the program even if you never name it
again: it registers the Indy socket adapter from its `initialization` section,
and without it a connection has no transport to use.

To build the runtime package instead, build Neon's package first
(`Libs\Neon\Packages\11AndLater\Neon.dproj`) so that `Neon.dcp` exists, then
`Packages\NatsLibrary.dproj`. From a command prompt — `rsvars.bat` lives in the
RAD Studio `bin` folder and puts MSBuild and the compiler on the path:

```
call "C:\Program Files (x86)\Embarcadero\Studio\37.0\bin\rsvars.bat"
msbuild Libs\Neon\Packages\11AndLater\Neon.dproj /t:Build /p:Config=Release /p:Platform=Win32
msbuild Packages\NatsLibrary.dproj /t:Build /p:Config=Release /p:Platform=Win32
```

An application that links `NatsLibrary` as a runtime package has to deploy `Neon*.bpl` and `dbrtl*.bpl` alongside it.

## Quick start

```pascal
var LConnection := TNatsConnection.Create;
try
  LConnection
    .SetChannel('localhost', 4222, 5000)
    .Open(
      procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
      begin
        // optional: amend the options the client is about to send
        AConnectOptions.Name := 'my-app';
      end);

  // Open only starts the handshake - wait for it before using the connection
  if not LConnection.WaitForReady then
    raise Exception.Create('Could not connect: ' + LConnection.LastError);

  LConnection.Subscribe('greetings',
    procedure (const AMsg: TNatsArgsMSG)
    begin
      // runs on a worker thread - see "Threading"
      Writeln(AMsg.Payload);
    end);

  LConnection.Publish('greetings', 'hello from delphi');

  Readln;
finally
  LConnection.Free;   // closes the connection if it is still open
end;
```

## Connecting

`SetChannel` configures the transport and returns the connection, so it chains
into `Open`:

```pascal
LConnection.SetChannel('localhost', 4222, 5000);
```

The third argument is the **connect timeout** in milliseconds — how long
establishing the TCP connection may take. It is *not* a read timeout; see
[Timeouts and keep-alive](#timeouts-and-keep-alive).

`Open` takes a connect handler and an optional disconnect handler:

```pascal
LConnection.Open(
  procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
  begin
    // AInfo is the server's INFO: ServerName, Version, MaxPayload, AuthRequired...
    // AConnectOptions is passed by var, so credentials can be set here
    AConnectOptions.User := 'joe';
    AConnectOptions.Pass := 'secret';
  end,
  procedure
  begin
    // the connection has gone away, for any reason
  end);
```

**`Open` is asynchronous.** It returns as soon as the socket is up, while the
handshake (the server's `INFO`, then the client's `CONNECT`) completes on a
worker thread. Publishing before that finishes races it, so wait:

```pascal
if LConnection.WaitForReady(5000) then
  // Connected is now True: CONNECT is on the wire and the server knows our options
```

`Connected` means *the handshake finished*, not merely that a socket is open.
Note that the connect handler runs **before** `CONNECT` is written — that is
what lets it amend the options — so it is not a substitute for `WaitForReady`.

Closing is `LConnection.Close`, and destroying the connection closes it for you.

## Publishing

```pascal
LConnection.Publish('subject', 'payload');
LConnection.Publish('subject', 'payload', 'reply.subject');

LConnection.PublishBytes('subject', [1, 2, 3]);
```

Payload lengths are counted in **bytes**, not characters, so multi-byte UTF-8
text is published correctly. A subject that is empty, or that contains
whitespace or a line break, raises `ENatsException` rather than being sent and
corrupting the stream.

## Subscribing

```pascal
var LSid := LConnection.Subscribe('subject',
  procedure (const AMsg: TNatsArgsMSG)
  begin
    // AMsg.Subject, AMsg.ReplyTo, AMsg.Payload, AMsg.PayloadData, AMsg.Headers
  end);
```

Wildcards work as usual (`orders.*`, `orders.>`). For a queue group, pass its
name:

```pascal
LConnection.Subscribe('jobs', 'workers',
  procedure (const AMsg: TNatsArgsMSG)
  begin
  end);
```

`Subscribe` returns the subscription id. Unsubscribe by id or by subject:

```pascal
LConnection.Unsubscribe(LSid);
LConnection.Unsubscribe('subject');

// auto-unsubscribe: stop after 10 messages have been delivered on this
// subscription IN TOTAL, not 10 more from now
LConnection.Unsubscribe(LSid, 10);
```

`GetSubscriptionList` returns a snapshot (`TArray<TNatsSubscriptionInfo>`) of
what is currently subscribed — id, subject, queue, and the message counts.

### Payloads

`AMsg.Payload` is the payload decoded as UTF-8, which is what you want for text.
For binary data use `AMsg.PayloadData`, which holds the bytes exactly as they
arrived; a payload that is not valid UTF-8 has an empty `Payload` but intact
`PayloadData`. `AMsg.PayloadBytes` is the byte count the server declared.

## Request / Reply

```pascal
LConnection.Request('time.service', 'now',
  procedure (const AMsg: TNatsArgsMSG)
  begin
    Writeln('reply: ' + AMsg.Payload);
  end);
```

`Request` subscribes to a fresh `_INBOX.<nuid>`, arms an auto-unsubscribe after
one reply, then publishes with that inbox as the reply subject — so a request
does not leave a subscription behind on either side.

It has **no timeout**: a request that is never answered keeps its subscription
until the connection closes. `RequestSync` is the blocking form, and does have
one:

```pascal
var LReply: TNatsArgsMSG;
if LConnection.RequestSync('time.service', 'now', LReply, 2000) then
  Writeln(LReply.Payload)
else
  Writeln('nobody answered in 2 s');
```

`False` means it timed out — an ordinary outcome. A connection torn down while
waiting **raises** instead, because a dead connection is not the same answer as
silence. The inbox subscription is removed on every exit path, timeout included.

> Never call `RequestSync` from a message, connect or disconnect handler: those
> run on the consumer thread, which is the thread that has to deliver the reply.

Answering a request is an ordinary publish to `AMsg.ReplyTo`:

```pascal
LConnection.Subscribe('time.service',
  procedure (const AMsg: TNatsArgsMSG)
  begin
    if AMsg.ReplyTo <> '' then
      LConnection.Publish(AMsg.ReplyTo, DateTimeToStr(Now));
  end);
```

## Headers

```pascal
var LHeaders: TNatsHeaders := nil;
LHeaders.Add('Nats-Msg-Id', '42');
LHeaders.SetHeader('X-Tenant', 'acme');

LConnection.Publish('subject', 'payload', '', LHeaders);
```

On the receiving side:

```pascal
procedure (const AMsg: TNatsArgsMSG)
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := AMsg.Headers;
  Writeln(LHeaders.GetHeader('Nats-Msg-Id'));
end
```

Header support is negotiated during the handshake: `ConnectOptions.Headers`
defaults to `True`, and a server will refuse `HPUB` outright from a client that
has not declared it. Leave it on unless you know otherwise.

## JetStream

JetStream is the NATS persistence layer. `nats.delphi` covers it in full:
managing streams and consumers, publishing with acknowledgement, pull and push
consumption, and the two stores built on top — Key/Value and Object Store.

Everything runs over an ordinary `TNatsConnection`, through a context that does
not own it:

```pascal
var LJs := TJetStreamContext.Create(LConnection);
try
  // a stream capturing orders.>
  var LCfg := Default(TJetStreamStreamConfig);
  LCfg.Name := 'ORDERS';
  LCfg.Subjects := ['orders.>'];
  LCfg.Storage := TJetStreamStorage.Filestore;
  LCfg.MaxAge := TJetStreamDuration.FromMinutes(60);
  LJs.AddStream(LCfg);

  // publishing waits for the stream to acknowledge - that is the whole
  // difference from a core publish, and it is what turns "nothing captures
  // this subject" from silent data loss into an exception
  var LAck := LJs.Publish('orders.new', '{"id":42}');
  Writeln('stored at seq ', LAck.Seq);

  // a durable pull consumer
  var LCons := Default(TJetStreamConsumerConfig);
  LCons.DurableName := 'workers';
  LCons.AckPolicy := TJetStreamAckPolicy.Explicit;
  LJs.AddConsumer('ORDERS', LCons);

  for var LMsg in LJs.Fetch('ORDERS', 'workers', 10, 5000) do
  begin
    Writeln(LMsg.Payload, ' (', LMsg.Metadata.NumPending, ' still pending)');
    LMsg.Ack;
  end;
finally
  LJs.Free;
end;
```

Key/Value and Object Store are conventions over a stream, and have handles of
their own:

```pascal
var LKV := TJetStreamKV.CreateBucket(LJs, 'config');
LKV.Put('db.host', 'localhost');
Writeln(LKV.Get('db.host', '(unset)'));

var LOs := TJetStreamObjectStore.CreateBucket(LJs, 'files');
LOs.PutFile('report.pdf', 'C:\tmp\report.pdf');   // chunked past max_payload
LOs.GetFile('report.pdf', 'C:\out\report.pdf');   // digest-verified on the way back
```

Three things to know before writing any of it:

- **Every JetStream call blocks**, and none of them may be made from a message,
  connect or disconnect handler — those run on the consumer thread, which is the
  thread that has to deliver the reply. Inside a push handler, plain `Ack` /
  `Nak` / `Term` are fine; they only write.
- **Every duration is nanoseconds.** Build them with
  `TJetStreamDuration.FromSeconds` and friends, never by writing the zeroes out.
- **A failed call raises rather than returning.** `EJetStreamApiError` carries
  the `ErrCode` to branch on; `EJetStreamTimeout` usually means JetStream is not
  enabled on that server.

**[Docs/JetStream-API.md](Docs/JetStream-API.md) is the full reference** — every
method, every entity field, and the reasoning behind the parts that are easy to
get subtly wrong.

## Threading

A connection runs two worker threads: one reads the socket, one dispatches.
**Every handler you supply — message, connect, disconnect, error — runs on a
worker thread, never on the thread that opened the connection.** Handler code
must be thread safe, and anything touching the UI has to marshal:

```pascal
LConnection.Subscribe('subject',
  procedure (const AMsg: TNatsArgsMSG)
  begin
    var LText := AMsg.Payload;
    TThread.Queue(nil,
      procedure
      begin
        Memo1.Lines.Add(LText);   // now on the main thread
      end);
  end);
```

The connection itself is safe to use from several threads at once: writes are
serialized, so two threads publishing concurrently cannot interleave their
frames, and the subscription list is guarded.

## Errors and disconnects

A connection can fail for reasons no return value can report — the peer
vanishing, a protocol error, an `-ERR` from the server. Those arrive through
`OnError`, followed by the disconnect handler:

```pascal
LConnection.OnError :=
  procedure (const AError: string)
  begin
    // e.g. 'ERR from server: Authorization Violation'
    //      'The connection to the server was lost'
    //      'Protocol error: ...'
  end;
```

`LastError` holds the same text afterwards, and is empty after a deliberate
`Close`. Both `OnError` and the disconnect handler fire exactly once per
connection.

## Timeouts and keep-alive

Two different things, deliberately kept apart:

| | meaning | default |
|---|---|---|
| connect timeout | how long establishing the connection may take | 5 s |
| read timeout | how long a single read may block | 3 min |

An idle connection is a healthy one, so the read timeout must outlast the
server's ping interval (2 minutes by default) — otherwise a perfectly good
connection looks broken. Override it with the optional fourth argument:

```pascal
LConnection.SetChannel('localhost', 4222, 5000, 60000);
```

When a read does time out the client probes with its own `PING`; if the `PONG`
for a previous probe never arrived, the peer is gone and the connection is torn
down with an error. Server `PING`s are answered automatically.

## Pluggable sockets

`INatsSocket` abstracts the transport. `Nats.Socket.Indy` provides the Indy
implementation and registers itself as the default:

```pascal
initialization
  TNatsSocketRegistry.Register<TNatsSocketIndy>('Indy', True);
```

To supply your own, descend from `TNatsSocket`, implement the interface, and
register it the same way — the test suite does exactly this with an in-memory
socket so the protocol can be tested byte for byte without a server.

## Demos

| Project | What it is |
|---|---|
| `Demos\DemoNats.dproj` | A VCL application that opens several connections in tabs and lets you publish, subscribe, request and unsubscribe interactively while watching the log |
| `Demos\JetStreamJson.dproj` | A console program that pretty-prints every JetStream entity, for eyeballing the wire format |
| `Demos\JetStreamBench.dproj` | A serialization benchmark for the entity layer. Build it **Release** — Debug figures are meaningless |

## Tests

`Tests\Nats.Tests.Framework.dproj` is a [DUnitX](https://github.com/VSoftTechnologies/DUnitX)
suite of **343 tests**: the parser, entities and NUID; the connection driven
against a mock socket asserting the exact bytes on the wire; thread-safety and
lifecycle tests; the JetStream units, offline; and end-to-end tests against a
real server.

```
Tests\Bin\Nats.Tests.Framework.exe                  # everything
Tests\Bin\Nats.Tests.Framework.exe --exclude:Live   # no server needed
Tests\Bin\Nats.Tests.Framework.exe --include:Live   # only the live tests
```

The `Live` category needs a `nats-server` listening on `localhost:4222`, with
**JetStream enabled** for the JetStream fixtures:

```
nats-server -js
```

The live tests are not redundant with the mock ones. nats-server silently
ignores a JSON field or a header whose name it does not recognise and answers
with its own default, so a misspelled key passes every offline test — only a
real server proves the names on the wire are the ones it actually reads.

## Status and limitations

Implemented and tested:

- **Core NATS** — connect/handshake, publish and subscribe (queue groups and
  wildcards included), request/reply in both an asynchronous and a blocking
  form, headers, auto-unsubscribe, keep-alive, payload-size checking and
  disconnect reporting.
- **JetStream** — the management API (streams, consumers, account), publish with
  acknowledgement and the deduplication / expectation headers, pull consumption
  (`Fetch`, `FetchNoWait`, `Next`), push consumption with flow control, message
  acknowledgement (`Ack` / `Nak` / `InProgress` / `Term`), Key/Value and Object
  Store.

Not implemented yet:

- **Automatic reconnect.** A dropped connection is reported, not re-established; `connect_urls` from the server's `INFO` is ignored.
- **TLS.** A server whose `INFO` says `tls_required` is refused with a clear error rather than being talked to in plaintext.
- **NKey / JWT authentication.** `User`, `Pass` and `AuthToken` work; nothing signs the server's nonce.
- **A timeout on the asynchronous `Request`.** `RequestSync` has one.

`DocsInternal\Core-Protocol-Review.md` is a detailed protocol and concurrency
review of this codebase, with each finding cross-referenced to the tests that
cover it — worth reading before changing anything in the connection or the
reader. `DocsInternal\JetStream-Plan.md` is the corresponding record for the
JetStream layer.
