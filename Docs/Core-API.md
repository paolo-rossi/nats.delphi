# Core NATS API Reference

Reference for the core NATS surface of `nats.delphi`: connecting, publishing,
subscribing, request/reply, headers, and the transport underneath. JetStream is
documented separately in [JetStream-API.md](JetStream-API.md).

Everything here lives in these units:

| Unit | What it holds |
|---|---|
| `Nats.Connection` | `TNatsConnection` — the client. Also `TNatsSubscriptionInfo`, `TNatsGenerator`, `TNatsNetwork` |
| `Nats.Classes` | `TNatsArgsMSG`, `TNatsHeaders`, the handler types, `TNatsCommandQueue` |
| `Nats.Entities` | `TNatsServerInfo`, `TNatsConnectOptions` |
| `Nats.Consts` | `NatsConstants`, `NatsConstants.Protocol`, `NatsConstants.Status` |
| `Nats.Exceptions` | `ENatsException` and its descendants |
| `Nats.Socket` | `INatsSocket`, `TNatsSocket`, `TNatsSocketRegistry` |
| `Nats.Socket.Indy` | The default transport. **Must be linked** — see [Transport](#transport) |
| `Nats.Parser` | `TNatsParser` — the control-line parser |
| `Nats.Nuid` | `TNUID` — the unique-id generator used for inboxes |

## Contents

- [Minimal program](#minimal-program)
- [TNatsConnection](#tnatsconnection)
  - [Lifecycle](#lifecycle)
  - [Publishing](#publishing)
  - [Subscribing](#subscribing)
  - [Request / Reply](#request--reply)
  - [Properties and events](#properties-and-events)
- [Messages](#messages)
- [Headers](#headers)
- [Connect options and server info](#connect-options-and-server-info)
- [Exceptions](#exceptions)
- [Threading rules](#threading-rules)
- [Constants](#constants)
- [Transport](#transport)
- [Utilities](#utilities)

---

## Minimal program

```pascal
program Minimal;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Nats.Classes,
  Nats.Entities,
  Nats.Connection,
  Nats.Socket.Indy;   // links the transport - see "Transport"

var
  LConn: TNatsConnection;
begin
  LConn := TNatsConnection.Create;
  try
    LConn
      .SetChannel('localhost', 4222, 5000)
      .Open(
        procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
        begin
          AConnectOptions.Name := 'minimal';
        end);

    if not LConn.WaitForReady(5000) then
      raise Exception.Create('connect failed: ' + LConn.LastError);

    LConn.Subscribe('greetings',
      procedure (const AMsg: TNatsArgsMSG)
      begin
        Writeln(AMsg.Payload);   // worker thread!
      end);

    LConn.Publish('greetings', 'hello from delphi');

    Readln;
  finally
    LConn.Free;
  end;
end.
```

---

## TNatsConnection

```pascal
constructor Create;
destructor Destroy; override;   // closes the connection if still open
```

One instance is one TCP connection to one server. It is safe to use from
several threads at once; see [Threading rules](#threading-rules).

### Lifecycle

#### SetChannel

```pascal
function SetChannel(const AHost: string; APort, AConnectTimeout: Integer;
  AReadTimeout: Integer = 0): TNatsConnection;
```

Configures the transport and returns `Self`, so it chains into `Open`.

| Parameter | Meaning |
|---|---|
| `AHost`, `APort` | Where the server is. `NatsConstants.DEFAULT_PORT` is 4222 |
| `AConnectTimeout` | Milliseconds allowed to **establish** the TCP connection |
| `AReadTimeout` | Milliseconds a **single read** may block. `0` keeps the socket's own default (`DEFAULT_READ_TIMEOUT`, 3 min) |

These two timeouts are deliberately separate and must not be given the same
value. A connection that takes five seconds to establish is broken; one that
says nothing for five seconds is merely idle. The read timeout must stay
comfortably above the server's ping interval (2 minutes by default) or an idle
but perfectly healthy connection reads as dead.

```pascal
LConn.SetChannel('localhost', 4222, 5000);          // default 3 min read timeout
LConn.SetChannel('localhost', 4222, 5000, 300000);  // 5 min read timeout
```

#### Open

```pascal
procedure Open(AConnectHandler: TNatsConnectHandler;
  ADisconnectHandler: TNatsDisconnectHandler = nil);
```

Opens the socket and starts the reader and consumer threads. **It is
asynchronous**: it returns as soon as the socket is up, while the handshake
(server `INFO` → client `CONNECT`) completes on a worker thread.

The connect handler receives the server's `INFO` and the options about to be
sent, **by `var`**, so it is the place to set credentials:

```pascal
LConn.Open(
  procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
  begin
    if AInfo.AuthRequired then
    begin
      AConnectOptions.User := 'joe';
      AConnectOptions.Pass := 'secret';
    end;
  end,
  procedure
  begin
    // the connection has gone away, for any reason
  end);
```

The connect handler runs **before** `CONNECT` is written — that is what lets it
amend the options — so it is *not* a signal that the connection is usable.

#### WaitForReady

```pascal
function WaitForReady(ATimeoutMs: Cardinal = 5000): Boolean;
```

Blocks until the handshake has completed, i.e. until `CONNECT` is actually on
the wire. This is the guarantee `Open` does not give. Publishing or subscribing
before it returns `True` races the handshake.

```pascal
if not LConn.WaitForReady(5000) then
  raise Exception.Create(LConn.LastError);
```

#### Close

```pascal
procedure Close();
```

Tears the connection down and joins the worker threads. Idempotent. `Destroy`
calls it, so a `try..finally LConn.Free` is enough in simple code.

`Close` called *from a handler* (i.e. from the consumer thread) degrades to a
tear-down without the join, rather than deadlocking on a self-join.

#### Ping / Connect

```pascal
procedure Ping();
procedure Connect(AOptions: TNatsConnectOptions);
```

`Ping` writes a `PING`; the server's `PONG` is consumed internally. Server
`PING`s are answered automatically, and an idle read timeout triggers a
keep-alive `PING` on its own, so calling this by hand is rarely needed.

`Connect` re-sends `CONNECT` with new options. The handshake sends one already —
this is for changing options on a live connection.

### Publishing

```pascal
procedure Publish(const ASubject, AMessage: string; const AReplyTo: string = ''); overload;
procedure Publish(const ASubject, AMessage: string; const AReplyTo: string;
  AHeaders: TNatsHeaders); overload;

procedure PublishBytes(const ASubject: string; const AData: TBytes;
  const AReplyTo: string = ''); overload;
procedure PublishBytes(const ASubject: string; const AData: TBytes;
  const AReplyTo: string; AHeaders: TNatsHeaders); overload;
```

```pascal
LConn.Publish('orders.new', '{"id":42}');
LConn.Publish('orders.new', '{"id":42}', 'inbox.replies');
LConn.PublishBytes('files.raw', [1, 2, 3, 4]);
```

Text is encoded as UTF-8 and the length on the wire is counted in **bytes**, so
multi-byte text publishes correctly.

The overloads taking `AHeaders` emit `HPUB` instead of `PUB`. That requires
`ConnectOptions.Headers` to have been `True` at handshake time (it defaults to
`True`) — a server drops the connection of a client that sends `HPUB` without
having declared header support.

**Size is checked before anything is written.** The connection remembers the
`max_payload` the server declared in `INFO` and raises `ENatsMaxPayloadError`
rather than letting the server answer `-ERR 'Maximum Payload Violation'` and
close the connection, taking every subscription with it. For `HPUB` the check
is on the header block **plus** the payload, because that is what the server
counts.

```pascal
if Length(LData) > LConn.MaxPayload then
  // split it yourself, or catch ENatsMaxPayloadError
```

A subject that is empty, or that contains whitespace or a line break, raises
`ENatsException`: it would otherwise split the control line and desynchronize
the whole stream.

### Subscribing

```pascal
function Subscribe(const ASubject: string; AHandler: TNatsMsgHandler): Integer; overload;
function Subscribe(const ASubject, AQueue: string; AHandler: TNatsMsgHandler): Integer; overload;
```

Returns the subscription id. Wildcards are the usual NATS ones — `orders.*` for
one token, `orders.>` for the rest of the subject.

```pascal
var LSid := LConn.Subscribe('orders.>',
  procedure (const AMsg: TNatsArgsMSG)
  begin
    Writeln(AMsg.Subject, ' -> ', AMsg.Payload);
  end);

// queue group: exactly one member of 'workers' gets each message
LConn.Subscribe('jobs', 'workers',
  procedure (const AMsg: TNatsArgsMSG)
  begin
    Process(AMsg.Payload);
  end);
```

#### Unsubscribe

```pascal
procedure Unsubscribe(AId: Integer; AMaxMsg: Integer = 0); overload;
procedure Unsubscribe(const ASubject: string; AMaxMsg: Integer = 0); overload;
```

`AMaxMsg = 0` unsubscribes immediately. A non-zero value arms an
**auto-unsubscribe**: the subscription ends once it has received that many
messages **in total**, counting the ones already delivered — not that many more
from now.

```pascal
LConn.Unsubscribe(LSid);            // now
LConn.Unsubscribe(LSid, 10);        // after 10 messages in total
LConn.Unsubscribe('orders.new');    // every subscription on that subject
```

#### GetSubscriptionList

```pascal
function GetSubscriptionList: TArray<TNatsSubscriptionInfo>;
```

A snapshot, safe to hold and read at any time because it shares nothing with
the live subscriptions:

```pascal
TNatsSubscriptionInfo = record
  Id: Integer;
  Subject: string;
  Queue: string;
  Received: Integer;   // delivered so far
  Expected: Integer;   // auto-unsubscribe target, 0 when none
  Remaining: Integer;
end;
```

### Request / Reply

A request is *subscribe to a fresh inbox, publish with that inbox as reply-to*.
Two forms: asynchronous and blocking.

#### Request (asynchronous)

```pascal
function Request(const ASubject: string; AHandler: TNatsMsgHandler): Integer; overload;
function Request(const ASubject, AMessage: string; AHandler: TNatsMsgHandler): Integer; overload;
```

```pascal
LConn.Request('time.service', 'now',
  procedure (const AMsg: TNatsArgsMSG)
  begin
    Writeln('reply: ' + AMsg.Payload);   // worker thread
  end);
```

It arms an auto-unsubscribe after one reply, so a served request leaves nothing
behind. There is **no timeout**: a request nobody answers keeps its inbox
subscription until the connection closes. Use `RequestSync` when that matters.

#### RequestSync (blocking)

```pascal
function RequestSync(const ASubject, AMessage: string; out AReply: TNatsArgsMSG;
  ATimeoutMs: Cardinal = NatsConstants.DEFAULT_REQUEST_TIMEOUT): Boolean; overload;

function RequestSync(const ASubject: string; const AData: TBytes; out AReply: TNatsArgsMSG;
  ATimeoutMs: Cardinal = NatsConstants.DEFAULT_REQUEST_TIMEOUT): Boolean; overload;

function RequestSync(const ASubject: string; const AData: TBytes; AHeaders: TNatsHeaders;
  out AReply: TNatsArgsMSG;
  ATimeoutMs: Cardinal = NatsConstants.DEFAULT_REQUEST_TIMEOUT): Boolean; overload;
```

Publishes and blocks until one reply arrives or the timeout elapses.
`DEFAULT_REQUEST_TIMEOUT` is 5 seconds.

```pascal
var LReply: TNatsArgsMSG;
if LConn.RequestSync('time.service', 'now', LReply, 2000) then
  Writeln(LReply.Payload)
else
  Writeln('nobody answered in 2 s');
```

- **`False` means it timed out.** That is an ordinary outcome — nobody is
  serving the subject, or they were slow.
- **A connection torn down while waiting raises**, because a dead connection is
  not the same answer as silence.
- The inbox subscription is removed on **every** exit path, including the
  timeout and a publish that raised.

> **Never call `RequestSync` from a message, connect or disconnect handler.**
> Those run on the consumer thread, which is the very thread that would have to
> deliver the reply, so the call blocks until the timeout and gets nothing.

#### Answering a request

An answer is an ordinary publish to the message's `ReplyTo`:

```pascal
LConn.Subscribe('time.service',
  procedure (const AMsg: TNatsArgsMSG)
  begin
    if AMsg.ReplyTo <> '' then
      LConn.Publish(AMsg.ReplyTo, DateTimeToStr(Now));
  end);
```

#### GetNewInbox

```pascal
function GetNewInbox(): string;
```

A fresh `_INBOX.<nuid>` subject, for hand-rolled reply patterns.

### Properties and events

```pascal
ConnectOptions: TNatsConnectOptions;        // a public FIELD, not a property
property Name: string;                      // your label for this connection
property Connected: Boolean;                // handshake finished (read-only)
property Ready: Boolean;                    // an alias for Connected
property MaxPayload: Integer;               // from INFO; 0 before the handshake
property LastError: string;                 // why it last failed; '' after a clean Close
property OnError: TNatsErrorHandler;
property Reader: TNatsReader;               // the worker threads, exposed for diagnostics
property Consumer: TNatsConsumer;
```

`ConnectOptions` is a **public field on purpose**: the connect handler receives
it as `var` and assigns into it, which a property would not allow.

`Connected` means **the handshake finished**, not merely that a socket is up.
Until `CONNECT` is on the wire the server knows nothing about this client.

#### OnError

```pascal
LConn.OnError :=
  procedure (const AError: string)
  begin
    // 'ERR from server: Authorization Violation'
    // 'The connection to the server was lost'
    // 'Protocol error: ...'
  end;
```

Fires for the failures no return value can report: a protocol error, a dead
socket, an `-ERR` from the server. It runs on a worker thread, is followed by
the disconnect handler, and both fire exactly once per connection. `LastError`
holds the same text afterwards.

### TNatsNetwork

```pascal
TNatsNetwork = class(TObjectDictionary<string, TNatsConnection>)
  function NewConnection(const AName: string): TNatsConnection;
end;
```

A named collection of connections that owns and frees them. The VCL demo uses
one to keep a connection per tab.

---

## Messages

Every handler receives a `TNatsArgsMSG` (`Nats.Classes`):

```pascal
TNatsArgsMSG = record
  Id: Integer;             // the subscription that matched
  Subject: string;
  ReplyTo: string;         // '' when the publisher wants no answer
  PayloadBytes: Integer;   // the byte COUNT the server declared
  Payload: string;         // the payload decoded as UTF-8
  PayloadData: TBytes;     // the payload exactly as it arrived
  HeaderBytes: Integer;    // size of the header block (HMSG)
  TotalMsgBytes: Integer;  // HeaderBytes + PayloadBytes (HMSG)
  Headers: TNatsHeaders;
  Status: Integer;         // status-line code, 0 for an ordinary message
  Description: string;     // the status line's wording, informational only

  function HasStatus: Boolean;
end;
```

### Payloads

- `Payload` is the UTF-8 reading of the bytes — what you want for text.
- `PayloadData` is the bytes themselves — what you want for anything binary. A
  payload that is not valid UTF-8 has an **empty `Payload`** but intact
  `PayloadData`.
- `PayloadBytes` is the byte *count*, not the data. The name predates
  `PayloadData`.

### Status messages

A header block's first line may be a status line rather than a header pair:

```
NATS/1.0 404 No Messages
```

`Status` is then 404, `Description` is `'No Messages'`, and `HasStatus` is
`True`. **A status message has an empty body** — it is control flow, not data.
Codes live in `NatsConstants.Status`:

| Constant | Code | Meaning |
|---|---|---|
| `NO_MESSAGES` | 404 | A pull batch produced nothing |
| `REQUEST_TIMEOUT` | 408 | The pull request's own expiry elapsed |
| `CONFLICT` | 409 | Consumer deleted, or `MaxWaiting` exceeded |
| `IDLE_HEARTBEAT` | 100 | Keep-alive on an idle consumer or batch |
| `NO_RESPONDERS` | 503 | Nobody is subscribed to that subject |

These matter mostly to JetStream, which is where pull consumers live, but any
subscription can see one.

---

## Headers

`TNatsHeaders` is `TArray<TPair<string, string>>` with a record helper
(`Nats.Classes`):

```pascal
procedure Add(const AName, AValue: string);          // appends; names may repeat
procedure CopyHeaders(const AHeaders: TNatsHeaders);
function  GetIndex(const AName: string): Integer;    // -1 when absent
function  GetHeader(const AName: string): string;    // '' when absent
procedure SetHeader(const AName, AValue: string);    // replaces, or adds
function  GetHeaderAsInt(const AName: string; ADefault: UInt64): UInt64;
function  Count: Integer;
function  Text: string;                              // the HTTP "Key: Value" form
```

Lookups are **case-insensitive**, which is what the NATS spec requires.

```pascal
var LHeaders: TNatsHeaders := nil;   // a dynamic array - nil is empty
LHeaders.Add('X-Tenant', 'acme');
LHeaders.SetHeader('X-Trace-Id', '0f3a...');

LConn.Publish('orders.new', '{"id":42}', '', LHeaders);
```

Reading them back:

```pascal
LConn.Subscribe('orders.new',
  procedure (const AMsg: TNatsArgsMSG)
  begin
    Writeln(AMsg.Headers.GetHeader('X-Tenant'));
    if AMsg.Headers.GetIndex('X-Trace-Id') >= 0 then
      Trace(AMsg.Headers.GetHeader('X-Trace-Id'));
  end);
```

`Text` emits exactly the form `TNatsParser.ParseHeaders` reads, so headers
round-trip symmetrically.

---

## Connect options and server info

`Nats.Entities`. Both records are serialized with
[Neon](https://github.com/paolo-rossi/delphi-neon), and their **field names are
protocol**: the wire JSON names (`max_payload`, `auth_token`, …) are derived
from them by a snake-case rule. Renaming a field renames a protocol field.

### TNatsConnectOptions

```pascal
TNatsConnectOptions = record
  Verbose: Boolean;        // ask the server to +OK every command
  Pedantic: Boolean;
  TlsRequired: Boolean;
  AuthToken: string;
  User: string;
  Pass: string;
  Name: string;            // shows up in the server's client list
  Lang: string;            // set by the library
  Version: string;         // set by the library
  Protocol: Integer;
  Echo: Boolean;           // False to not receive your own publishes
  Headers: Boolean;        // must be True to use headers - defaults to True
  Sig: string;             // nkey signature - not produced by this library yet
  Jwt: string;

  function ToJSONString: string;
  class function FromJSONString(const AValue: string): TNatsConnectOptions; static;
end;
```

Leave `Headers` alone unless you know otherwise: a server strips headers from
anything it delivers to a client that has not declared support, sending `MSG`
instead of `HMSG`, and refuses `HPUB` outright.

### TNatsServerInfo

What the connect handler receives — the server's `INFO`:

```pascal
TNatsServerInfo = record
  ServerId, ServerName, Version, GitCommit, Go, Host, ClientIp, Nonce: string;
  Proto, Port, MaxPayload, ClientId: Integer;
  Headers, AuthRequired, TlsRequired, TlsAvailable, Jetstream: Boolean;

  class function FromJSONString(const AValue: string): TNatsServerInfo; static;
end;
```

`Jetstream` says whether the server has JetStream enabled — worth checking
before building a `TJetStreamContext` over the connection.

---

## Exceptions

All descend from `ENatsException` (`Nats.Exceptions`):

| Exception | Raised when |
|---|---|
| `ENatsException` | The general case: a bad subject, no transport registered, a call on a closed connection |
| `ENatsMaxPayloadError` | A publish is larger than the server's `max_payload`. Refused **before** anything is written |
| `ENatsReadTimeout` | A read exceeded the socket's read timeout. **Not fatal** — the client answers it with a keep-alive `PING` |
| `ENatsProtocolError` | The peer sent something unparseable. The stream can no longer be trusted, so the connection is torn down |

`ENatsReadTimeout` and `ENatsProtocolError` are mostly internal: a custom socket
adapter must preserve the distinction, because one means "idle" and the other
means "broken".

---

## Threading rules

A connection runs two worker threads:

```
socket --(TNatsReader)--> TNatsCommandQueue --(TNatsConsumer)--> your handlers
```

**Every handler you supply — message, connect, disconnect, error — runs on a
worker thread, never on the thread that opened the connection.** Handler code
must be thread safe, and anything touching the UI has to marshal:

```pascal
LConn.Subscribe('log.>',
  procedure (const AMsg: TNatsArgsMSG)
  begin
    var LText := AMsg.Payload;        // capture by value first
    TThread.Queue(nil,
      procedure
      begin
        Memo1.Lines.Add(LText);       // now on the main thread
      end);
  end);
```

What is safe:

- **Publishing from several threads at once.** Writes are serialized for one
  complete command, so a control line and its payload can never be split by
  another thread's write.
- **Subscribing and unsubscribing from any thread.** The subscription list is
  guarded.
- **Calling `Close` from a handler.** It degrades to a tear-down rather than
  deadlocking on a self-join.

What is not:

- **Calling `RequestSync` from a handler.** It would block the thread that has
  to deliver the reply. Same for anything built on it — the whole JetStream
  management API, and `IJetStreamMsg.AckSync`.

---

## Constants

`NatsConstants` (`Nats.Consts`). Use these, never string literals.

```pascal
NatsConstants.DEFAULT_PORT               = 4222;
NatsConstants.DEFAULT_CONNECT_TIMEOUT    = 5 * 1000;         // 5 s
NatsConstants.DEFAULT_READ_TIMEOUT       = 3 * 60 * 1000;    // 3 min
NatsConstants.DEFAULT_SERVER_PING_INTERVAL = 2 * 60 * 1000;  // 2 min
NatsConstants.DEFAULT_REQUEST_TIMEOUT    = 5 * 1000;         // 5 s
NatsConstants.INBOX_PREFIX               = '_INBOX.';
NatsConstants.CLIENT_HEADER_VERSION      = 'NATS/1.0';
```

`DEFAULT_READ_TIMEOUT` **must** stay above `DEFAULT_SERVER_PING_INTERVAL`, or an
idle-but-healthy connection reads as broken. A test pins that.

`NatsConstants.Protocol` holds the verb strings (`PUB`, `HPUB`, `SUB`, `UNSUB`,
`CONNECT`, `PING`, `PONG`, `INFO`, `MSG`, `HMSG`, `+OK`, `-ERR`);
`NatsConstants.Status` the status-line codes listed [above](#status-messages).

---

## Transport

`INatsSocket` (`Nats.Socket`) is a line-oriented transport:

```pascal
procedure Open();
procedure Close();
procedure SendString(const AValue: string);   // appends CRLF
procedure SendBytes(const AValue: TBytes);    // appends CRLF
function  ReceiveString: string;              // one line, CRLF stripped
function  ReceiveBytes: TBytes;
function  ReceiveExactBytes(ACount: Integer): TBytes;   // a payload block

property Connected: Boolean;
property Host: string;
property Port: Integer;
property ConnectTimeout: Cardinal;
property ReadTimeout: Cardinal;
property MaxLineLength: Cardinal;
```

Implementations register themselves in a process-wide singleton from their
`initialization` section:

```pascal
initialization
  TNatsSocketRegistry.Register<TNatsSocketIndy>('Indy', True);   // True = default
```

> **Consequence:** a project that never `uses` a `Nats.Socket.*` unit has no
> transport and raises at connection time. That is why every `.dpr` in this
> repository lists `Nats.Socket.Indy` explicitly even though it never names the
> class again.

To supply your own — a TLS transport, a test double — descend from
`TNatsSocket`, implement the interface and register it the same way:

```pascal
type
  TMySocket = class(TNatsSocket)
  public
    constructor Create; override;   // TNatsSocket.Create is virtual AND concrete
    // ...
  end;

initialization
  TNatsSocketRegistry.Register<TMySocket>('MySocket', True);
```

Two things a new adapter must get right: call `inherited Create`, and raise
`ENatsReadTimeout` (not a generic exception) when a read times out, so the
connection treats it as idle rather than broken.

`TNatsConnection` always asks the registry for the default
(`TNatsSocketRegistry.Get('')`) — there is no per-connection adapter choice, so
the default is process-wide. The test suite exploits this with an in-memory
socket, which is how the protocol is asserted byte for byte with no server
running.

---

## Utilities

### TNUID

`Nats.Nuid`. The 22-character base-62 unique id used for inbox names, and
useful for `Nats-Msg-Id` values and unique subjects:

```pascal
uses Nats.Nuid;

var LId := TNUID.NextNuid;              // thread-safe, uses the global generator

var LGen := TNUID.Create;               // or your own instance
try
  Writeln(LGen.Next);
finally
  LGen.Free;
end;
```

### TNatsParser

`Nats.Parser`. Parses a **control line** into a `TNatsCommand`; payload and
header blocks are read by the connection's reader by exact byte count, not by
the parser. You need it directly only when decoding a header block yourself —
for instance the raw block a JetStream `STREAM.MSG.GET` returns:

```pascal
var LParser := TNatsParser.Create;
try
  var LHeaders: TNatsHeaders;
  var LStatus: Integer;
  var LDescription: string;
  LParser.ParseHeaders(LRawBlock, LHeaders, LStatus, LDescription);
finally
  LParser.Free;
end;
```

---

## See also

- [JetStream-API.md](JetStream-API.md) — streams, consumers, Key/Value, Object Store
- `Demos\DemoNats.dproj` — a VCL application exercising all of the above
- `Tests\Source\Nats.Tests.Adapters.pas` — the wire format, asserted byte for byte
