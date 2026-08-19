# JetStream API Reference

Reference for the JetStream surface of `nats.delphi`: the management API,
publishing with acknowledgement, pull and push consumption, message
acknowledgement, Key/Value and Object Store. Core NATS is documented separately
in [Core-API.md](Core-API.md).

JetStream sits **strictly above** core: nothing in core `uses` a JetStream unit,
so a program that only wants core NATS never links any of this.

| Unit | What it holds |
|---|---|
| `Nats.JetStream.Client` | `TJetStreamContext` (management, publish, consume), `TJetStreamPubOptions`, the exceptions |
| `Nats.JetStream.Entities` | The API records — stream and consumer configs, infos, requests, responses |
| `Nats.JetStream.Message` | `IJetStreamMsg`, `TJetStreamMsgMetadata` |
| `Nats.JetStream.KV` | `TJetStreamKV`, `TKVEntry`, `TJetStreamKVConfig` |
| `Nats.JetStream.ObjectStore` | `TJetStreamObjectStore`, `TJetStreamObjectInfo` |
| `Nats.JetStream.Consts` | `JetStreamConstants` — API subjects, headers, ack payloads |

## Contents

- [Getting started](#getting-started)
- [Two rules that apply everywhere](#two-rules-that-apply-everywhere)
- [TJetStreamContext](#tjetstreamcontext)
  - [Account](#account)
  - [Streams](#streams)
  - [Reading a stored message](#reading-a-stored-message)
  - [Consumers](#consumers)
  - [Publishing](#publishing)
  - [Pull consumption](#pull-consumption)
  - [Push consumption](#push-consumption)
  - [ScanSubject](#scansubject)
  - [Raw API access](#raw-api-access)
- [IJetStreamMsg — acknowledging](#ijetstreammsg--acknowledging)
- [Durations and timestamps](#durations-and-timestamps)
- [Entities reference](#entities-reference)
- [Exceptions](#exceptions)
- [Key/Value](#keyvalue)
- [Object Store](#object-store)
- [Constants](#constants)

---

## Getting started

A context is built over an existing, **already connected** `TNatsConnection`
that it does **not** own:

```pascal
uses
  Nats.Connection,
  Nats.Socket.Indy,
  Nats.JetStream.Client,
  Nats.JetStream.Entities,
  Nats.JetStream.Message;

var
  LConn: TNatsConnection;
  LJs: TJetStreamContext;
begin
  LConn := TNatsConnection.Create;
  try
    LConn.SetChannel('localhost', 4222, 5000).Open(nil);
    if not LConn.WaitForReady(5000) then
      raise Exception.Create(LConn.LastError);

    LJs := TJetStreamContext.Create(LConn);
    try
      // create a stream
      var LCfg := Default(TJetStreamStreamConfig);
      LCfg.Name := 'ORDERS';
      LCfg.Subjects := ['orders.>'];
      LCfg.Storage := TJetStreamStorage.Filestore;
      LJs.AddStream(LCfg);

      // publish, and wait for the stream to acknowledge it
      var LAck := LJs.Publish('orders.new', '{"id":42}');
      Writeln('stored in ', LAck.Stream, ' at seq ', LAck.Seq);

      // a durable pull consumer
      var LCons := Default(TJetStreamConsumerConfig);
      LCons.DurableName := 'workers';
      LCons.AckPolicy := TJetStreamAckPolicy.Explicit;
      LJs.AddConsumer('ORDERS', LCons);

      // pull and acknowledge
      for var LMsg in LJs.Fetch('ORDERS', 'workers', 10, 2000) do
      begin
        Writeln(LMsg.Payload);
        LMsg.Ack;
      end;
    finally
      LJs.Free;
    end;
  finally
    LConn.Free;
  end;
end;
```

```pascal
constructor TJetStreamContext.Create(AConnection: TNatsConnection;
  const ADomain: string = '');
```

`ADomain` moves the whole API under `$JS.<domain>.API` instead of `$JS.API` —
that is how a leaf node reaches the hub's JetStream rather than its own. Leave
it empty for the ordinary case.

---

## Two rules that apply everywhere

**1. Every method here blocks.** There is no sensible asynchronous form of
"create a stream", and management calls come from setup code. `Timeout`
(default: the connection's `DEFAULT_REQUEST_TIMEOUT`, 5 s) bounds them.

**2. Never call any of it from a message, connect or disconnect handler.**
Those run on the connection's consumer thread, which is the thread that has to
deliver the API's reply, so the call would block until it timed out. This is
`RequestSync`'s constraint and it is inherited whole by:

- the entire `TJetStreamContext` management API
- `Publish` / `PublishBytes`
- `Fetch` / `FetchNoWait` / `Next` / `ScanSubject`
- `IJetStreamMsg.AckSync`
- every `TJetStreamKV` and `TJetStreamObjectStore` method

Inside a push handler, plain `Ack` / `Nak` / `Term` / `InProgress` are fine —
they only write and wait for nothing.

---

## TJetStreamContext

```pascal
property Domain: string;              // empty unless built for a domain
property Timeout: Cardinal;           // how long to wait for an API reply
```

### Account

```pascal
function AccountInfo: TJetStreamAccountInfo;
```

The cheapest call there is, so it doubles as "is JetStream even enabled here":

```pascal
try
  var LInfo := LJs.AccountInfo;
  Writeln(Format('%d streams, %d consumers, %d bytes stored',
    [LInfo.Streams, LInfo.Consumers, LInfo.Storage]));
except
  on E: EJetStreamTimeout do
    Writeln('JetStream is not enabled on this server');
end;
```

### Streams

```pascal
function AddStream(const AConfig: TJetStreamStreamConfig): TJetStreamStreamInfo;
function UpdateStream(const AConfig: TJetStreamStreamConfig): TJetStreamStreamInfo;
function StreamInfo(const AStream: string): TJetStreamStreamInfo;
function DeleteStream(const AStream: string): Boolean;

function PurgeStream(const AStream: string): UInt64; overload;
function PurgeStream(const AStream: string;
  const ARequest: TJetStreamPurgeRequest): UInt64; overload;

function ListStreams(AOffset: Integer = 0): TJetStreamStreamListResponse;
function StreamNames(AOffset: Integer = 0): TArray<string>;
```

```pascal
var LCfg := Default(TJetStreamStreamConfig);
LCfg.Name := 'EVENTS';
LCfg.Subjects := ['events.>'];
LCfg.Retention := TJetStreamRetention.Limits;
LCfg.Storage := TJetStreamStorage.Filestore;
LCfg.MaxAge := TJetStreamDuration.FromMinutes(60);
LCfg.MaxMsgs := 1000000;
LCfg.Discard := TJetStreamDiscard.Old;

var LInfo := LJs.AddStream(LCfg);
Writeln(LInfo.State.Messages, ' messages, last seq ', LInfo.State.LastSeq);
```

> **`UpdateStream` replaces the WHOLE configuration.** Read `StreamInfo`, change
> what you want, and send it all back — fields you leave out revert to the
> server's defaults.
>
> ```pascal
> var LCfg := LJs.StreamInfo('EVENTS').Config;
> LCfg.MaxAge := TJetStreamDuration.FromMinutes(120);
> LJs.UpdateStream(LCfg);
> ```

Purging:

```pascal
LJs.PurgeStream('EVENTS');                        // empty it entirely

var LPurge := Default(TJetStreamPurgeRequest);
LPurge.Filter := 'events.audit.>';                // only this subject space
LPurge.Keep := 100;                               // ...keeping the last 100
LJs.PurgeStream('EVENTS', LPurge);
```

Listing is paged. `ListStreams` / `ListConsumers` return ONE page and
`Total` is how many exist, not how many are in this page — so page manually:

```pascal
var LOffset := 0;
repeat
  var LPage := LJs.ListStreams(LOffset);
  for var LStream in LPage.Streams do
    Writeln(LStream.Config.Name);
  Inc(LOffset, Length(LPage.Streams));
until (LOffset >= LPage.Total) or (Length(LPage.Streams) = 0);
```

`StreamNames` and `ConsumerNames` page through internally — they return the
whole list, so the loop above is only needed for the `List*` forms.

**Names are checked before they go anywhere.** A stream or consumer name goes
into the API subject verbatim, so a `.`, `*`, `>` or whitespace would address a
*different endpoint* rather than fail. Those raise `ENatsException`.

### Reading a stored message

Without creating a consumer at all:

```pascal
function GetMsg(const AStream: string; const ARequest: TJetStreamMsgGetRequest;
  out AMsg: TJetStreamStoredMsg): Boolean; overload;
function GetMsg(const AStream: string; ASeq: UInt64;
  out AMsg: TJetStreamStoredMsg): Boolean; overload;
function GetLastMsg(const AStream, ASubject: string;
  out AMsg: TJetStreamStoredMsg): Boolean;
```

`False` means the server answered err_code 10037 "no message found" — an
ordinary answer. A stream that does not exist (err_code 10059) is **not** that
answer: it raises `EJetStreamApiError`, so a missing bucket can never be
mistaken for an absent key.

```pascal
var LMsg: TJetStreamStoredMsg;
if LJs.GetLastMsg('EVENTS', 'events.config', LMsg) then
  Writeln(TEncoding.UTF8.GetString(TNetEncoding.Base64.DecodeStringToBytes(LMsg.Data)));
```

`Data` and `Hdrs` are **base64**, because a stored message is arbitrary bytes
and JSON cannot carry those. `Hdrs` is the whole raw header block — `NATS/1.0`,
the pairs and the blank line — so it goes back through
`TNatsParser.ParseHeaders` rather than being read by hand.

This is the workhorse behind a Key/Value get.

### Consumers

```pascal
function AddConsumer(const AStream: string;
  const AConfig: TJetStreamConsumerConfig): TJetStreamConsumerInfo;
function ConsumerInfo(const AStream, AConsumer: string): TJetStreamConsumerInfo;
function DeleteConsumer(const AStream, AConsumer: string): Boolean;
function ListConsumers(const AStream: string; AOffset: Integer = 0): TJetStreamConsumerListResponse;
function ConsumerNames(const AStream: string; AOffset: Integer = 0): TArray<string>;
```

Two axes decide what kind of consumer you get:

| | Durable | Ephemeral |
|---|---|---|
| **Pull** (`DeliverSubject` empty) | `DurableName` set — position survives restarts | neither set — server picks a name, discards it when nobody is bound |
| **Push** (`DeliverSubject` set) | `DurableName` set | `DurableName` empty |

```pascal
// a durable pull consumer
var LCons := Default(TJetStreamConsumerConfig);
LCons.DurableName := 'workers';
LCons.AckPolicy := TJetStreamAckPolicy.Explicit;
LCons.AckWait := TJetStreamDuration.FromSeconds(30);
LCons.MaxDeliver := 5;
LCons.FilterSubject := 'events.orders.>';
LJs.AddConsumer('EVENTS', LCons);

// a durable push consumer - setting DeliverSubject is the whole difference
var LPush := Default(TJetStreamConsumerConfig);
LPush.DurableName := 'live';
LPush.AckPolicy := TJetStreamAckPolicy.Explicit;
LPush.DeliverSubject := 'deliver.live';
LJs.AddConsumer('EVENTS', LPush);
```

Progress is on `TJetStreamConsumerInfo`:

```pascal
var LInfo := LJs.ConsumerInfo('EVENTS', 'workers');
Writeln(LInfo.Delivered.StreamSeq, ' delivered, ',
        LInfo.AckFloor.StreamSeq, ' acked, ',
        LInfo.NumPending, ' pending, ',
        LInfo.NumAckPending, ' awaiting ack');
```

### Publishing

```pascal
function Publish(const ASubject, AMessage: string): TJetStreamPubAck; overload;
function Publish(const ASubject, AMessage: string;
  const AOptions: TJetStreamPubOptions): TJetStreamPubAck; overload;
function PublishBytes(const ASubject: string; const AData: TBytes): TJetStreamPubAck; overload;
function PublishBytes(const ASubject: string; const AData: TBytes;
  const AOptions: TJetStreamPubOptions): TJetStreamPubAck; overload;
```

A JetStream publish is an **ordinary core `PUB`/`HPUB` to an ordinary subject**.
Waiting for the acknowledgement is the entire difference — and it is what turns
"no stream captures this subject" from silent data loss into an exception:

```pascal
try
  var LAck := LJs.Publish('orders.new', '{"id":42}');
  Writeln('seq ', LAck.Seq, ' in ', LAck.Stream);
except
  on E: EJetStreamTimeout do
    Writeln('nothing captured that subject');   // core NATS would have dropped it silently
end;
```

```pascal
TJetStreamPubAck = record
  Stream: string;
  Seq: UInt64;
  Duplicate: Boolean;   // recognised by Nats-Msg-Id and NOT stored again; Seq is the original
  Domain: string;
end;
```

#### TJetStreamPubOptions

Deduplication and optimistic concurrency, built fluently:

```pascal
class function New: TJetStreamPubOptions; static;

function WithMsgId(const AValue: string): TJetStreamPubOptions;
function WithExpectedStream(const AValue: string): TJetStreamPubOptions;
function WithExpectedLastSeq(AValue: UInt64): TJetStreamPubOptions;
function WithExpectedLastSubjectSeq(AValue: UInt64): TJetStreamPubOptions;
function WithExpectedLastMsgId(const AValue: string): TJetStreamPubOptions;
function WithHeader(const AName, AValue: string): TJetStreamPubOptions;

function Headers: TNatsHeaders;
```

```pascal
// deduplicate: within the stream's duplicate window the same id is stored once
var LAck := LJs.Publish('orders.new', LBody,
  TJetStreamPubOptions.New.WithMsgId(LOrderId));
if LAck.Duplicate then
  Writeln('already had it, at seq ', LAck.Seq);

// compare-and-set: only publish if the subject is still where we last saw it
LJs.Publish('config.db', LNewValue,
  TJetStreamPubOptions.New
    .WithExpectedStream('CONFIG')
    .WithExpectedLastSubjectSeq(LKnownSeq));

// assert the stream is still EMPTY - zero is a real expectation, not "unset"
LJs.Publish('ledger.open', LBody,
  TJetStreamPubOptions.New.WithExpectedLastSeq(0));
```

> **Why fluent rather than a record of plain fields:** zero is a meaningful
> value for every expectation here. `Nats-Expected-Last-Sequence: 0` asserts the
> stream is empty, which a record of `UInt64`s could not tell apart from "left
> alone". Calling the method is what puts the header on the wire.

The record has value semantics, so a half-built options record can be kept and
branched from safely:

```pascal
var LBase := TJetStreamPubOptions.New.WithHeader('X-Tenant', 'acme');
var LFirst  := LBase.WithMsgId('a');
var LSecond := LBase.WithMsgId('b');   // does not disturb LFirst
```

A failed expectation comes back as an `EJetStreamApiError`, not as a timeout.

### Pull consumption

```pascal
function Fetch(const AStream, AConsumer: string; ABatch: Integer = 1;
  ATimeoutMs: Cardinal = 0): TArray<IJetStreamMsg>;
function FetchNoWait(const AStream, AConsumer: string;
  ABatch: Integer = 1): TArray<IJetStreamMsg>;
function Next(const AStream, AConsumer: string; out AMsg: IJetStreamMsg;
  ATimeoutMs: Cardinal = 0): Boolean;
```

`ATimeoutMs = 0` means this context's `Timeout`.

```pascal
// a batch
for var LMsg in LJs.Fetch('EVENTS', 'workers', 25, 5000) do
begin
  Handle(LMsg.Payload);
  LMsg.Ack;
end;

// one message
var LMsg: IJetStreamMsg;
if LJs.Next('EVENTS', 'workers', LMsg, 1000) then
begin
  Handle(LMsg.Payload);
  LMsg.Ack;
end;
```

- **A short batch is a result, not a failure.** An empty consumer returns an
  empty array and nothing raises.
- `FetchNoWait` returns with whatever is already waiting instead of holding the
  request open. **Ideal for draining, wrong for polling** — a poll loop built on
  it hammers the server.
- `Fetch` sends its own, slightly shorter expiry with the request. Without one
  the server holds the request open against the consumer's `MaxWaiting` even
  after the client has walked away.

A typical worker loop:

```pascal
while not Terminated do
begin
  var LBatch := LJs.Fetch('EVENTS', 'workers', 50, 5000);
  if Length(LBatch) = 0 then
    Continue;                         // nothing pending; Fetch already waited

  for var LMsg in LBatch do
    try
      Handle(LMsg);
      LMsg.Ack;
    except
      LMsg.Nak;                       // redeliver rather than lose it
    end;
end;
```

### Push consumption

```pascal
function SubscribePush(const AStream, AConsumer: string;
  AHandler: TJetStreamMsgHandler): Integer;
```

Subscribes to an **existing** push consumer's delivery subject — the subject is
read from `ConsumerInfo`, not guessed — and returns the core subscription id,
which is what `TNatsConnection.Unsubscribe` takes. Passing a pull consumer
raises.

```pascal
var LSid := LJs.SubscribePush('EVENTS', 'live',
  procedure (const AMsg: IJetStreamMsg)
  begin
    // consumer thread: thread-safe state only, and no blocking calls
    Log(AMsg.Payload);
    AMsg.Ack;          // plain Ack only writes - allowed here
  end);

// later
LConn.Unsubscribe(LSid);
```

> The handler runs on the connection's consumer thread. `AckSync`, `Fetch` and
> the whole management API are **forbidden** inside it. Plain `Ack`, `Nak`,
> `Term` and `InProgress` are fine.

Idle heartbeats and flow-control requests are answered and swallowed here rather
than handed on: they are the server talking to the client, not data. A
flow-control request **must** be answered or the server stops sending, which is
why it is not simply ignored.

### ScanSubject

```pascal
procedure ScanSubject(const AStream, AFilter: string;
  ALastPerSubject, AHeadersOnly: Boolean; const AProc: TProc<IJetStreamMsg>);
```

"Read every message matching a filter, once", through a throwaway consumer that
is deleted on every exit path. This is the general form of looking at a set of
messages, and it is what Key/Value's `Keys` and `History` and the object store's
reads are built on.

| Parameter | Effect |
|---|---|
| `ALastPerSubject` | `True` gives **one** message per subject — the current state. `False` gives every message, in stream order |
| `AHeadersOnly` | `True` leaves the payloads on the server, for when only metadata is wanted |

```pascal
// the current state of every subject in the stream
LJs.ScanSubject('EVENTS', 'events.>', True, False,
  procedure (const AMsg: IJetStreamMsg)
  begin
    Writeln(AMsg.Subject, ' = ', AMsg.Payload);
  end);

// every subject name, without moving any payloads
LJs.ScanSubject('EVENTS', 'events.>', True, True,
  procedure (const AMsg: IJetStreamMsg)
  begin
    LNames.Add(AMsg.Subject);
  end);
```

The throwaway consumer also carries an inactivity threshold, so a client that
dies mid-scan does not leave one behind for good.

### Raw API access

For endpoints this class does not wrap yet:

```pascal
function ApiRequest<TResp: record>(const ASubject: string): TResp; overload;
function ApiRequest<TReq, TResp: record>(const ASubject: string;
  const ARequest: TReq): TResp; overload;
```

`ASubject` is the part **after** the prefix — `'STREAM.INFO.EVENTS'`, not
`'$JS.API.STREAM.INFO.EVENTS'`. The prefix, the domain, serialization, and the
error check are all applied for you.

```pascal
var LInfo := LJs.ApiRequest<TJetStreamStreamInfo>('STREAM.INFO.EVENTS');
```

---

## IJetStreamMsg — acknowledging

`Nats.JetStream.Message`. What `Fetch`, `Next`, `SubscribePush` and
`ScanSubject` all hand back: the message, its metadata, and the connection to
answer over, in one place.

```pascal
property Subject: string;
property Payload: string;              // decoded as UTF-8
property PayloadData: TBytes;          // the bytes as they arrived
property Headers: TNatsHeaders;
property Metadata: TJetStreamMsgMetadata;
property AckSubject: string;           // where an ack goes: the reply-to
property Acknowledged: Boolean;

procedure Ack;
procedure AckSync(ATimeoutMs: Cardinal = NatsConstants.DEFAULT_REQUEST_TIMEOUT);
procedure Nak; overload;
procedure Nak(ADelay: TJetStreamDuration); overload;
procedure InProgress;
procedure Term;
```

| Call | Means | Wire payload |
|---|---|---|
| `Ack` | Done with it. Fire and forget — returns as soon as the ack is written | `+ACK` |
| `AckSync` | Ack and wait for the server to confirm. **Blocks** | `+ACK` |
| `Nak` | Could not handle it: redeliver, without waiting out `AckWait` first | `-NAK` |
| `Nak(delay)` | As `Nak`, but not before `delay` has passed | `-NAK {"delay":n}` |
| `InProgress` | Still working. Resets `AckWait` without acknowledging | `+WPI` |
| `Term` | Never redeliver, whatever `MaxDeliver` says — a poison message | `+TERM` |

`Ack`, `Nak` and `Term` set `Acknowledged`, and **a second settlement raises**
`EJetStreamAckError`. `InProgress` deliberately does not: it settles nothing and
is the one ack meant to be repeated.

```pascal
// a long job that must not be redelivered while it is still running
LJs.SubscribePush('EVENTS', 'slow',
  procedure (const AMsg: IJetStreamMsg)
  begin
    TTask.Run(                                 // off the consumer thread
      procedure
      begin
        try
          while not Done do
          begin
            Step;
            AMsg.InProgress;                   // may be called as often as needed
          end;
          AMsg.Ack;
        except
          AMsg.Term;                           // it will fail every time
        end;
      end);
  end);
```

An interface, so a fetched batch frees itself — nothing to `Free`.

### TJetStreamMsgMetadata

JetStream puts a delivery's metadata in the **reply-to subject** rather than in
headers, and that same subject is where the ack is published:

```
$JS.ACK.<domain>.<hash>.<stream>.<consumer>.<delivered>.<stream seq>.<consumer seq>.<timestamp>.<pending>.<random>
```

```pascal
TJetStreamMsgMetadata = record
  Domain: string;            // '' when the server has none (or a V1 subject)
  AccountHash: string;       // '' on a V1 subject
  Stream: string;
  Consumer: string;
  NumDelivered: UInt64;      // counting this one, so it starts at 1
  StreamSeq: UInt64;
  ConsumerSeq: UInt64;
  TimestampNanos: UInt64;    // authoritative
  NumPending: UInt64;

  function IsRedelivery: Boolean;
  function TimestampUTC: TDateTime;   // lossy: TDateTime cannot hold nanoseconds

  class function TryParse(const ASubject: string;
    out AMetadata: TJetStreamMsgMetadata): Boolean; static;
end;
```

```pascal
if LMsg.Metadata.IsRedelivery then
  Writeln('attempt ', LMsg.Metadata.NumDelivered);
Writeln(LMsg.Metadata.NumPending, ' still to come');
```

Both the V1 (9-token) and V2 (12+ token) layouts are handled; V1 is normalised
onto V2 so there is one set of indices. V2's count is a **minimum**, so a future
server appending tokens will not break the parser.

### Wrapping a core message yourself

```pascal
class function TJetStreamMsg.TryWrap(AConnection: TNatsConnection;
  const AData: TNatsArgsMSG; out AMsg: IJetStreamMsg): Boolean; static;
constructor TJetStreamMsg.Create(AConnection: TNatsConnection;
  const AData: TNatsArgsMSG);
```

For a hand-rolled subscription on a push consumer's delivery subject.
`TryWrap` reports "not a JetStream delivery" — an ordinary core reply, or a
status message, both of which a subscription legitimately sees — while the
constructor raises.

```pascal
LConn.Subscribe('deliver.live',
  procedure (const AData: TNatsArgsMSG)
  var
    LMsg: IJetStreamMsg;
  begin
    if TJetStreamMsg.TryWrap(LConn, AData, LMsg) then
      LMsg.Ack;
  end);
```

---

## Durations and timestamps

**Every duration in JetStream is nanoseconds.** `TJetStreamDuration` is a
distinct `Int64` so the compiler helps:

```pascal
class function FromMillis(AValue: Int64): TJetStreamDuration; static;
class function FromSeconds(AValue: Int64): TJetStreamDuration; static;
class function FromMinutes(AValue: Int64): TJetStreamDuration; static;

function ToMillis: Int64;
function ToSeconds: Double;
```

```pascal
LCfg.MaxAge  := TJetStreamDuration.FromMinutes(30);
LCfg.AckWait := TJetStreamDuration.FromSeconds(30);
```

> Build them with these, never by writing the zeroes out. Passing milliseconds
> where the server wants nanoseconds is a factor-of-10⁶ error that still
> creates the stream — it just behaves nothing like you asked.

**Timestamps are `string`, not `TDateTime`**, and RFC3339. An empty stream
reports `0001-01-01T00:00:00Z`, which is outside the range `TDateTime` handles
comfortably and would either raise or land on the wrong date.

---

## Entities reference

`Nats.JetStream.Entities`. Records, serialized with Neon through
`JetStreamJSONConfig`, which derives the wire names (`max_msgs_per_subject`,
`deliver_subject`, …) from the Delphi ones. **A field name here is a protocol
field.**

### TJetStreamStreamConfig

| Field | Type | Notes |
|---|---|---|
| `Name` | `string` | No `.`, `*`, `>` or whitespace |
| `Subjects` | `TArray<string>` | What the stream captures |
| `Description` | `string` | |
| `Retention` | `TJetStreamRetention` | `Limits` (keep until a limit forces them out), `Interest` (drop once every consumer acked), `WorkQueue` (drop once **one** did) |
| `Storage` | `TJetStreamStorage` | `Filestore`, `Memory` |
| `Discard` | `TJetStreamDiscard` | When full: `Old` drops the oldest, `New` refuses the write |
| `MaxConsumers` | `Integer` | `-1` unlimited |
| `MaxMsgs`, `MaxBytes` | `Int64` | |
| `MaxMsgsPerSubject` | `Int64` | Per-subject history depth |
| `MaxMsgSize` | `Integer` | |
| `MaxAge` | `TJetStreamDuration` | |
| `DuplicateWindow` | `TJetStreamDuration` | How far back `Nats-Msg-Id` is remembered. Server default 2 min |
| `NumReplicas` | `Integer` | Server default is 1 when omitted |
| `NoAck` | `Boolean` | |
| `DenyDelete`, `DenyPurge` | `Boolean` | |
| `AllowRollupHdrs` | `Boolean` | Required for `Nats-Rollup`, i.e. for a KV purge |
| `AllowDirect`, `MirrorDirect` | `Boolean` | |
| `Sealed` | `Boolean` | Set by the server; a sealed stream cannot be changed |

Two Neon behaviours are load-bearing here:

- **Unset numbers, strings and durations are omitted**, so the server applies
  its own default and a read-modify-write does not rewrite fields you never
  touched.
- **Booleans and enums are always emitted** — Neon ignores the omit rule for
  them. That is safe only because each Delphi zero value equals the server's own
  default: retention `limits`, storage `file`, discard `old`, every flag false.
  The one field that broke that correspondence, `AckPolicy`, is a
  `Nullable<TJetStreamAckPolicy>` for exactly this reason: an unset policy is
  omitted from the request and the server applies its default (`explicit`).

### TJetStreamConsumerConfig

| Field | Type | Notes |
|---|---|---|
| `DurableName` | `string` | Set it to make the consumer durable |
| `Name`, `Description` | `string` | |
| `DeliverPolicy` | `TJetStreamDeliverPolicy` | `All`, `Last`, `New`, `ByStartSequence`, `ByStartTime`, `LastPerSubject` |
| `OptStartSeq` / `OptStartTime` | `UInt64` / `string` | Only read for `ByStartSequence` / `ByStartTime` |
| `AckPolicy` | `Nullable<TJetStreamAckPolicy>` | `None`, `All`, `Explicit`. Only `Explicit` makes sense for a work queue. Leave it unset to omit the field and get the server default (`explicit`); setting it to `None` explicitly sends `"ack_policy":"none"` |
| `AckWait` | `TJetStreamDuration` | Server default 30 s |
| `MaxDeliver` | `Integer` | `-1` unlimited |
| `FilterSubject` / `FilterSubjects` | `string` / `TArray<string>` | |
| `ReplayPolicy` | `TJetStreamReplayPolicy` | `Instant`, `Original` |
| `RateLimitBps` | `UInt64` | |
| `SampleFreq` | `string` | |
| `MaxWaiting` | `Integer` | Pull only: how many unfulfilled batch requests may queue |
| `MaxAckPending`, `MaxBatch` | `Integer` | |
| `MaxExpires`, `InactiveThreshold` | `TJetStreamDuration` | |
| `NumReplicas` | `Integer` | |
| `MemStorage`, `HeadersOnly` | `Boolean` | |
| `DeliverSubject` | `string` | **Setting it makes this a PUSH consumer** |
| `DeliverGroup` | `string` | |
| `FlowControl` | `Boolean` | Push only |
| `IdleHeartbeat` | `TJetStreamDuration` | |

### Info and response records

```pascal
TJetStreamStreamInfo = record
  Config: TJetStreamStreamConfig;
  State: TJetStreamStreamState;
  Created: string;
end;

TJetStreamStreamState = record
  Messages, Bytes, FirstSeq, LastSeq: UInt64;
  ConsumerCount, NumSubjects, NumDeleted: Integer;
  FirstTs, LastTs: string;
end;

TJetStreamConsumerInfo = record
  StreamName, Name, Created: string;
  Config: TJetStreamConsumerConfig;
  Delivered: TJetStreamSequenceInfo;   // the last message handed out
  AckFloor: TJetStreamSequenceInfo;    // everything below this is acknowledged
  NumAckPending, NumRedelivered, NumWaiting: Integer;
  NumPending: UInt64;
  PushBound: Boolean;
end;

TJetStreamAccountInfo = record
  Memory, Storage: UInt64;
  Streams, Consumers: Integer;
  Domain: string;
  Api: TJetStreamApiStats;
  Limits: TJetStreamAccountLimits;     // -1 means unlimited, hence signed
end;

TJetStreamStoredMsg = record
  Subject: string;
  Seq: UInt64;
  Data: string;      // BASE64
  Hdrs: string;      // BASE64 of the whole raw header block
  Time: string;      // RFC3339
end;
```

### Request records

```pascal
TJetStreamPurgeRequest = record
  Filter: string;    // only this subject, which may be a wildcard
  Seq: UInt64;       // only messages BELOW this sequence
  Keep: UInt64;      // keep this many of the most recent
end;

TJetStreamMsgGetRequest = record
  Seq: UInt64;
  LastBySubj: string;   // the current value of a KV key
  NextBySubj: string;
end;
```

### Serializing them yourself

```pascal
class function TJetStreamJSON.ToJSON<T: record>(const AValue: T): string; static;
class function TJetStreamJSON.FromJSON<T: record>(const AJson: string): T; static;
```

> **Always go through these** (or through `JetStreamJSONConfig` explicitly),
> never the parameterless `TNeon.ValueToJSONString` / `TNeon.JSONToValue`
> overloads. Those resolve to `TNeonConfiguration.Default`, which is PascalCase,
> so every field would be renamed — and it fails *silently*: the server ignores
> fields it does not recognise and answers with its own defaults, and a response
> deserializes into an empty record that reads exactly like a successful call.

---

## Exceptions

| Exception | Meaning | Carries |
|---|---|---|
| `EJetStreamApiError` | The server answered, and the answer was an error object | `ErrCode`, `Code`, `Error`, `IsNotFound` |
| `EJetStreamStatusError` | The server answered with a header status line instead of a body | `Status`, `Description` |
| `EJetStreamTimeout` | Nothing answered at all — usually means JetStream is not enabled | — |
| `EJetStreamAckError` | Acknowledging a message that cannot be, or already has been | — |
| `EJetStreamKVError` | An unusable bucket or key name, or a failed compare-and-set | — |
| `EJetStreamObjectError` | An unusable bucket or object name, or a failed digest check | — |

All descend from `ENatsException`.

**Branch on `ErrCode`**, which identifies the specific failure; `Code` is only
the broad HTTP-like class (404, 400, 503):

```pascal
try
  LJs.AddStream(LCfg);
except
  on E: EJetStreamApiError do
    if E.ErrCode = 10058 then         // stream name already in use
      LJs.UpdateStream(LCfg)
    else
      raise;
end;
```

The "it isn't there" family has a helper, since *create it if missing* is the
commonest reason to catch this at all:

```pascal
try
  LInfo := LJs.StreamInfo('EVENTS');
except
  on E: EJetStreamApiError do
    if E.IsNotFound then
      LInfo := LJs.AddStream(LCfg)
    else
      raise;
end;
```

Every reply passes one shared gate before the caller deserializes anything. It
rejects a **status line**, an **empty body** and an **error object** — all three
of which would otherwise deserialize into a record of zeroes that reads exactly
like a successful call whose every field happened to be a default.

---

## Key/Value

`Nats.JetStream.KV`. Key/Value adds **no protocol at all**. A bucket is the
stream `KV_<bucket>` capturing `$KV.<bucket>.>`; a key is a subject under it;
the current value is the **last message** on that subject; a revision **is** the
stream sequence; and a delete is a tombstone message rather than the removal of
one. Everything below is that convention and nothing more.

### Creating and binding

```pascal
class function CreateBucket(AContext: TJetStreamContext;
  const AConfig: TJetStreamKVConfig): TJetStreamKV; overload; static;
class function CreateBucket(AContext: TJetStreamContext;
  const ABucket: string): TJetStreamKV; overload; static;

constructor Create(AContext: TJetStreamContext; const ABucket: string);

class procedure DeleteBucket(AContext: TJetStreamContext; const ABucket: string); static;
class function ListBuckets(AContext: TJetStreamContext): TArray<string>; static;
```

`Create` binds to an **existing** bucket without a round trip — use `Status` to
confirm it is there. Both hand back an object the caller frees; neither owns the
context.

```pascal
var LCfg := Default(TJetStreamKVConfig);
LCfg.Bucket := 'config';
LCfg.History := 5;                                  // 5 revisions per key
LCfg.TTL := TJetStreamDuration.FromMinutes(60);
LCfg.Storage := TJetStreamStorage.Filestore;

var LKV := TJetStreamKV.CreateBucket(LJs, LCfg);
try
  LKV.Put('db.host', 'localhost');
finally
  LKV.Free;
end;
```

```pascal
TJetStreamKVConfig = record
  Bucket: string;
  Description: string;
  History: Integer;                // revisions kept per key; default 1, max 64
  TTL: TJetStreamDuration;         // 0 = forever
  MaxValueSize: Integer;           // 0 = the server's own limit
  MaxBytes: Int64;                 // 0 = unlimited
  Storage: TJetStreamStorage;
  Replicas: Integer;
end;
```

Four of the underlying stream settings are load-bearing and set for you:
`MaxMsgsPerSubject` **is** the history depth; `Discard: New` makes a full bucket
refuse a write rather than shed another key; `DenyDelete`; and
`AllowRollupHdrs`, without which `Purge` could not erase a key's history.

### Reading

```pascal
function Get(const AKey: string; out AEntry: TKVEntry): Boolean; overload;
function Get(const AKey: string; const ADefault: string = ''): string; overload;
function GetRevision(const AKey: string; ARevision: UInt64;
  out AEntry: TKVEntry): Boolean;
function History(const AKey: string): TArray<TKVEntry>;
function Keys: TArray<string>;
function Status: TJetStreamKVStatus;
```

```pascal
var LEntry: TKVEntry;
if LKV.Get('db.host', LEntry) then
  Writeln(LEntry.ValueString, ' @ revision ', LEntry.Revision);

// or, when a default will do
Writeln(LKV.Get('db.host', 'localhost'));

for var LKey in LKV.Keys do
  Writeln(LKey);

for var LRev in LKV.History('db.host') do
  if LRev.IsDelete then
    Writeln(LRev.Revision, ': deleted')
  else
    Writeln(LRev.Revision, ': ', LRev.ValueString);
```

`Get` returning `False` means the key is not set — never set, or deleted —
which is an ordinary answer rather than an error. It checks the tombstone, so a
deleted key never comes back as an empty value. A bucket whose stream does not
exist raises `EJetStreamApiError` instead, so "the bucket is gone" stays
distinguishable from "the key is absent".

`Keys` lists only keys currently **holding a value**; deleted and purged keys
are left out, even though their tombstones are still in the stream.

```pascal
TKVEntry = record
  Bucket: string;
  Key: string;
  Value: TBytes;              // empty for anything but a Put
  Revision: UInt64;           // the STREAM sequence: global to the bucket, not per key
  Created: string;            // RFC3339
  Operation: TKVOperation;    // Put, Delete, Purge

  function ValueString: string;
  function IsDelete: Boolean;
end;
```

> `Revision` is a *stream* sequence, so it is unique across the whole bucket and
> revisions of one key are **not** consecutive. `GetRevision` checks the
> returned subject for exactly that reason.

### Writing

```pascal
function Put(const AKey: string; const AValue: TBytes): UInt64; overload;
function Put(const AKey, AValue: string): UInt64; overload;

function PutIfAbsent(const AKey: string; const AValue: TBytes): UInt64; overload;
function PutIfAbsent(const AKey, AValue: string): UInt64; overload;

function Update(const AKey: string; const AValue: TBytes; ARevision: UInt64): UInt64; overload;
function Update(const AKey, AValue: string; ARevision: UInt64): UInt64; overload;

procedure Delete(const AKey: string);
procedure Purge(const AKey: string);
```

All the writers return the new revision.

```pascal
LKV.Put('db.host', 'db1.internal');                    // whatever it held before

try
  LKV.PutIfAbsent('lock.leader', LMyId);               // NATS KV calls this Create
except
  on E: EJetStreamKVError do
    Writeln('somebody else holds it');
end;

// compare-and-set
var LEntry: TKVEntry;
if LKV.Get('counter', LEntry) then
  LKV.Update('counter', IntToStr(LEntry.ValueString.ToInteger + 1), LEntry.Revision);
```

`PutIfAbsent` and `Update` are nothing but the publish expectations
(`Nats-Expected-Last-Subject-Sequence`), which is why an expectation *of zero*
had to be expressible.

`Delete` keeps the history — a tombstone is appended, so `History` still shows
what the value used to be. `Purge` removes the key **and** its history by
rolling the subject up to a single tombstone; the old values are gone for good.

### Names

```pascal
class procedure CheckBucket(const ABucket: string); static;
class procedure CheckKey(const AKey: string); static;
```

A **bucket** name becomes part of a stream name *and* of every subject, so it is
restricted to letters, digits, `_` and `-`. A **key** becomes a subject token,
so dots are fine — they simply make several tokens — but wildcards are not, and
it may neither begin nor end with a dot. Both raise `EJetStreamKVError`.

```pascal
property Bucket: string;
property StreamName: string;      // KV_<bucket>
```

---

## Object Store

`Nats.JetStream.ObjectStore`. A second convention over a stream, and a thicker
one, because `max_payload` is a hard ~1 MB server limit: an object has to be
**split**. A bucket is the stream `OBJ_<bucket>` capturing two subject spaces —
`$O.<b>.C.>` for chunks and `$O.<b>.M.>` for metadata.

**Every chunk of one object shares one subject**, keyed by a NUID rather than by
the object's name. That is what makes stream order equal chunk order, and what
lets a replace write a new NUID and purge the old subject while a reader is
still finishing the old version.

### Creating and binding

```pascal
class function CreateBucket(AContext: TJetStreamContext;
  const AConfig: TJetStreamObjectStoreConfig): TJetStreamObjectStore; overload; static;
class function CreateBucket(AContext: TJetStreamContext;
  const ABucket: string): TJetStreamObjectStore; overload; static;

constructor Create(AContext: TJetStreamContext; const ABucket: string;
  AChunkSize: Integer = 0);

class procedure DeleteBucket(AContext: TJetStreamContext; const ABucket: string); static;
class function ListBuckets(AContext: TJetStreamContext): TArray<string>; static;
```

```pascal
TJetStreamObjectStoreConfig = record
  Bucket: string;
  Description: string;
  TTL: TJetStreamDuration;    // 0 = forever
  MaxBytes: Int64;
  Storage: TJetStreamStorage;
  Replicas: Integer;
  ChunkSize: Integer;         // 0 uses DEFAULT_CHUNK_SIZE (128 KB)
end;
```

```pascal
var LOs := TJetStreamObjectStore.CreateBucket(LJs, 'files');
try
  LOs.PutFile('report.pdf', 'C:\tmp\report.pdf');
finally
  LOs.Free;
end;
```

### Writing

```pascal
function Put(const AName: string; AStream: TStream): TJetStreamObjectInfo; overload;
function Put(const AName: string; const AData: TBytes): TJetStreamObjectInfo; overload;
function PutString(const AName, AData: string): TJetStreamObjectInfo;
function PutFile(const AName, AFileName: string): TJetStreamObjectInfo;
```

Each replaces whatever was stored under that name. The stream overload reads
from the current position to the end.

```pascal
var LInfo := LOs.PutFile('backup.zip', 'C:\tmp\backup.zip');
Writeln(LInfo.Size, ' bytes in ', LInfo.Chunks, ' chunks');
```

### Reading

```pascal
function Get(const AName: string; ADest: TStream): Boolean; overload;
function Get(const AName: string; out AData: TBytes): Boolean; overload;
function GetString(const AName: string; const ADefault: string = ''): string;
function GetFile(const AName, AFileName: string): Boolean;
```

`False` means no such object — never stored, or deleted.

```pascal
if not LOs.GetFile('backup.zip', 'C:\restore\backup.zip') then
  Writeln('no such object');

Writeln(LOs.GetString('greeting.txt', '(missing)'));
```

A read verifies **both** the chunk count and the SHA-256 digest, raising
`EJetStreamObjectError` on a mismatch. Without those checks a truncated object
is just a shorter file with nothing having reported an error.

### Listing and deleting

```pascal
function Info(const AName: string; out AInfo: TJetStreamObjectInfo): Boolean;
function List: TArray<TJetStreamObjectInfo>;
function Status: TJetStreamObjectStoreStatus;
procedure Delete(const AName: string);
```

```pascal
for var LObj in LOs.List do
  Writeln(Format('%-30s %10d bytes  %s', [LObj.Name, LObj.Size, LObj.Mtime]));
```

`Delete` removes the object's bytes and marks its metadata deleted. The metadata
record itself stays, so a name that *was* stored is distinguishable from one
that never existed — `Info` still finds it, `List` leaves it out.

```pascal
TJetStreamObjectInfo = record
  Name: string;
  Description: string;
  Bucket: string;
  Nuid: string;         // identifies the CHUNK subject; regenerated on every write
  Size: UInt64;
  Chunks: Integer;
  Digest: string;       // 'SHA-256=<base64url>' over the whole object
  Mtime: string;        // RFC3339
  Deleted: Boolean;
  Options: TJetStreamObjectOptions;   // MaxChunkSize
end;

TJetStreamObjectStoreStatus = record
  Bucket: string;
  StreamName: string;
  Messages: UInt64;     // chunks AND metadata records - not the number of objects
  Bytes: UInt64;
  TTL: TJetStreamDuration;
end;
```

```pascal
property Bucket: string;
property StreamName: string;   // OBJ_<bucket>
property ChunkSize: Integer;
```

### Names

Object names are arbitrary text — usually file names, with spaces and slashes —
so they are **base64url-encoded, unpadded**, into the subject rather than used
raw. The only name rejected is an empty one. `TObjectStoreEncoding` is public so
anyone debugging a bucket can work out which subject an object lives on:

```pascal
class function TObjectStoreEncoding.Encode(const AData: TBytes): string; overload; static;
class function TObjectStoreEncoding.Encode(const AText: string): string; overload; static;
```

> **An object bucket must NOT set `MaxMsgsPerSubject`** the way a KV bucket
> does: every chunk of an object shares one subject, so it would silently delete
> the start of every large object.

---

## Constants

`JetStreamConstants` (`Nats.JetStream.Consts`), nested in the same idiom as
`NatsConstants`. Use these, never string literals.

| Group | Holds |
|---|---|
| `Api` | `PREFIX` (`$JS.API.`), `PREFIX_DOMAIN`, and every endpoint template — `STREAM_CREATE`, `CONSUMER_MSG_NEXT`, `STREAM_MSG_GET`, … |
| `Header` | `MSG_ID`, `EXPECTED_STREAM`, `EXPECTED_LAST_SEQ`, `EXPECTED_LAST_SUBJECT_SEQ`, `EXPECTED_LAST_MSG_ID`, `ROLLUP` |
| `KV` | `STREAM_PREFIX` (`KV_`), the subject templates, `HEADER_OPERATION`, `DEFAULT_HISTORY` (1), `MAX_HISTORY` (64) |
| `Obj` | `STREAM_PREFIX` (`OBJ_`), the subject templates, `DEFAULT_CHUNK_SIZE` (128 KB), `DIGEST_PREFIX` |
| `Ack` | `PREFIX` (`$JS.ACK.`), the payloads (`+ACK`, `-NAK`, `+WPI`, `+TERM`), the token counts and positions |
| `Naming` | `INVALID_CHARS` — what a stream or consumer name may not contain |

A note worth internalising: **nats-server silently ignores a header or JSON
field whose name it does not recognise**, and answers with its own default. A
misspelled `Nats-Msg-Id` does not fail — it just stops deduplicating. That is
why the live test suite exists alongside the mock one, and why these constants
are not to be retyped by hand. The one exception is `Nats-Rollup`, whose
*value* the server validates.

---

## See also

- [Core-API.md](Core-API.md) — connections, publishing, subscribing, headers
- `Demos\JetStreamJson.dpr` — every entity pretty-printed, for eyeballing the wire format
- `Demos\JetStreamBench.dpr` — serialization benchmark for the entity layer
- `Tests\Source\Nats.Tests.JetStream.pas` — the offline suite, driven against a mock socket
- `Tests\Source\Nats.Tests.Live.pas` — round trips against a real server with JetStream enabled
