{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.JetStream.KV;

interface

{$SCOPEDENUMS ON}

uses
  System.SysUtils, System.Classes, System.NetEncoding, System.Generics.Collections,
  System.SyncObjs,

  Nats.Consts,
  Nats.Classes,
  Nats.Parser,
  Nats.Connection,
  Nats.Exceptions,
  Nats.JetStream.Consts,
  Nats.JetStream.Client,
  Nats.JetStream.Entities,
  Nats.JetStream.Message;

type
  /// <summary>
  ///   What a stored revision of a key actually was
  /// </summary>
  /// <remarks>
  ///   A delete is not the absence of a message - it IS a message, a tombstone,
  ///   which is why history can show one. Only Put carries a value.
  /// </remarks>
  TKVOperation = (Put, Delete, Purge);

  /// <summary>
  ///   One revision of one key
  /// </summary>
  /// <remarks>
  ///   A record rather than an interface: it is data with no behaviour and no
  ///   connection behind it, so there is nothing to own and nothing to free.
  /// </remarks>
  TKVEntry = record
    Bucket: string;
    Key: string;
    /// Empty for anything but a Put - a tombstone has no value
    Value: TBytes;
    /// <summary>
    ///   The stream sequence this revision was stored at. It is global to the
    ///   bucket, not per key, so revisions of one key are not consecutive
    /// </summary>
    Revision: UInt64;
    /// RFC3339, as text - see TJetStreamStoredMsg for why
    Created: string;
    Operation: TKVOperation;

    /// The value read as UTF-8. Empty when the entry is a tombstone
    function ValueString: string;
    /// True when this revision removed the key rather than setting it
    function IsDelete: Boolean;
  end;

  /// <summary>
  ///   What a bucket is created with. Everything here maps onto an ordinary
  ///   stream setting - a bucket IS a stream
  /// </summary>
  TJetStreamKVConfig = record
    Bucket: string;
    Description: string;
    /// <summary>
    ///   How many revisions of each key to keep. 1 - the default - means only
    ///   the current value, which is what most callers want
    /// </summary>
    History: Integer;
    /// How long a value lives before the server drops it. 0 means forever
    TTL: TJetStreamDuration;
    /// Ceiling on one value, in bytes. 0 means the server's own limit
    MaxValueSize: Integer;
    /// Ceiling on the whole bucket. 0 means unlimited
    MaxBytes: Int64;
    Storage: TJetStreamStorage;
    Replicas: Integer;
  end;

  TJetStreamKVStatus = record
    Bucket: string;
    /// <summary>
    ///   How many keys currently hold at least one message - live keys plus
    ///   tombstoned ones. A key with several revisions counts ONCE: this is
    ///   the server's subject count (num_subjects), not the message count
    /// </summary>
    Values: UInt64;
    /// Revisions kept per key
    History: Int64;
    TTL: TJetStreamDuration;
    Bytes: UInt64;
    /// The stream behind the bucket, for anyone who wants to look
    StreamName: string;
  end;

  /// <summary>
  ///   Raised for a bucket or key name a bucket cannot hold, and for a failed
  ///   compare-and-set
  /// </summary>
  EJetStreamKVError = class(ENatsException);

  /// <summary>
  ///   How a watch behaves. The empty set is the common case: the current value
  ///   of every matching key first, then every change as it happens
  /// </summary>
  TKVWatchOption = (
    /// Every revision still held, oldest first, instead of only the current one
    IncludeHistory,
    /// No initial values at all - only changes made after the watch starts
    UpdatesOnly,
    /// Deletes and purges are not reported
    IgnoreDeletes,
    /// Values are left on the server: entries arrive with an empty Value
    MetaOnly
  );
  TKVWatchOptions = set of TKVWatchOption;

  /// <summary>
  ///   Called once per change. Runs on the connection's consumer thread, so it
  ///   must not block on a reply - no Get, Put or any other bucket call
  ///   inside it - and must marshal anything that touches the UI
  /// </summary>
  TKVWatchHandler = reference to procedure(const AEntry: TKVEntry);

  /// <summary>
  ///   A running watch. Releasing the last reference stops it
  /// </summary>
  IJetStreamKVWatcher = interface
    ['{5C0F6A3E-2B7D-4E8A-9C1F-3D6B8E2A4F71}']
    function GetInitialValuesDone: Boolean;
    /// <summary>
    ///   Blocks until every value that existed when the watch started has
    ///   been handed to the handler. False on timeout. Never call it from a
    ///   handler: the thread it would block is the one delivering them
    /// </summary>
    function WaitForInitialValues(ATimeoutMs: Cardinal): Boolean;
    /// <summary>
    ///   Stops delivery and deletes the server-side consumer. Blocks on that
    ///   delete, so not from a handler either. Safe to call twice
    /// </summary>
    procedure Stop;
    /// True once the initial values are all delivered - immediately, for
    /// UpdatesOnly or a watch that matched nothing
    property InitialValuesDone: Boolean read GetInitialValuesDone;
  end;

  /// <summary>
  ///   A handle on one key/value bucket
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     Key/Value adds no protocol whatsoever. A bucket is a stream named
  ///     KV_&lt;bucket&gt; capturing '$KV.&lt;bucket&gt;.&gt;'; a key is a
  ///     subject under it; the current value is the LAST message on that
  ///     subject; a delete is a tombstone message; and a revision is the stream
  ///     sequence. Everything below is that convention and nothing more.
  ///   </para>
  ///   <para>
  ///     Every method BLOCKS, and inherits TJetStreamContext's rule with it:
  ///     never call one from a message, connect or disconnect handler.
  ///   </para>
  ///   <para>
  ///     Does NOT own the context - it is constructed over one and expects it
  ///     to outlive the bucket handle.
  ///   </para>
  /// </remarks>
  TJetStreamKV = class
  private
    FContext: TJetStreamContext;
    FBucket: string;
    FStream: string;

    /// The key a '$KV.<bucket>.<key>' subject names
    function SubjectKey(const ASubject: string): string;
    /// <summary>
    ///   Turns a stored message into an entry, reading the operation out of its
    ///   header block. No KV-Operation header at all means a plain value
    /// </summary>
    function EntryFromStored(const AMsg: TJetStreamStoredMsg): TKVEntry;
    function EntryFromDelivered(const AMsg: IJetStreamMsg): TKVEntry;
    /// <summary>
    ///   EntryFromDelivered without Self - a watch outlives nothing it should
    ///   not, and its handler must keep working if this handle is freed first
    /// </summary>
    class function DeliveredEntry(const ABucket: string;
      const AMsg: IJetStreamMsg): TKVEntry; static;
    /// Publishes to the key's subject and returns the revision it landed at
    function PutRaw(const AKey: string; const AValue: TBytes;
      AOptions: TJetStreamPubOptions): UInt64;
    /// <summary>
    ///   True when AErrCode means "a publish expectation did not hold" - the
    ///   only failure PutIfAbsent and Update translate into EJetStreamKVError.
    ///   Everything else must propagate unchanged
    /// </summary>
    class function IsExpectationError(AErrCode: Integer): Boolean; static;
  public
    /// <summary>
    ///   Binds to an EXISTING bucket without a round trip. Use Status to
    ///   confirm it is there
    /// </summary>
    constructor Create(AContext: TJetStreamContext; const ABucket: string);

    { bucket management }

    /// Creates the bucket's stream and returns a handle on it
    class function CreateBucket(AContext: TJetStreamContext;
      const AConfig: TJetStreamKVConfig): TJetStreamKV; overload; static;
    /// The common case: a bucket with all the defaults
    class function CreateBucket(AContext: TJetStreamContext;
      const ABucket: string): TJetStreamKV; overload; static;
    class procedure DeleteBucket(AContext: TJetStreamContext;
      const ABucket: string); static;
    /// Every bucket in the account, by name rather than by stream name
    class function ListBuckets(AContext: TJetStreamContext): TArray<string>; static;

    /// <summary>
    ///   Raises unless ABucket is usable. A bucket name goes into a stream name
    ///   AND into every subject, so it is checked against both
    /// </summary>
    class procedure CheckBucket(const ABucket: string); static;
    /// <summary>
    ///   Raises unless AKey is usable. A key becomes a subject token: dots are
    ///   fine and simply make several tokens, wildcards are not, and it may
    ///   neither begin nor end with a dot
    /// </summary>
    class procedure CheckKey(const AKey: string); static;
    /// <summary>
    ///   CheckKey, except that '*' and '&gt;' are allowed as whole tokens - a
    ///   watch takes a subject filter, not a single key
    /// </summary>
    class procedure CheckKeyFilter(const AKey: string); static;
    /// The subject '$KV.<bucket>.<key>' that AKey is stored on - the bucket's
    /// stream captures it. Needed by anything that purges or scans the stream
    /// directly rather than going through this class
    function KeySubject(const AKey: string): string;

    { reading }

    /// <summary>
    ///   The current value. False means the key is not set - never set, or
    ///   deleted - which is an ordinary answer rather than an error. A bucket
    ///   whose stream does not exist raises EJetStreamApiError instead, so a
    ///   missing bucket can never be mistaken for an unset key
    /// </summary>
    function Get(const AKey: string; out AEntry: TKVEntry): Boolean; overload;
    /// The current value as text, or ADefault when the key is not set
    function Get(const AKey: string; const ADefault: string = ''): string; overload;
    /// <summary>
    ///   One specific revision, deleted ones included. False means no message
    ///   at that sequence, or one belonging to a different key
    /// </summary>
    function GetRevision(const AKey: string; ARevision: UInt64;
      out AEntry: TKVEntry): Boolean;
    /// <summary>
    ///   Every revision of AKey still held, oldest first, tombstones included.
    ///   Bounded by the bucket's History
    /// </summary>
    function History(const AKey: string): TArray<TKVEntry>;
    /// <summary>
    ///   Every key currently holding a value. Deleted and purged keys are left
    ///   out - their tombstones are still in the stream, but they are not keys
    /// </summary>
    function Keys: TArray<string>;
    function Status: TJetStreamKVStatus;

    { writing }

    /// Sets AKey whatever it held before, and returns the new revision
    function Put(const AKey: string; const AValue: TBytes): UInt64; overload;
    function Put(const AKey, AValue: string): UInt64; overload;
    /// <summary>
    ///   Sets AKey only if it holds nothing - NATS KV calls this Create; the
    ///   name is taken here by the constructor. Raises EJetStreamKVError when
    ///   the key already exists; any OTHER API error - a bucket that was never
    ///   created, a quota - propagates unchanged as EJetStreamApiError
    /// </summary>
    function PutIfAbsent(const AKey: string; const AValue: TBytes): UInt64; overload;
    function PutIfAbsent(const AKey, AValue: string): UInt64; overload;
    /// <summary>
    ///   Compare-and-set: writes only if AKey is still at ARevision, and raises
    ///   EJetStreamKVError otherwise. The revision comes from a previous Get.
    ///   As with PutIfAbsent, only the lost race is translated - every other
    ///   API error propagates unchanged
    /// </summary>
    function Update(const AKey: string; const AValue: TBytes;
      ARevision: UInt64): UInt64; overload;
    function Update(const AKey, AValue: string; ARevision: UInt64): UInt64; overload;

    /// <summary>
    ///   Removes the key but keeps its history: a tombstone is appended, so
    ///   History still shows what the value used to be
    /// </summary>
    procedure Delete(const AKey: string);
    /// <summary>
    ///   Removes the key AND its history, by rolling the subject up to a single
    ///   tombstone. The old values are gone for good
    /// </summary>
    procedure Purge(const AKey: string);

    { watching }

    /// <summary>
    ///   Calls AHandler for every change to the keys AKey matches, until the
    ///   returned watcher is stopped or released. AKey may use '*' for one
    ///   token and a trailing '&gt;' for the rest, as a subject filter does
    /// </summary>
    /// <remarks>
    ///   <para>
    ///     Unless UpdatesOnly is given, the handler first receives the current
    ///     value of every matching key - deleted keys included, as tombstones,
    ///     so a cache built from the watch knows to drop them. Watcher's
    ///     WaitForInitialValues tells when that initial set is complete.
    ///   </para>
    ///   <para>
    ///     Built on an ephemeral, unacknowledged push consumer, which is what
    ///     every NATS client does. It is NOT an ordered consumer: there is no
    ///     automatic recreation after a gap, so a watch does not survive a
    ///     dropped connection - start a new one.
    ///   </para>
    ///   <para>
    ///     Blocks while the consumer is created, so never call it from a
    ///     handler. The handler itself runs on the consumer thread.
    ///   </para>
    /// </remarks>
    function Watch(const AKey: string; AHandler: TKVWatchHandler; AOptions: TKVWatchOptions = []): JetStreamKVWatcher;
    /// Watch('&gt;'): every key in the bucket
    function WatchAll(AHandler: TKVWatchHandler; AOptions: TKVWatchOptions = []): IJetStreamKVWatcher;

    property Bucket: string read FBucket;
    /// KV_<bucket> - a bucket is a stream, and this is the stream
    property StreamName: string read FStream;
  end;

implementation

const
  /// <summary>
  ///   How often the server proves a quiet watch is still alive. Required for
  ///   flow control, which a push consumer without heartbeats cannot enable
  /// </summary>
  WATCH_HEARTBEAT_SECONDS = 5;
  /// <summary>
  ///   How long the server keeps a watch consumer nobody is listening to. The
  ///   safety net for a client that dies without stopping its watch
  /// </summary>
  WATCH_IDLE_SECONDS = 60;

type
  /// <summary>
  ///   What the delivery handler needs, kept apart from the watcher so the
  ///   handler's reference does not keep the watcher alive - releasing the
  ///   watcher is what stops the watch
  /// </summary>
  IKVWatchState = interface
    ['{8E4B1D27-6A3C-4F95-B0D2-71C5E9A3F648}']
    procedure Deliver(const AMsg: IJetStreamMsg);
    procedure MarkInitialDone;
    procedure Halt;
    function IsInitialDone: Boolean;
    function WaitInitial(ATimeoutMs: Cardinal): Boolean;
  end;

  TKVWatchState = class(TInterfacedObject, IKVWatchState)
  private
    FBucket: string;
    FHandler: TKVWatchHandler;
    FOptions: TKVWatchOptions;
    FInitial: TLightweightEvent;
    FHalted: Integer;
  public
    constructor Create(const ABucket: string; AHandler: TKVWatchHandler;
      AOptions: TKVWatchOptions);
    destructor Destroy; override;

    procedure Deliver(const AMsg: IJetStreamMsg);
    procedure MarkInitialDone;
    procedure Halt;
    function IsInitialDone: Boolean;
    function WaitInitial(ATimeoutMs: Cardinal): Boolean;
  end;

  TJetStreamKVWatcher = class(TInterfacedObject, IJetStreamKVWatcher)
  private
    FContext: TJetStreamContext;
    FStream: string;
    FConsumer: string;
    FSid: Integer;
    FState: IKVWatchState;
    FStopped: Integer;
  public
    constructor Create(AContext: TJetStreamContext; const AStream, AConsumer: string;
      ASid: Integer; const AState: IKVWatchState);
    destructor Destroy; override;

    function GetInitialValuesDone: Boolean;
    function WaitForInitialValues(ATimeoutMs: Cardinal): Boolean;
    procedure Stop;
  end;

{ TKVWatchState }

constructor TKVWatchState.Create(const ABucket: string; AHandler: TKVWatchHandler;
  AOptions: TKVWatchOptions);
begin
  inherited Create;
  FBucket := ABucket;
  FHandler := AHandler;
  FOptions := AOptions;
  FInitial := TLightweightEvent.Create(False);
end;

destructor TKVWatchState.Destroy;
begin
  FInitial.Free;
  inherited;
end;

procedure TKVWatchState.Deliver(const AMsg: IJetStreamMsg);
var
  LEntry: TKVEntry;
begin
  { Deliveries already queued when the watch was stopped still reach this
    point - the caller asked for them to stop, so they stop here }
  if TInterlocked.CompareExchange(FHalted, 0, 0) <> 0 then
    Exit;

  LEntry := TJetStreamKV.DeliveredEntry(FBucket, AMsg);

  if not (LEntry.IsDelete and (TKVWatchOption.IgnoreDeletes in FOptions)) then
    FHandler(LEntry);

  { NumPending is what the consumer still has to send. Zero on a delivery
    means the backlog that existed when the watch started is drained - AFTER
    the handler, so a waiter released by it sees that entry handled. Checked
    even for an ignored delete, which can be the last of the backlog }
  if AMsg.Metadata.NumPending = 0 then
    MarkInitialDone;
end;

procedure TKVWatchState.MarkInitialDone;
begin
  FInitial.SetEvent;
end;

procedure TKVWatchState.Halt;
begin
  TInterlocked.Exchange(FHalted, 1);
end;

function TKVWatchState.IsInitialDone: Boolean;
begin
  Result := FInitial.IsSet;
end;

function TKVWatchState.WaitInitial(ATimeoutMs: Cardinal): Boolean;
begin
  Result := FInitial.WaitFor(ATimeoutMs) = TWaitResult.wrSignaled;
end;

{ TJetStreamKVWatcher }

constructor TJetStreamKVWatcher.Create(AContext: TJetStreamContext;
  const AStream, AConsumer: string; ASid: Integer; const AState: IKVWatchState);
begin
  inherited Create;
  FContext := AContext;
  FStream := AStream;
  FConsumer := AConsumer;
  FSid := ASid;
  FState := AState;
end;

destructor TJetStreamKVWatcher.Destroy;
begin
  { Releasing the watcher IS stopping it - a watch nobody holds would go on
    receiving and discarding every change for the life of the connection.
    A destructor must not raise, and the consumer's inactivity threshold
    cleans up whatever this could not }
  try
    Stop;
  except
  end;
  inherited;
end;

function TJetStreamKVWatcher.GetInitialValuesDone: Boolean;
begin
  Result := FState.IsInitialDone;
end;

function TJetStreamKVWatcher.WaitForInitialValues(ATimeoutMs: Cardinal): Boolean;
begin
  Result := FState.WaitInitial(ATimeoutMs);
end;

procedure TJetStreamKVWatcher.Stop;
var
  LConnection: TNatsConnection;
begin
  if TInterlocked.Exchange(FStopped, 1) <> 0 then
    Exit;

  { First, so nothing more reaches the handler whatever happens below }
  FState.Halt;

  LConnection := FContext.Connection;

  if not LConnection.Connected then
  begin
    { Nothing to tell the server, which dropped the consumer's interest along
      with the connection. Unsubscribe still clears the local entry - it
      removes it before it tries to write, and the write is what fails }
    try
      LConnection.Unsubscribe(FSid);
    except
    end;
    Exit;
  end;

  { Unsubscribing first removes the consumer's interest, so the server stops
    pushing before it is asked to delete }
  LConnection.Unsubscribe(FSid);

  try
    FContext.DeleteConsumer(FStream, FConsumer);
  except
    { Already gone - the inactivity threshold got there first, or the bucket
      was deleted under the watch. Either way the watch is stopped }
    on E: EJetStreamApiError do ;
    on E: EJetStreamTimeout do ;
  end;
end;

{ TKVEntry }

function TKVEntry.ValueString: string;
begin
  if Length(Value) = 0 then
    Exit(String.Empty);

  Result := TEncoding.UTF8.GetString(Value);
end;

function TKVEntry.IsDelete: Boolean;
begin
  Result := Operation <> TKVOperation.Put;
end;

{ TJetStreamKV }

constructor TJetStreamKV.Create(AContext: TJetStreamContext; const ABucket: string);
begin
  inherited Create;

  if not Assigned(AContext) then
    raise EJetStreamKVError.Create('A key/value bucket needs a JetStream context');

  CheckBucket(ABucket);

  FContext := AContext;
  FBucket := ABucket;
  FStream := JetStreamConstants.KV.STREAM_PREFIX + ABucket;
end;

class procedure TJetStreamKV.CheckBucket(const ABucket: string);
var
  LChar: Char;
begin
  if ABucket.IsEmpty then
    raise EJetStreamKVError.Create('A bucket name cannot be empty');

  for LChar in ABucket do
    if not CharInSet(LChar, JetStreamConstants.KV.VALID_BUCKET_CHARS) then
      raise EJetStreamKVError.CreateFmt(
        'Bucket name [%s] cannot contain %s - a bucket name becomes part of a ' +
        'stream name and of every subject in it, so only letters, digits, ' +
        'underscore and hyphen are allowed', [ABucket, QuotedStr(LChar)]);
end;

class procedure TJetStreamKV.CheckKey(const AKey: string);
var
  LChar: Char;
begin
  if AKey.IsEmpty then
    raise EJetStreamKVError.Create('A key cannot be empty');

  { A leading or trailing dot would produce an empty subject token, which is
    not the same subject and does not address this key }
  if AKey.StartsWith(NatsConstants.SEP) or AKey.EndsWith(NatsConstants.SEP) then
    raise EJetStreamKVError.CreateFmt(
      'Key [%s] cannot begin or end with a dot - that would put an empty token ' +
      'in the subject', [AKey]);

  for LChar in AKey do
    if not CharInSet(LChar, JetStreamConstants.KV.VALID_KEY_CHARS) then
      raise EJetStreamKVError.CreateFmt(
        'Key [%s] cannot contain %s - a key becomes a subject token, so a ' +
        'wildcard or a space would address something else entirely',
        [AKey, QuotedStr(LChar)]);
end;

class function TJetStreamKV.CreateBucket(AContext: TJetStreamContext;
  const AConfig: TJetStreamKVConfig): TJetStreamKV;
var
  LStream: TJetStreamStreamConfig;
  LHistory: Integer;
begin
  CheckBucket(AConfig.Bucket);

  LHistory := AConfig.History;
  if LHistory <= 0 then
    LHistory := JetStreamConstants.KV.DEFAULT_HISTORY;

  if LHistory > JetStreamConstants.KV.MAX_HISTORY then
    raise EJetStreamKVError.CreateFmt(
      'A history of %d is beyond the server''s ceiling of %d revisions per key',
      [LHistory, JetStreamConstants.KV.MAX_HISTORY]);

  LStream := Default(TJetStreamStreamConfig);
  LStream.Name := JetStreamConstants.KV.STREAM_PREFIX + AConfig.Bucket;
  LStream.Subjects := [Format(JetStreamConstants.KV.SUBJECT_ALL, [AConfig.Bucket])];
  LStream.Description := AConfig.Description;

  { History IS MaxMsgsPerSubject: keeping N revisions of a key is keeping N
    messages on its subject, and there is nothing else to it }
  LStream.MaxMsgsPerSubject := LHistory;
  LStream.MaxAge := AConfig.TTL;
  LStream.MaxMsgSize := AConfig.MaxValueSize;
  LStream.MaxBytes := AConfig.MaxBytes;
  LStream.Storage := AConfig.Storage;
  LStream.NumReplicas := AConfig.Replicas;

  { The four settings that make a stream behave like a key/value store:
    - Discard New, so a full bucket REFUSES a write rather than silently
      dropping the oldest key, which would lose data with no error anywhere
    - DenyDelete, so nobody removes a revision behind the bucket's back
    - AllowRollupHdrs, without which Purge cannot erase a key's history
    - AllowDirect, which lets a replica serve a get }
  LStream.Discard := TJetStreamDiscard.New;
  LStream.DenyDelete := True;
  LStream.AllowRollupHdrs := True;
  LStream.AllowDirect := True;

  AContext.AddStream(LStream);

  Result := TJetStreamKV.Create(AContext, AConfig.Bucket);
end;

class function TJetStreamKV.CreateBucket(AContext: TJetStreamContext;
  const ABucket: string): TJetStreamKV;
var
  LConfig: TJetStreamKVConfig;
begin
  LConfig := Default(TJetStreamKVConfig);
  LConfig.Bucket := ABucket;

  Result := CreateBucket(AContext, LConfig);
end;

class procedure TJetStreamKV.DeleteBucket(AContext: TJetStreamContext;
  const ABucket: string);
begin
  CheckBucket(ABucket);
  AContext.DeleteStream(JetStreamConstants.KV.STREAM_PREFIX + ABucket);
end;

class function TJetStreamKV.ListBuckets(AContext: TJetStreamContext): TArray<string>;
var
  LNames: TArray<string>;
  LName: string;
  LResult: TList<string>;
begin
  LNames := AContext.StreamNames;

  LResult := TList<string>.Create;
  try
    { Every KV bucket is a stream, but not every stream is a bucket - so this
      filters on the prefix and hands back BUCKET names, not stream names }
    for LName in LNames do
      if LName.StartsWith(JetStreamConstants.KV.STREAM_PREFIX) then
        LResult.Add(LName.Substring(Length(JetStreamConstants.KV.STREAM_PREFIX)));

    Result := LResult.ToArray;
  finally
    LResult.Free;
  end;
end;

function TJetStreamKV.KeySubject(const AKey: string): string;
begin
  Result := Format(JetStreamConstants.KV.SUBJECT_KEY, [FBucket, AKey]);
end;

function TJetStreamKV.SubjectKey(const ASubject: string): string;
var
  LPrefix: string;
begin
  { '$KV.<bucket>.' - everything after it is the key, dots and all, because a
    key may legitimately span several tokens }
  LPrefix := Format(JetStreamConstants.KV.SUBJECT_KEY, [FBucket, '']);

  if ASubject.StartsWith(LPrefix) then
    Result := ASubject.Substring(Length(LPrefix))
  else
    Result := ASubject;
end;

function TJetStreamKV.EntryFromStored(const AMsg: TJetStreamStoredMsg): TKVEntry;
var
  LParser: TNatsParser;
  LHeaders: TNatsHeaders;
  LOperation, LBlock: string;
begin
  Result := Default(TKVEntry);
  Result.Bucket := FBucket;
  Result.Key := SubjectKey(AMsg.Subject);
  Result.Revision := AMsg.Seq;
  Result.Created := AMsg.Time;
  Result.Operation := TKVOperation.Put;

  if not AMsg.Data.IsEmpty then
    Result.Value := TNetEncoding.Base64.DecodeStringToBytes(AMsg.Data);

  if AMsg.Hdrs.IsEmpty then
    Exit;

  { Hdrs is the raw header block, base64'd whole - so it goes back through the
    same parser that reads one off the wire rather than being picked apart here }
  LBlock := TEncoding.UTF8.GetString(
    TNetEncoding.Base64.DecodeStringToBytes(AMsg.Hdrs));

  LParser := TNatsParser.Create;
  try
    LHeaders := nil;
    LParser.ParseHeaders(LBlock, LHeaders);
  finally
    LParser.Free;
  end;

  LOperation := LHeaders.GetHeader(JetStreamConstants.KV.HEADER_OPERATION);

  { No KV-Operation header at all is the common case: an ordinary value }
  if LOperation = JetStreamConstants.KV.OP_DELETE then
    Result.Operation := TKVOperation.Delete
  else if LOperation = JetStreamConstants.KV.OP_PURGE then
    Result.Operation := TKVOperation.Purge;

  { A tombstone has no value, and reporting the empty payload as one would let
    a caller mistake a deleted key for a key set to '' }
  if Result.Operation <> TKVOperation.Put then
    Result.Value := nil;
end;

function TJetStreamKV.EntryFromDelivered(const AMsg: IJetStreamMsg): TKVEntry;
begin
  Result := DeliveredEntry(FBucket, AMsg);
end;

class function TJetStreamKV.DeliveredEntry(const ABucket: string; const AMsg: IJetStreamMsg): TKVEntry;
var
  LOperation, LPrefix: string;
  LHeaders: TNatsHeaders;
begin
  Result := Default(TKVEntry);
  Result.Bucket := ABucket;

  { As SubjectKey, which needs an instance - '$KV.<bucket>.' and everything
    after it, dots and all }
  LPrefix := Format(JetStreamConstants.KV.SUBJECT_KEY, [ABucket, '']);
  if AMsg.Subject.StartsWith(LPrefix) then
    Result.Key := AMsg.Subject.Substring(Length(LPrefix))
  else
    Result.Key := AMsg.Subject;

  Result.Revision := AMsg.Metadata.StreamSeq;
  Result.Operation := TKVOperation.Put;
  Result.Value := AMsg.PayloadData;

  LHeaders := AMsg.Headers;
  LOperation := LHeaders.GetHeader(JetStreamConstants.KV.HEADER_OPERATION);

  if LOperation = JetStreamConstants.KV.OP_DELETE then
    Result.Operation := TKVOperation.Delete
  else if LOperation = JetStreamConstants.KV.OP_PURGE then
    Result.Operation := TKVOperation.Purge;

  if Result.Operation <> TKVOperation.Put then
    Result.Value := nil;
end;

function TJetStreamKV.Get(const AKey: string; out AEntry: TKVEntry): Boolean;
var
  LMsg: TJetStreamStoredMsg;
begin
  CheckKey(AKey);
  AEntry := Default(TKVEntry);

  { The current value of a key IS the last message on its subject, so this is
    one request and no consumer }
  if not FContext.GetLastMsg(FStream, KeySubject(AKey), LMsg) then
    Exit(False);

  AEntry := EntryFromStored(LMsg);

  { A tombstone is the last message on the subject but the key is NOT set, and
    reporting it as a hit would hand back an empty value that reads like one
    somebody stored }
  Result := not AEntry.IsDelete;
  if not Result then
    AEntry := Default(TKVEntry);
end;

function TJetStreamKV.Get(const AKey: string; const ADefault: string): string;
var
  LEntry: TKVEntry;
begin
  if Get(AKey, LEntry) then
    Result := LEntry.ValueString
  else
    Result := ADefault;
end;

function TJetStreamKV.GetRevision(const AKey: string; ARevision: UInt64;
  out AEntry: TKVEntry): Boolean;
var
  LMsg: TJetStreamStoredMsg;
begin
  CheckKey(AKey);
  AEntry := Default(TKVEntry);

  if not FContext.GetMsg(FStream, ARevision, LMsg) then
    Exit(False);

  { A revision is a STREAM sequence, so it is unique across the whole bucket
    rather than per key - asking for one that belongs to a different key has to
    be a miss, not somebody else's value returned under this key's name }
  if LMsg.Subject <> KeySubject(AKey) then
    Exit(False);

  AEntry := EntryFromStored(LMsg);
  Result := True;
end;

function TJetStreamKV.PutRaw(const AKey: string; const AValue: TBytes;
  AOptions: TJetStreamPubOptions): UInt64;
begin
  CheckKey(AKey);
  Result := FContext.PublishBytes(KeySubject(AKey), AValue, AOptions).Seq;
end;

class function TJetStreamKV.IsExpectationError(AErrCode: Integer): Boolean;
begin
  { The publish-expectation family. KV publishes only with
    Nats-Expected-Last-Subject-Sequence, and nats-server versions differ in
    which code they return for a failed one: 10071 when the subject check is
    folded into the generic "wrong last sequence", 10072 in the classic
    numbering, 10164 on newer servers (as observed by the nats.py client).
    They all mean the same thing here - the CAS did not hold }
  Result := (AErrCode = JetStreamConstants.ErrCode.WRONG_LAST_SEQ) or
            (AErrCode = JetStreamConstants.ErrCode.WRONG_LAST_SUBJECT_SEQ) or
            (AErrCode = JetStreamConstants.ErrCode.WRONG_LAST_SUBJECT_SEQ_NEW);
end;

function TJetStreamKV.Put(const AKey: string; const AValue: TBytes): UInt64;
begin
  Result := PutRaw(AKey, AValue, TJetStreamPubOptions.New);
end;

function TJetStreamKV.Put(const AKey, AValue: string): UInt64;
begin
  Result := Put(AKey, TEncoding.UTF8.GetBytes(AValue));
end;

function TJetStreamKV.PutIfAbsent(const AKey: string; const AValue: TBytes): UInt64;
begin
  try
    { Zero here means "this subject has never been written to", which is
      exactly "the key does not exist" - and is why the publish options had to
      be able to express an expectation OF zero }
    Result := PutRaw(AKey, AValue, TJetStreamPubOptions.New.WithExpectedLastSubjectSeq(0));
  except
    on E: EJetStreamApiError do
    begin
      { ONLY "the expectation did not hold" means the key exists. Every other
        API error - a bucket that was never created (10059), a bad request, a
        quota - must propagate unchanged: rephrasing those as "key already
        exists" would report the exact opposite of what happened }
      if not IsExpectationError(E.ErrCode) then
        raise;
      raise EJetStreamKVError.CreateFmt(
        'Key [%s] already exists in bucket [%s]: %s', [AKey, FBucket, E.Error.Description]);
    end;
  end;
end;

function TJetStreamKV.PutIfAbsent(const AKey, AValue: string): UInt64;
begin
  Result := PutIfAbsent(AKey, TEncoding.UTF8.GetBytes(AValue));
end;

function TJetStreamKV.Update(const AKey: string; const AValue: TBytes;
  ARevision: UInt64): UInt64;
begin
  try
    Result := PutRaw(AKey, AValue,
      TJetStreamPubOptions.New.WithExpectedLastSubjectSeq(ARevision));
  except
    { The whole point of a compare-and-set is that losing the race is a normal
      outcome, so it gets an error the caller can recognise and retry on - but
      ONLY when the server actually said the expectation failed. Anything else
      (a missing bucket, a quota) is a real error and keeps its own type }
    on E: EJetStreamApiError do
    begin
      if not IsExpectationError(E.ErrCode) then
        raise;
      raise EJetStreamKVError.CreateFmt(
        'Key [%s] in bucket [%s] is no longer at revision %d: %s',
        [AKey, FBucket, ARevision, E.Error.Description]);
    end;
  end;
end;

function TJetStreamKV.Update(const AKey, AValue: string; ARevision: UInt64): UInt64;
begin
  Result := Update(AKey, TEncoding.UTF8.GetBytes(AValue), ARevision);
end;

procedure TJetStreamKV.Delete(const AKey: string);
begin
  { A delete is a MESSAGE, not the removal of one - the stream denies deletes.
    History still shows what the key used to hold }
  PutRaw(AKey, nil, TJetStreamPubOptions.New.WithHeader(
    JetStreamConstants.KV.HEADER_OPERATION, JetStreamConstants.KV.OP_DELETE));
end;

procedure TJetStreamKV.Purge(const AKey: string);
begin
  { Nats-Rollup: sub is what erases the history - it tells the server this
    message REPLACES every earlier one on the subject. The bucket was created
    with AllowRollupHdrs precisely so this works }
  PutRaw(AKey, nil, TJetStreamPubOptions.New
    .WithHeader(JetStreamConstants.KV.HEADER_OPERATION, JetStreamConstants.KV.OP_PURGE)
    .WithHeader(JetStreamConstants.Header.ROLLUP, JetStreamConstants.Header.ROLLUP_SUBJECT));
end;

function TJetStreamKV.Keys: TArray<string>;
var
  LResult: TList<string>;
begin
  LResult := TList<string>.Create;
  try
    { LastPerSubject gives the CURRENT state of each key, which is what a key
      list is. HeadersOnly because only the operation matters here - pulling
      every value across just to discard it would be the expensive way to ask }
    FContext.ScanSubject(FStream, Format(JetStreamConstants.KV.SUBJECT_ALL, [FBucket]),
      True, True,
      procedure (AMsg: IJetStreamMsg)
      var
        LEntry: TKVEntry;
      begin
        LEntry := EntryFromDelivered(AMsg);

        { A deleted key still has a subject and a last message - the tombstone -
          so filtering on the operation is the only thing that tells a live key
          from a dead one }
        if not LEntry.IsDelete then
          LResult.Add(LEntry.Key);
      end);

    Result := LResult.ToArray;
  finally
    LResult.Free;
  end;
end;

function TJetStreamKV.History(const AKey: string): TArray<TKVEntry>;
var
  LResult: TList<TKVEntry>;
begin
  CheckKey(AKey);

  LResult := TList<TKVEntry>.Create;
  try
    { Every revision in stream order, tombstones included - that IS a history }
    FContext.ScanSubject(FStream, KeySubject(AKey), False, False,
      procedure (AMsg: IJetStreamMsg)
      begin
        LResult.Add(EntryFromDelivered(AMsg));
      end);

    Result := LResult.ToArray;
  finally
    LResult.Free;
  end;
end;

class procedure TJetStreamKV.CheckKeyFilter(const AKey: string);
var
  LTokens: TArray<string>;
  LIndex: Integer;
  LToken: string;
begin
  if AKey.IsEmpty then
    raise EJetStreamKVError.Create('A key filter cannot be empty');

  LTokens := AKey.Split([NatsConstants.SEP]);

  for LIndex := 0 to High(LTokens) do
  begin
    LToken := LTokens[LIndex];

    if LToken = '*' then
      Continue;

    { '>' swallows everything after it, so anything following it is a
      filter the server would reject rather than a narrower one }
    if LToken = '>' then
    begin
      if LIndex <> High(LTokens) then
        raise EJetStreamKVError.CreateFmt(
          'Key filter [%s]: ''>'' may only be the last token', [AKey]);
      Continue;
    end;

    { Anything else is an ordinary key token - including the rule that a
      wildcard character inside a token ('a*') is not a wildcard at all }
    if LToken.IsEmpty then
      raise EJetStreamKVError.CreateFmt(
        'Key filter [%s] has an empty token - a doubled, leading or trailing dot',
        [AKey]);
    CheckKey(LToken);
  end;
end;

function TJetStreamKV.Watch(const AKey: string; AHandler: TKVWatchHandler; AOptions: TKVWatchOptions): IJetStreamKVWatcher;
var
  LState: IKVWatchState;
  LConnection: TNatsConnection;
  LInbox: string;
  LSid: Integer;
  LConfig: TJetStreamConsumerConfig;
  LInfo: TJetStreamConsumerInfo;
begin
  CheckKeyFilter(AKey);

  if not Assigned(AHandler) then
    raise EJetStreamKVError.Create('A watch needs a handler');

  if (TKVWatchOption.IncludeHistory in AOptions) and
     (TKVWatchOption.UpdatesOnly in AOptions) then
    raise EJetStreamKVError.Create(
      'IncludeHistory and UpdatesOnly contradict each other - one asks for ' +
      'every past revision, the other for none');

  LState := TKVWatchState.Create(FBucket, AHandler, AOptions);
  LConnection := FContext.Connection;
  LInbox := LConnection.GetNewInbox;

  { Subscribe FIRST. The server starts pushing the moment the consumer exists,
    often before the create reply is back, and core NATS keeps nothing for a
    subject nobody is subscribed to - the initial values would be lost }
  LSid := FContext.SubscribeDelivery(LInbox,
    procedure (const AMsg: IJetStreamMsg)
    begin
      LState.Deliver(AMsg);
    end);

  try
    LConfig := Default(TJetStreamConsumerConfig);
    LConfig.FilterSubject := KeySubject(AKey);
    LConfig.DeliverSubject := LInbox;
    LConfig.HeadersOnly := TKVWatchOption.MetaOnly in AOptions;

    if TKVWatchOption.UpdatesOnly in AOptions then
      LConfig.DeliverPolicy := TJetStreamDeliverPolicy.New
    else if TKVWatchOption.IncludeHistory in AOptions then
      LConfig.DeliverPolicy := TJetStreamDeliverPolicy.All
    else
      LConfig.DeliverPolicy := TJetStreamDeliverPolicy.LastPerSubject;

    { The shape every NATS client gives a watch: nothing to ack (a watch
      observes, it does not process), flow control so a slow handler is not
      cut off as a slow consumer, heartbeats which flow control requires, and
      memory storage with one replica because the consumer is throwaway }
    LConfig.AckPolicy := TJetStreamAckPolicy.None;
    LConfig.FlowControl := True;
    LConfig.IdleHeartbeat := TJetStreamDuration.FromSeconds(WATCH_HEARTBEAT_SECONDS);
    LConfig.MemStorage := True;
    LConfig.NumReplicas := 1;
    LConfig.InactiveThreshold := TJetStreamDuration.FromSeconds(WATCH_IDLE_SECONDS);

    LInfo := FContext.AddConsumer(FStream, LConfig);
  except
    LState.Halt;
    LConnection.Unsubscribe(LSid);
    raise;
  end;

  { Nothing to wait for when no initial values were asked for, or none
    matched. Delivered must be zero too: if the server already pushed
    something, the handler will see NumPending reach zero on its own, AFTER
    that entry is handled - marking it here could release a waiter early }
  if (TKVWatchOption.UpdatesOnly in AOptions) or
     ((LInfo.NumPending = 0) and (LInfo.Delivered.ConsumerSeq = 0)) then
    LState.MarkInitialDone;

  Result := TJetStreamKVWatcher.Create(FContext, FStream, LInfo.Name, LSid, LState);
end;

function TJetStreamKV.WatchAll(AHandler: TKVWatchHandler; AOptions: TKVWatchOptions): IJetStreamKVWatcher;
begin
  Result := Watch('>', AHandler, AOptions);
end;

function TJetStreamKV.Status: TJetStreamKVStatus;
var
  LInfo: TJetStreamStreamInfo;
begin
  LInfo := FContext.StreamInfo(FStream);

  Result := Default(TJetStreamKVStatus);
  Result.Bucket := FBucket;
  Result.StreamName := FStream;

  { "How many keys" is the stream's SUBJECT count, not its message count: a
    key with History revisions is one subject holding History messages, and a
    delete leaves a tombstone that keeps the subject alive. The KV bucket is
    created with AllowDirect, which is what makes the server track subjects }
  Result.Values := LInfo.State.NumSubjects;

  Result.History := LInfo.Config.MaxMsgsPerSubject;
  Result.TTL := LInfo.Config.MaxAge;
  Result.Bytes := LInfo.State.Bytes;
end;

end.
