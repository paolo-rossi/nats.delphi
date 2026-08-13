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

  Nats.Consts,
  Nats.Classes,
  Nats.Parser,
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
    /// How many keys currently hold a value, tombstones included
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

    function KeySubject(const AKey: string): string;
    /// The key a '$KV.<bucket>.<key>' subject names
    function SubjectKey(const ASubject: string): string;
    /// <summary>
    ///   Turns a stored message into an entry, reading the operation out of its
    ///   header block. No KV-Operation header at all means a plain value
    /// </summary>
    function EntryFromStored(const AMsg: TJetStreamStoredMsg): TKVEntry;
    function EntryFromDelivered(const AMsg: IJetStreamMsg): TKVEntry;
    /// Publishes to the key's subject and returns the revision it landed at
    function PutRaw(const AKey: string; const AValue: TBytes;
      AOptions: TJetStreamPubOptions): UInt64;
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

    { reading }

    /// <summary>
    ///   The current value. False means the key is not set - never set, or
    ///   deleted - which is an ordinary answer rather than an error
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
    ///   the key already exists
    /// </summary>
    function PutIfAbsent(const AKey: string; const AValue: TBytes): UInt64; overload;
    function PutIfAbsent(const AKey, AValue: string): UInt64; overload;
    /// <summary>
    ///   Compare-and-set: writes only if AKey is still at ARevision, and raises
    ///   EJetStreamKVError otherwise. The revision comes from a previous Get
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

    property Bucket: string read FBucket;
    /// KV_<bucket> - a bucket is a stream, and this is the stream
    property StreamName: string read FStream;
  end;

implementation

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
var
  LOperation: string;
  LHeaders: TNatsHeaders;
begin
  Result := Default(TKVEntry);
  Result.Bucket := FBucket;
  Result.Key := SubjectKey(AMsg.Subject);
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
      raise EJetStreamKVError.CreateFmt(
        'Key [%s] already exists in bucket [%s]: %s', [AKey, FBucket, E.Error.Description]);
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
      outcome, so it gets an error the caller can recognise and retry on }
    on E: EJetStreamApiError do
      raise EJetStreamKVError.CreateFmt(
        'Key [%s] in bucket [%s] is no longer at revision %d: %s',
        [AKey, FBucket, ARevision, E.Error.Description]);
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

function TJetStreamKV.Status: TJetStreamKVStatus;
var
  LInfo: TJetStreamStreamInfo;
begin
  LInfo := FContext.StreamInfo(FStream);

  Result := Default(TJetStreamKVStatus);
  Result.Bucket := FBucket;
  Result.StreamName := FStream;
  Result.Values := LInfo.State.Messages;
  Result.History := LInfo.Config.MaxMsgsPerSubject;
  Result.TTL := LInfo.Config.MaxAge;
  Result.Bytes := LInfo.State.Bytes;
end;

end.
