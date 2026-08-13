{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.JetStream.Client;

interface

uses
  System.SysUtils, System.Classes, System.SyncObjs, System.Generics.Collections,

  Nats.Consts,
  Nats.Classes,
  Nats.Connection,
  Nats.Exceptions,
  Nats.JetStream.Consts,
  Nats.JetStream.Entities,
  Nats.JetStream.Message;

type
  /// <summary>
  ///   The server answered, and the answer was an error rather than a result
  /// </summary>
  /// <remarks>
  ///   Raised rather than returned because a JetStream error carries no result
  ///   at all: there is nothing to hand back. Branch on ErrCode, which
  ///   identifies the specific failure - Code is only the broad HTTP-like class
  ///   (404 not found, 400 bad request, 503 unavailable).
  /// </remarks>
  EJetStreamApiError = class(ENatsException)
  private
    FError: TJetStreamApiError;
  public
    constructor Create(const AError: TJetStreamApiError); reintroduce;

    /// The whole error object, as the server sent it
    property Error: TJetStreamApiError read FError;
    function Code: Integer;
    function ErrCode: Integer;
    /// <summary>
    ///   True for the "it isn't there" family. Worth its own test because
    ///   "create it if missing" is the single most common reason to catch this
    /// </summary>
    function IsNotFound: Boolean;
  end;

  /// <summary>
  ///   The JetStream API did not answer at all, as opposed to answering with an
  ///   error. Usually means JetStream is not enabled on the server
  /// </summary>
  EJetStreamTimeout = class(ENatsException);

  /// <summary>
  ///   The server answered with a header status line instead of a response
  ///   body - "NATS/1.0 503 No Responders" and its relatives
  /// </summary>
  /// <remarks>
  ///   Distinct from EJetStreamApiError, which is a JSON error object with an
  ///   err_code to branch on. A status message carries no body at all, so there
  ///   is nothing to deserialize and nothing to report but the code itself.
  /// </remarks>
  EJetStreamStatusError = class(ENatsException)
  private
    FStatus: Integer;
    FDescription: string;
  public
    constructor Create(const ASubject: string; AStatus: Integer;
      const ADescription: string); reintroduce;

    /// The code from the status line. See NatsConstants.Status
    property Status: Integer read FStatus;
    /// The server's own wording. Informational - branch on Status
    property Description: string read FDescription;
  end;

  /// <summary>
  ///   The optional headers of a JetStream publish: the deduplication id and
  ///   the optimistic-concurrency expectations
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     Fluent rather than a record of plain fields because zero is a
  ///     MEANINGFUL value for every expectation here. "Nats-Expected-Last-
  ///     Sequence: 0" asserts the stream is still empty, which is precisely how
  ///     a caller publishes the first message of a sequence and no other; a
  ///     record of UInt64s could not tell that apart from "left alone".
  ///     Calling the method is what puts the header on the wire.
  ///   </para>
  ///   <para>
  ///     Value semantics, so a half-built options record can be kept and
  ///     branched from: each With... returns a copy, and the copy's own Add
  ///     uniquifies the header array before touching it.
  ///   </para>
  /// </remarks>
  TJetStreamPubOptions = record
  private
    FHeaders: TNatsHeaders;
    /// <summary>
    ///   A copy whose header array is genuinely its own. Every With... starts
    ///   here, and see the body for why the copy cannot be skipped
    /// </summary>
    function Fork: TJetStreamPubOptions;
    /// Idempotent: calling the same With... twice replaces, never duplicates
    function Put(const AName, AValue: string): TJetStreamPubOptions;
  public
    /// <summary>
    ///   The start of a chain: TJetStreamPubOptions.New.WithMsgId('...'). A
    ///   plain local variable works just as well - FHeaders is a managed field
    ///   and so is nil to begin with - but the chain has to start somewhere
    ///   readable
    /// </summary>
    class function New: TJetStreamPubOptions; static;

    /// <summary>
    ///   Deduplication id. Within the stream's duplicate window a second
    ///   message with this id is not stored, and the PubAck comes back with
    ///   Duplicate set and Seq pointing at the message already there
    /// </summary>
    function WithMsgId(const AValue: string): TJetStreamPubOptions;
    /// Rejects the publish unless the subject lands in exactly this stream
    function WithExpectedStream(const AValue: string): TJetStreamPubOptions;
    /// <summary>
    ///   Rejects the publish unless the stream's last sequence is this. Zero is
    ///   an assertion that the stream is empty, not an absent value
    /// </summary>
    function WithExpectedLastSeq(AValue: UInt64): TJetStreamPubOptions;
    /// As WithExpectedLastSeq, but counting only the subject published to
    function WithExpectedLastSubjectSeq(AValue: UInt64): TJetStreamPubOptions;
    /// Rejects the publish unless the last stored message carried this MsgId
    function WithExpectedLastMsgId(const AValue: string): TJetStreamPubOptions;
    /// <summary>
    ///   Anything else the application wants on the message. Appends rather
    ///   than replaces - NATS headers may legitimately repeat a name
    /// </summary>
    function WithHeader(const AName, AValue: string): TJetStreamPubOptions;

    /// The headers as built. Empty when nothing was set, so the publish is PUB
    function Headers: TNatsHeaders;
  end;

  /// <summary>
  ///   The JetStream management API, over an existing connection
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     Every method here BLOCKS. There is no sensible asynchronous form of
  ///     "create a stream", and management calls are made from setup code, not
  ///     from a message handler. Consumption (Phase 4) is handler-based
  ///     instead, matching the rest of the library.
  ///   </para>
  ///   <para>
  ///     **Never call these from a message, connect or disconnect handler.**
  ///     Those run on the connection's consumer thread, which is the thread
  ///     that has to deliver the API's reply, so the call would block until it
  ///     times out. This is RequestSync's constraint and it is inherited whole.
  ///   </para>
  ///   <para>
  ///     Does NOT own the connection - it is constructed over one and expects
  ///     it to outlive the context.
  ///   </para>
  /// </remarks>
  TJetStreamContext = class
  private
    FConnection: TNatsConnection;
    FDomain: string;
    FTimeout: Cardinal;

    /// $JS.API. or $JS.<domain>.API.
    function Prefix: string;
    function ApiSubject(const ATemplate: string; const AArgs: array of const): string;
    /// <summary>
    ///   The choke point: one place that builds the subject, sends the request,
    ///   and decides whether the answer is a result or an error
    /// </summary>
    function ApiRequestRaw(const ASubject, ARequestJson: string): string;
    /// <summary>
    ///   Everything that makes a reply unusable, in one place: a status line, an
    ///   empty body, an error object. Shared by the API and the publish paths
    ///   because the failure modes are the same for both
    /// </summary>
    function CheckReply(const ASubject: string; const AReply: TNatsArgsMSG): string;
    /// The choke point for publishing, as ApiRequestRaw is for the API
    function PublishRaw(const ASubject: string; const AData: TBytes;
      AHeaders: TNatsHeaders): TJetStreamPubAck;
    /// The choke point for pulling, and the only place a batch request is built
    function FetchRaw(const AStream, AConsumer: string;
      const ARequest: TJetStreamNextRequest;
      AWaitMs: Cardinal): TArray<IJetStreamMsg>;
    /// ATimeoutMs, or this context's own Timeout when it is 0
    function EffectiveTimeout(ATimeoutMs: Cardinal): Cardinal;
    /// <summary>
    ///   Raises if AName cannot go into an API subject unescaped. A name with a
    ///   dot in it would address a different endpoint, not fail
    /// </summary>
    procedure CheckName(const AKind, AName: string);
  public
    constructor Create(AConnection: TNatsConnection; const ADomain: string = '');

    { the choke point, exposed for calls this class does not wrap yet }

    function ApiRequest<TResp: record>(const ASubject: string): TResp; overload;
    function ApiRequest<TReq, TResp: record>(const ASubject: string;
      const ARequest: TReq): TResp; overload;

    { publishing }

    /// <summary>
    ///   Publishes and waits for the stream's acknowledgement. An ordinary core
    ///   NATS publish to an ordinary subject - what makes it a JetStream
    ///   publish is waiting for the PubAck the capturing stream sends back
    /// </summary>
    /// <remarks>
    ///   <para>
    ///     Blocks, like everything else here, and inherits RequestSync's rule:
    ///     never call it from a message, connect or disconnect handler.
    ///   </para>
    ///   <para>
    ///     Raises EJetStreamTimeout when nothing answers, and that is the normal
    ///     way to discover that NO STREAM captures the subject. Core NATS would
    ///     have accepted the same publish silently and dropped it - waiting for
    ///     the ack is the only thing that tells the two apart.
    ///   </para>
    /// </remarks>
    function Publish(const ASubject, AMessage: string): TJetStreamPubAck; overload;
    function Publish(const ASubject, AMessage: string;
      const AOptions: TJetStreamPubOptions): TJetStreamPubAck; overload;
    function PublishBytes(const ASubject: string; const AData: TBytes): TJetStreamPubAck; overload;
    function PublishBytes(const ASubject: string; const AData: TBytes;
      const AOptions: TJetStreamPubOptions): TJetStreamPubAck; overload;

    { consuming - pull }

    /// <summary>
    ///   Asks a PULL consumer for up to ABatch messages and blocks until they
    ///   arrive, the server closes the batch out, or ATimeoutMs elapses
    /// </summary>
    /// <remarks>
    ///   <para>
    ///     A short batch is a normal result, not a failure - an empty array
    ///     simply means the consumer had nothing pending. Nothing here raises
    ///     for an empty stream.
    ///   </para>
    ///   <para>
    ///     ATimeoutMs of 0 means this context's Timeout. The request carries
    ///     its own, slightly shorter expiry so the SERVER closes the batch and
    ///     stops holding it open; see FETCH_EXPIRY_MARGIN.
    ///   </para>
    ///   <para>
    ///     Blocks, so never call it from a message, connect or disconnect
    ///     handler - the thread it would block is the one delivering the batch.
    ///   </para>
    /// </remarks>
    function Fetch(const AStream, AConsumer: string; ABatch: Integer = 1;
      ATimeoutMs: Cardinal = 0): TArray<IJetStreamMsg>;
    /// <summary>
    ///   As Fetch, but returns with whatever is already waiting instead of
    ///   holding the request open. Ideal for draining, wrong for polling
    /// </summary>
    function FetchNoWait(const AStream, AConsumer: string;
      ABatch: Integer = 1): TArray<IJetStreamMsg>;
    /// <summary>
    ///   One message, the common case. False means none arrived in time, which
    ///   is an ordinary outcome rather than an error
    /// </summary>
    function Next(const AStream, AConsumer: string; out AMsg: IJetStreamMsg;
      ATimeoutMs: Cardinal = 0): Boolean;

    { consuming - push }

    /// <summary>
    ///   Subscribes to an existing PUSH consumer's delivery subject, wrapping
    ///   each delivery so the handler can ack it. Returns the core subscription
    ///   id, which is what Unsubscribe takes
    /// </summary>
    /// <remarks>
    ///   <para>
    ///     The handler runs on the connection's consumer thread, like every
    ///     other NATS handler - so it must not call anything that blocks on a
    ///     reply, which includes AckSync, Fetch and the whole management API.
    ///     Plain Ack / Nak / Term only write, and are fine.
    ///   </para>
    ///   <para>
    ///     Idle heartbeats and flow-control requests are answered and swallowed
    ///     here rather than being handed on: they are the server talking to the
    ///     client, not data. A flow-control request MUST be answered or the
    ///     server stops sending, which is why it is not simply ignored.
    ///   </para>
    /// </remarks>
    function SubscribePush(const AStream, AConsumer: string;
      AHandler: TJetStreamMsgHandler): Integer;

    { account }

    function AccountInfo: TJetStreamAccountInfo;

    { streams }

    function AddStream(const AConfig: TJetStreamStreamConfig): TJetStreamStreamInfo;
    /// <summary>
    ///   Replaces the WHOLE configuration - read StreamInfo, change what you
    ///   want and send it all back, or the fields you leave out revert
    /// </summary>
    function UpdateStream(const AConfig: TJetStreamStreamConfig): TJetStreamStreamInfo;
    function StreamInfo(const AStream: string): TJetStreamStreamInfo;
    function DeleteStream(const AStream: string): Boolean;
    /// Removes every message but keeps the stream. Returns how many went
    function PurgeStream(const AStream: string): UInt64;
    function ListStreams(AOffset: Integer = 0): TJetStreamStreamListResponse;
    function StreamNames(AOffset: Integer = 0): TArray<string>;

    { consumers }

    /// <summary>
    ///   Creates a durable consumer when the config names one, an ephemeral
    ///   consumer when it does not - the two use different API subjects
    /// </summary>
    function AddConsumer(const AStream: string;
      const AConfig: TJetStreamConsumerConfig): TJetStreamConsumerInfo;
    function ConsumerInfo(const AStream, AConsumer: string): TJetStreamConsumerInfo;
    function DeleteConsumer(const AStream, AConsumer: string): Boolean;
    function ListConsumers(const AStream: string; AOffset: Integer = 0): TJetStreamConsumerListResponse;
    function ConsumerNames(const AStream: string; AOffset: Integer = 0): TArray<string>;

    /// Empty unless this context was built for a JetStream domain
    property Domain: string read FDomain;
    /// How long to wait for an API reply. Defaults to the connection's own
    property Timeout: Cardinal read FTimeout write FTimeout;
  end;

implementation

const
  /// <summary>
  ///   How much sooner than the caller's own deadline the SERVER is told to
  ///   give up. The order matters: with the server expiring first, the batch is
  ///   closed out by a 408 and the request stops counting against the
  ///   consumer's MaxWaiting - a client that simply walks away leaves it
  ///   pending, and enough of those turn later fetches into 409s
  /// </summary>
  FETCH_EXPIRY_MARGIN = 100;
  /// Floor for the above, so even a very short fetch carries a usable expiry
  FETCH_MIN_EXPIRY = 50;

type
  /// <summary>
  ///   Collects one pull batch: the handoff between the consumer thread, which
  ///   receives the messages, and the caller blocked inside Fetch
  /// </summary>
  /// <remarks>
  ///   Reference counted for the same reason INatsRequestWaiter is - the
  ///   subscription handler holds one reference and the waiting caller another,
  ///   so a message arriving after the caller gave up still finds a live object
  ///   rather than a freed stack frame.
  /// </remarks>
  IJetStreamFetch = interface
    ['{7C4A8E12-3D96-4B0F-A5E7-C8103BD6F921}']
    /// Runs on the consumer thread, once per delivered message
    procedure Deliver(const AMsg: TNatsArgsMSG);
    /// <summary>
    ///   Blocks until the batch is full, the server closes it out, or the wait
    ///   elapses. Whatever was collected is returned in all three cases
    /// </summary>
    function WaitFor(ATimeoutMs: Cardinal): TArray<IJetStreamMsg>;
  end;

  TJetStreamFetch = class(TInterfacedObject, IJetStreamFetch)
  private
    FConnection: TNatsConnection;
    FEvent: TLightweightEvent;
    FLock: TCriticalSection;
    FMessages: TList<IJetStreamMsg>;
    FBatch: Integer;
  public
    constructor Create(AConnection: TNatsConnection; ABatch: Integer);
    destructor Destroy; override;

    procedure Deliver(const AMsg: TNatsArgsMSG);
    function WaitFor(ATimeoutMs: Cardinal): TArray<IJetStreamMsg>;
  end;

{ TJetStreamFetch }

constructor TJetStreamFetch.Create(AConnection: TNatsConnection; ABatch: Integer);
begin
  inherited Create;

  FConnection := AConnection;
  FBatch := ABatch;
  FEvent := TLightweightEvent.Create;
  FLock := TCriticalSection.Create;
  FMessages := TList<IJetStreamMsg>.Create;
end;

destructor TJetStreamFetch.Destroy;
begin
  FMessages.Free;
  FLock.Free;
  FEvent.Free;
  inherited;
end;

procedure TJetStreamFetch.Deliver(const AMsg: TNatsArgsMSG);
var
  LWrapped: IJetStreamMsg;
  LComplete: Boolean;
begin
  if AMsg.HasStatus then
  begin
    { An idle heartbeat says the server is alive and the batch is still open -
      it is the one status that must NOT end the wait, which is the whole
      reason it exists }
    if AMsg.Status = NatsConstants.Status.IDLE_HEARTBEAT then
      Exit;

    { Everything else closes the batch: 404 nothing there, 408 the request's
      own expiry, 409 the consumer went away or MaxWaiting was exceeded. None
      of them is an error - whatever arrived before it is still a valid result,
      so the wait ends and the caller gets a short batch }
    FEvent.SetEvent;
    Exit;
  end;

  { Anything that is not a status and not a JetStream delivery has no business
    on a pull inbox. Dropping it is safer than handing back something that
    cannot be acked }
  if not TJetStreamMsg.TryWrap(FConnection, AMsg, LWrapped) then
    Exit;

  FLock.Enter;
  try
    FMessages.Add(LWrapped);
    LComplete := FMessages.Count >= FBatch;
  finally
    FLock.Leave;
  end;

  if LComplete then
    FEvent.SetEvent;
end;

function TJetStreamFetch.WaitFor(ATimeoutMs: Cardinal): TArray<IJetStreamMsg>;
begin
  { The wait's own result is ignored on purpose, as in TNatsRequestWaiter: a
    message that lands in the same instant the timeout expires still counts, so
    what matters is what was collected, not which got there first }
  FEvent.WaitFor(ATimeoutMs);

  FLock.Enter;
  try
    Result := FMessages.ToArray;
  finally
    FLock.Leave;
  end;
end;

{ EJetStreamApiError }

constructor EJetStreamApiError.Create(const AError: TJetStreamApiError);
begin
  { The description is the server's own wording - repeating the codes in the
    message means a log line identifies the failure without any extra work }
  inherited CreateFmt('JetStream API error %d (code %d): %s',
    [AError.ErrCode, AError.Code, AError.Description]);
  FError := AError;
end;

function EJetStreamApiError.Code: Integer;
begin
  Result := FError.Code;
end;

function EJetStreamApiError.ErrCode: Integer;
begin
  Result := FError.ErrCode;
end;

function EJetStreamApiError.IsNotFound: Boolean;
begin
  Result := FError.Code = 404;
end;

{ EJetStreamStatusError }

constructor EJetStreamStatusError.Create(const ASubject: string; AStatus: Integer;
  const ADescription: string);
begin
  inherited CreateFmt('JetStream answered [%s] with status %d: %s',
    [ASubject, AStatus, ADescription]);
  FStatus := AStatus;
  FDescription := ADescription;
end;

{ TJetStreamPubOptions }

class function TJetStreamPubOptions.New: TJetStreamPubOptions;
begin
  Result := Default(TJetStreamPubOptions);
end;

function TJetStreamPubOptions.Fork: TJetStreamPubOptions;
begin
  Result := Self;
  { The explicit Copy is what makes this record a value, and it is NOT
    redundant: assigning the record copies a reference to one shared array.
    Appending would uniquify it - SetLength does that - but REPLACING a value
    writes into the array in place and never calls SetLength, so without this
    a branch would write straight through into the record it branched from }
  Result.FHeaders := Copy(Self.FHeaders);
end;

function TJetStreamPubOptions.Put(const AName, AValue: string): TJetStreamPubOptions;
begin
  Result := Fork;
  { SetHeader rather than Add: setting the same expectation twice must send one
    header, not two, and the server would read only one of them anyway }
  Result.FHeaders.SetHeader(AName, AValue);
end;

function TJetStreamPubOptions.WithMsgId(const AValue: string): TJetStreamPubOptions;
begin
  Result := Put(JetStreamConstants.Header.MSG_ID, AValue);
end;

function TJetStreamPubOptions.WithExpectedStream(const AValue: string): TJetStreamPubOptions;
begin
  Result := Put(JetStreamConstants.Header.EXPECTED_STREAM, AValue);
end;

function TJetStreamPubOptions.WithExpectedLastSeq(AValue: UInt64): TJetStreamPubOptions;
begin
  Result := Put(JetStreamConstants.Header.EXPECTED_LAST_SEQ, UIntToStr(AValue));
end;

function TJetStreamPubOptions.WithExpectedLastSubjectSeq(AValue: UInt64): TJetStreamPubOptions;
begin
  Result := Put(JetStreamConstants.Header.EXPECTED_LAST_SUBJECT_SEQ, UIntToStr(AValue));
end;

function TJetStreamPubOptions.WithExpectedLastMsgId(const AValue: string): TJetStreamPubOptions;
begin
  Result := Put(JetStreamConstants.Header.EXPECTED_LAST_MSG_ID, AValue);
end;

function TJetStreamPubOptions.WithHeader(const AName, AValue: string): TJetStreamPubOptions;
begin
  Result := Fork;
  Result.FHeaders.Add(AName, AValue);
end;

function TJetStreamPubOptions.Headers: TNatsHeaders;
begin
  Result := FHeaders;
end;

{ TJetStreamContext }

constructor TJetStreamContext.Create(AConnection: TNatsConnection; const ADomain: string);
begin
  inherited Create;

  if not Assigned(AConnection) then
    raise ENatsException.Create('A JetStream context needs a connection');

  FConnection := AConnection;
  FDomain := ADomain;
  FTimeout := NatsConstants.DEFAULT_REQUEST_TIMEOUT;
end;

function TJetStreamContext.Prefix: string;
begin
  if FDomain.IsEmpty then
    Result := JetStreamConstants.Api.PREFIX
  else
    Result := Format(JetStreamConstants.Api.PREFIX_DOMAIN, [FDomain]);
end;

function TJetStreamContext.ApiSubject(const ATemplate: string; const AArgs: array of const): string;
begin
  Result := Prefix + Format(ATemplate, AArgs);
end;

procedure TJetStreamContext.CheckName(const AKind, AName: string);
var
  LChar: Char;
begin
  if AName.IsEmpty then
    raise ENatsException.CreateFmt('The %s name cannot be empty', [AKind]);

  for LChar in JetStreamConstants.Naming.INVALID_CHARS do
    if AName.Contains(LChar) then
      raise ENatsException.CreateFmt(
        'The %s name [%s] cannot contain %s - it goes into the API subject as ' +
        'it stands, so it would address a different endpoint rather than fail',
        [AKind, AName, QuotedStr(LChar)]);
end;

function TJetStreamContext.CheckReply(const ASubject: string; const AReply: TNatsArgsMSG): string;
var
  LResponse: TJetStreamApiResponse;
begin
  { A status message is control flow, not data, and its body is always empty.
    Letting one through would deserialize into a record of zeroes that reads
    exactly like a successful call whose every field happened to be a default }
  if AReply.HasStatus then
    raise EJetStreamStatusError.Create(ASubject, AReply.Status, AReply.Description);

  Result := AReply.Payload;
  if Result.Trim.IsEmpty then
    raise ENatsException.CreateFmt(
      'Empty reply from [%s] - a JSON body was expected', [ASubject]);

  { The error object is checked BEFORE the caller deserializes the real type,
    and this is the only place it happens. An error response carries no result,
    so deserializing it as the expected record yields an empty one - which is
    indistinguishable from a successful call that returned all defaults }
  LResponse := TJetStreamJSON.FromJSON<TJetStreamApiResponse>(Result);
  if LResponse.Error.HasError then
    raise EJetStreamApiError.Create(LResponse.Error);
end;

function TJetStreamContext.ApiRequestRaw(const ASubject, ARequestJson: string): string;
var
  LReply: TNatsArgsMSG;
begin
  if not FConnection.RequestSync(ASubject, ARequestJson, LReply, FTimeout) then
    raise EJetStreamTimeout.CreateFmt(
      'No reply from the JetStream API on [%s] within %d ms. The usual cause ' +
      'is JetStream not being enabled on this server', [ASubject, FTimeout]);

  Result := CheckReply(ASubject, LReply);
end;

function TJetStreamContext.ApiRequest<TResp>(const ASubject: string): TResp;
begin
  { No request body at all. Several API calls take none, and sending an empty
    JSON object where the server expects nothing is needless }
  Result := TJetStreamJSON.FromJSON<TResp>(ApiRequestRaw(ASubject, ''));
end;

function TJetStreamContext.ApiRequest<TReq, TResp>(const ASubject: string;
  const ARequest: TReq): TResp;
begin
  Result := TJetStreamJSON.FromJSON<TResp>(
    ApiRequestRaw(ASubject, TJetStreamJSON.ToJSON<TReq>(ARequest)));
end;

{ publishing }

function TJetStreamContext.PublishRaw(const ASubject: string; const AData: TBytes;
  AHeaders: TNatsHeaders): TJetStreamPubAck;
var
  LReply: TNatsArgsMSG;
begin
  { No CheckName here on purpose: this is a real subject, not a name going into
    an API subject, so wildcards and dots are the caller's business.
    RequestSync -> PublishBytes already runs it through CheckSubject, and
    through the max_payload check - on the HPUB total when there are headers }
  if not FConnection.RequestSync(ASubject, AData, AHeaders, LReply, FTimeout) then
    raise EJetStreamTimeout.CreateFmt(
      'No PubAck for [%s] within %d ms. Silence usually means no stream ' +
      'captures that subject: core NATS accepted the publish and dropped it',
      [ASubject, FTimeout]);

  Result := TJetStreamJSON.FromJSON<TJetStreamPubAck>(CheckReply(ASubject, LReply));
end;

function TJetStreamContext.Publish(const ASubject, AMessage: string): TJetStreamPubAck;
begin
  Result := PublishRaw(ASubject, TEncoding.UTF8.GetBytes(AMessage), nil);
end;

function TJetStreamContext.Publish(const ASubject, AMessage: string;
  const AOptions: TJetStreamPubOptions): TJetStreamPubAck;
begin
  Result := PublishRaw(ASubject, TEncoding.UTF8.GetBytes(AMessage), AOptions.Headers);
end;

function TJetStreamContext.PublishBytes(const ASubject: string; const AData: TBytes): TJetStreamPubAck;
begin
  Result := PublishRaw(ASubject, AData, nil);
end;

function TJetStreamContext.PublishBytes(const ASubject: string; const AData: TBytes;
  const AOptions: TJetStreamPubOptions): TJetStreamPubAck;
begin
  Result := PublishRaw(ASubject, AData, AOptions.Headers);
end;

{ consuming - pull }

function TJetStreamContext.EffectiveTimeout(ATimeoutMs: Cardinal): Cardinal;
begin
  if ATimeoutMs = 0 then
    Result := FTimeout
  else
    Result := ATimeoutMs;
end;

function TJetStreamContext.FetchRaw(const AStream, AConsumer: string;
  const ARequest: TJetStreamNextRequest; AWaitMs: Cardinal): TArray<IJetStreamMsg>;
var
  LFetch: IJetStreamFetch;
  LInbox, LSubject: string;
  LId: Integer;
begin
  CheckName('stream', AStream);
  CheckName('consumer', AConsumer);

  if ARequest.Batch < 1 then
    raise ENatsException.CreateFmt('A batch of %d asks for nothing', [ARequest.Batch]);

  LSubject := ApiSubject(JetStreamConstants.Api.CONSUMER_MSG_NEXT, [AStream, AConsumer]);
  LInbox := FConnection.GetNewInbox;
  LFetch := TJetStreamFetch.Create(FConnection, ARequest.Batch);

  { A batch is emphatically NOT a RequestSync: the server answers with up to
    Batch separate messages and then a status to close them out, where
    RequestSync takes the first reply and stops }
  LId := FConnection.Subscribe(LInbox,
    procedure (const AMsg: TNatsArgsMSG)
    begin
      LFetch.Deliver(AMsg);
    end);
  try
    FConnection.Publish(LSubject, TJetStreamJSON.ToJSON<TJetStreamNextRequest>(ARequest), LInbox);
    Result := LFetch.WaitFor(AWaitMs);
  finally
    { On every exit path, exactly as RequestSync does. Without it each fetch
      leaves one inbox subscribed here and on the server }
    FConnection.Unsubscribe(LId, 0);
  end;
end;

function TJetStreamContext.Fetch(const AStream, AConsumer: string; ABatch: Integer;
  ATimeoutMs: Cardinal): TArray<IJetStreamMsg>;
var
  LRequest: TJetStreamNextRequest;
  LWait, LExpiry: Cardinal;
begin
  LWait := EffectiveTimeout(ATimeoutMs);

  { The server is given the SHORTER deadline of the two, so an empty consumer
    ends the batch with a 408 rather than leaving us to time out blind }
  if LWait > FETCH_EXPIRY_MARGIN + FETCH_MIN_EXPIRY then
    LExpiry := LWait - FETCH_EXPIRY_MARGIN
  else
    LExpiry := FETCH_MIN_EXPIRY;

  LRequest := Default(TJetStreamNextRequest);
  LRequest.Batch := ABatch;
  LRequest.Expires := TJetStreamDuration.FromMillis(LExpiry);

  Result := FetchRaw(AStream, AConsumer, LRequest, LWait);
end;

function TJetStreamContext.FetchNoWait(const AStream, AConsumer: string;
  ABatch: Integer): TArray<IJetStreamMsg>;
var
  LRequest: TJetStreamNextRequest;
begin
  LRequest := Default(TJetStreamNextRequest);
  LRequest.Batch := ABatch;
  LRequest.NoWait := True;

  { No expiry: the server is not holding anything open, so there is nothing to
    expire. The wait is still the full timeout because the messages themselves
    have to travel back }
  Result := FetchRaw(AStream, AConsumer, LRequest, FTimeout);
end;

function TJetStreamContext.Next(const AStream, AConsumer: string;
  out AMsg: IJetStreamMsg; ATimeoutMs: Cardinal): Boolean;
var
  LBatch: TArray<IJetStreamMsg>;
begin
  AMsg := nil;

  LBatch := Fetch(AStream, AConsumer, 1, ATimeoutMs);

  Result := Length(LBatch) > 0;
  if Result then
    AMsg := LBatch[0];
end;

{ consuming - push }

function TJetStreamContext.SubscribePush(const AStream, AConsumer: string;
  AHandler: TJetStreamMsgHandler): Integer;
var
  LInfo: TJetStreamConsumerInfo;
  LConnection: TNatsConnection;
begin
  if not Assigned(AHandler) then
    raise ENatsException.Create('A push subscription needs a handler');

  { ConsumerInfo, not a guess: the delivery subject is the server's, and it is
    also the only way to tell a push consumer from a pull one }
  LInfo := ConsumerInfo(AStream, AConsumer);

  if LInfo.Config.DeliverSubject.IsEmpty then
    raise ENatsException.CreateFmt(
      'Consumer [%s] on stream [%s] is a PULL consumer - it has no delivery ' +
      'subject to subscribe to. Use Fetch or Next instead', [AConsumer, AStream]);

  LConnection := FConnection;

  Result := FConnection.Subscribe(LInfo.Config.DeliverSubject,
    procedure (const AMsg: TNatsArgsMSG)
    var
      LWrapped: IJetStreamMsg;
    begin
      if AMsg.HasStatus then
      begin
        { A flow-control request carries a reply-to and MUST be answered, or
          the server stops sending on this subject. An idle heartbeat has none
          and only needs swallowing. Either way it is control, not data, and
          handing it to the application would give it a phantom empty message
          it could not ack }
        if not AMsg.ReplyTo.IsEmpty then
          LConnection.Publish(AMsg.ReplyTo, String.Empty);
        Exit;
      end;

      if TJetStreamMsg.TryWrap(LConnection, AMsg, LWrapped) then
        AHandler(LWrapped);
    end);
end;

{ account }

function TJetStreamContext.AccountInfo: TJetStreamAccountInfo;
begin
  Result := ApiRequest<TJetStreamAccountInfo>(ApiSubject(JetStreamConstants.Api.INFO, []));
end;

{ streams }

function TJetStreamContext.AddStream(const AConfig: TJetStreamStreamConfig): TJetStreamStreamInfo;
begin
  CheckName('stream', AConfig.Name);

  Result := ApiRequest<TJetStreamStreamConfig, TJetStreamStreamInfo>(
    ApiSubject(JetStreamConstants.Api.STREAM_CREATE, [AConfig.Name]), AConfig);
end;

function TJetStreamContext.UpdateStream(const AConfig: TJetStreamStreamConfig): TJetStreamStreamInfo;
begin
  CheckName('stream', AConfig.Name);

  Result := ApiRequest<TJetStreamStreamConfig, TJetStreamStreamInfo>(
    ApiSubject(JetStreamConstants.Api.STREAM_UPDATE, [AConfig.Name]), AConfig);
end;

function TJetStreamContext.StreamInfo(const AStream: string): TJetStreamStreamInfo;
begin
  CheckName('stream', AStream);

  Result := ApiRequest<TJetStreamStreamInfo>(
    ApiSubject(JetStreamConstants.Api.STREAM_INFO, [AStream]));
end;

function TJetStreamContext.DeleteStream(const AStream: string): Boolean;
begin
  CheckName('stream', AStream);

  Result := ApiRequest<TJetStreamSuccessResponse>(
    ApiSubject(JetStreamConstants.Api.STREAM_DELETE, [AStream])).Success;
end;

function TJetStreamContext.PurgeStream(const AStream: string): UInt64;
begin
  CheckName('stream', AStream);

  Result := ApiRequest<TJetStreamSuccessResponse>(
    ApiSubject(JetStreamConstants.Api.STREAM_PURGE, [AStream])).Purged;
end;

function TJetStreamContext.ListStreams(AOffset: Integer): TJetStreamStreamListResponse;
var
  LRequest: TJetStreamListRequest;
begin
  LRequest.Offset := AOffset;

  Result := ApiRequest<TJetStreamListRequest, TJetStreamStreamListResponse>(
    ApiSubject(JetStreamConstants.Api.STREAM_LIST, []), LRequest);
end;

function TJetStreamContext.StreamNames(AOffset: Integer): TArray<string>;
var
  LRequest: TJetStreamListRequest;
begin
  LRequest.Offset := AOffset;

  Result := ApiRequest<TJetStreamListRequest, TJetStreamNamesResponse>(
    ApiSubject(JetStreamConstants.Api.STREAM_NAMES, []), LRequest).Streams;
end;

{ consumers }

function TJetStreamContext.AddConsumer(const AStream: string;
  const AConfig: TJetStreamConsumerConfig): TJetStreamConsumerInfo;
var
  LRequest: TJetStreamConsumerCreateRequest;
  LName, LSubject: string;
begin
  CheckName('stream', AStream);

  { DurableName is the durable one; Name covers the 2.9+ named-ephemeral case.
    With neither, the server names the consumer itself and the subject carries
    no name at all - a different endpoint, not the same one with a blank token }
  LName := AConfig.DurableName;
  if LName.IsEmpty then
    LName := AConfig.Name;

  if LName.IsEmpty then
    LSubject := ApiSubject(JetStreamConstants.Api.CONSUMER_CREATE, [AStream])
  else
  begin
    CheckName('consumer', LName);
    LSubject := ApiSubject(JetStreamConstants.Api.CONSUMER_CREATE_NAMED, [AStream, LName]);
  end;

  LRequest.StreamName := AStream;
  LRequest.Config := AConfig;

  Result := ApiRequest<TJetStreamConsumerCreateRequest, TJetStreamConsumerInfo>(
    LSubject, LRequest);
end;

function TJetStreamContext.ConsumerInfo(const AStream, AConsumer: string): TJetStreamConsumerInfo;
begin
  CheckName('stream', AStream);
  CheckName('consumer', AConsumer);

  Result := ApiRequest<TJetStreamConsumerInfo>(
    ApiSubject(JetStreamConstants.Api.CONSUMER_INFO, [AStream, AConsumer]));
end;

function TJetStreamContext.DeleteConsumer(const AStream, AConsumer: string): Boolean;
begin
  CheckName('stream', AStream);
  CheckName('consumer', AConsumer);

  Result := ApiRequest<TJetStreamSuccessResponse>(
    ApiSubject(JetStreamConstants.Api.CONSUMER_DELETE, [AStream, AConsumer])).Success;
end;

function TJetStreamContext.ListConsumers(const AStream: string;
  AOffset: Integer): TJetStreamConsumerListResponse;
var
  LRequest: TJetStreamListRequest;
begin
  CheckName('stream', AStream);
  LRequest.Offset := AOffset;

  Result := ApiRequest<TJetStreamListRequest, TJetStreamConsumerListResponse>(
    ApiSubject(JetStreamConstants.Api.CONSUMER_LIST, [AStream]), LRequest);
end;

function TJetStreamContext.ConsumerNames(const AStream: string;
  AOffset: Integer): TArray<string>;
var
  LRequest: TJetStreamListRequest;
begin
  CheckName('stream', AStream);
  LRequest.Offset := AOffset;

  Result := ApiRequest<TJetStreamListRequest, TJetStreamNamesResponse>(
    ApiSubject(JetStreamConstants.Api.CONSUMER_NAMES, [AStream]), LRequest).Consumers;
end;

end.
