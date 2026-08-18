{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.JetStream.Entities;

interface

{$SCOPEDENUMS ON}

uses
  System.SysUtils, System.Rtti, System.JSON, System.TypInfo,

  Neon.Core.Types,
  Neon.Core.Attributes,
  Neon.Core.Nullables,
  Neon.Core.Persistence,
  Neon.Core.Persistence.JSON;

type
  /// <summary>
  ///   A duration in NANOSECONDS, which is the only unit JetStream speaks.
  /// </summary>
  /// <remarks>
  ///   A distinct type on purpose. Passing milliseconds where the server wants
  ///   nanoseconds fails silently by being a factor of a million wrong: the
  ///   stream is created, it just behaves nothing like the caller asked. Build
  ///   these with FromSeconds / FromMillis rather than writing the zeroes out.
  /// </remarks>
  TJetStreamDuration = type Int64;

  TJetStreamDurationHelper = record helper for TJetStreamDuration
  public const
    NANOS_PER_MILLI  = Int64(1000000);
    NANOS_PER_SECOND = Int64(1000000000);
    NANOS_PER_MINUTE = Int64(60) * NANOS_PER_SECOND;
  public
    class function FromMillis(AValue: Int64): TJetStreamDuration; static;
    class function FromSeconds(AValue: Int64): TJetStreamDuration; static;
    class function FromMinutes(AValue: Int64): TJetStreamDuration; static;

    function ToMillis: Int64;
    function ToSeconds: Double;
  end;

  /// <summary>
  ///   What a stream does when it hits a limit. "limits" keeps messages until a
  ///   limit forces them out, "interest" drops them once every consumer has
  ///   acked, "workqueue" drops them once ONE consumer has
  /// </summary>
  [NeonEnumNames('limits,interest,workqueue')]
  TJetStreamRetention = (Limits, Interest, WorkQueue);

  [NeonEnumNames('file,memory')]
  TJetStreamStorage = (Filestore, Memory);

  /// <summary>
  ///   Which end gives way when the stream is full: drop the oldest message, or
  ///   refuse the new one
  /// </summary>
  [NeonEnumNames('old,new')]
  TJetStreamDiscard = (Old, New);

  /// <summary>
  ///   Where a new consumer starts reading. The Opt* fields of the consumer
  ///   config are only consulted for ByStartSequence and ByStartTime
  /// </summary>
  [NeonEnumNames('all,last,new,by_start_sequence,by_start_time,last_per_subject')]
  TJetStreamDeliverPolicy = (All, Last, New, ByStartSequence, ByStartTime, LastPerSubject);

  /// <summary>
  ///   How much acknowledgement the consumer requires. Explicit - every message
  ///   acked individually - is the only one that makes sense for a work queue
  /// </summary>
  [NeonEnumNames('none,all,explicit')]
  TJetStreamAckPolicy = (None, All, Explicit);

  [NeonEnumNames('instant,original')]
  TJetStreamReplayPolicy = (Instant, Original);

  /// <summary>
  ///   The error object a JetStream API response carries INSTEAD of a result.
  /// </summary>
  /// <remarks>
  ///   Code is the HTTP-like class (404, 400, 503); ErrCode is the specific
  ///   JetStream error and is the one worth branching on - 10059 is "stream not
  ///   found", 10058 "stream name already in use", and so on.
  /// </remarks>
  TJetStreamApiError = record
    Code: Integer;
    ErrCode: Integer;
    Description: string;

    /// <summary>
    ///   True when the server actually reported an error. An API response's
    ///   error object is absent on success, and an absent object deserializes
    ///   to an empty record rather than to nothing
    /// </summary>
    function HasError: Boolean;
  end;

  /// <summary>
  ///   The envelope every JetStream API response shares
  /// </summary>
  /// <remarks>
  ///   Deserialize into this FIRST and check Error before deserializing the
  ///   real response type. A server-side error carries no result at all, so
  ///   deserializing it as the expected type yields an empty record - which
  ///   reads exactly like a successful call that returned defaults.
  /// </remarks>
  TJetStreamApiResponse = record
    /// <summary>
    ///   e.g. 'io.nats.jetstream.api.v1.stream_info_response'. Named around
    ///   the Delphi reserved word, hence the explicit wire name
    /// </summary>
    [NeonProperty('type')]
    ResponseType: string;
    Error: TJetStreamApiError;
  end;

  /// <summary>
  ///   The server's answer to a JetStream publish: the message is only stored
  ///   once this comes back
  /// </summary>
  TJetStreamPubAck = record
    Stream: string;
    Seq: UInt64;
    /// <summary>
    ///   True when the message was recognised as a duplicate of one already in
    ///   the stream (by its Nats-Msg-Id) and therefore NOT stored again. Seq
    ///   then points at the original
    /// </summary>
    Duplicate: Boolean;
    Domain: string;
  end;

  /// <summary>
  ///   A stream's configuration - the record sent to create or update one, and
  ///   the one that comes back inside StreamInfo
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     Numbers, strings and durations are [NeonInclude(NotDefault)], so
  ///     anything left alone is omitted and the server applies its own default.
  ///     That keeps a read-modify-write round trip from rewriting fields the
  ///     caller never touched.
  ///   </para>
  ///   <para>
  ///     Booleans and enums are NOT omitted - Neon's WriteBoolean and WriteEnum
  ///     ignore IncludeIf - and that is safe only because the Delphi zero value
  ///     of each one is exactly the default nats-server applies when the field
  ///     is absent: retention "limits", storage "file", discard "old", every
  ///     flag false. A field that breaks that correspondence cannot be a plain
  ///     Boolean or enum; it needs Nullable&lt;T&gt;, whose serializer is
  ///     registered in JetStreamJSONConfig. Pinned by
  ///     StreamConfig_BooleansAndEnums_AreAlwaysEmitted.
  ///   </para>
  /// </remarks>
  TJetStreamStreamConfig = record
    [NeonInclude(IncludeIf.NotDefault)]
    Name: string;
    [NeonInclude(IncludeIf.NotEmpty)]
    Subjects: TArray<string>;
    [NeonInclude(IncludeIf.NotDefault)]
    Description: string;
    [NeonInclude(IncludeIf.NotDefault)]
    Retention: TJetStreamRetention;
    [NeonInclude(IncludeIf.NotDefault)]
    Storage: TJetStreamStorage;
    [NeonInclude(IncludeIf.NotDefault)]
    Discard: TJetStreamDiscard;
    /// -1 for unlimited, which is also what the server uses when this is absent
    [NeonInclude(IncludeIf.NotDefault)]
    MaxConsumers: Integer;
    [NeonInclude(IncludeIf.NotDefault)]
    MaxMsgs: Int64;
    [NeonInclude(IncludeIf.NotDefault)]
    MaxBytes: Int64;
    [NeonInclude(IncludeIf.NotDefault)]
    MaxMsgsPerSubject: Int64;
    [NeonInclude(IncludeIf.NotDefault)]
    MaxMsgSize: Integer;
    /// Nanoseconds - use TJetStreamDuration.FromSeconds and friends
    [NeonInclude(IncludeIf.NotDefault)]
    MaxAge: TJetStreamDuration;
    /// <summary>
    ///   How far back the server looks for a repeated Nats-Msg-Id. Nanoseconds;
    ///   the server's own default is two minutes
    /// </summary>
    [NeonInclude(IncludeIf.NotDefault)]
    DuplicateWindow: TJetStreamDuration;
    /// Server default is 1 when omitted, not 0
    [NeonInclude(IncludeIf.NotDefault)]
    NumReplicas: Integer;
    [NeonInclude(IncludeIf.NotDefault)]
    NoAck: Boolean;
    [NeonInclude(IncludeIf.NotDefault)]
    DenyDelete: Boolean;
    [NeonInclude(IncludeIf.NotDefault)]
    DenyPurge: Boolean;
    [NeonInclude(IncludeIf.NotDefault)]
    AllowRollupHdrs: Boolean;
    [NeonInclude(IncludeIf.NotDefault)]
    AllowDirect: Boolean;
    [NeonInclude(IncludeIf.NotDefault)]
    MirrorDirect: Boolean;
    /// Set by the server, never by a client: a sealed stream cannot be changed
    [NeonInclude(IncludeIf.NotDefault)]
    Sealed: Boolean;
  end;

  /// <summary>
  ///   What is actually in a stream right now. Read-only: the server fills it
  ///   in and a client never sends it
  /// </summary>
  TJetStreamStreamState = record
    Messages: UInt64;
    Bytes: UInt64;
    FirstSeq: UInt64;
    LastSeq: UInt64;
    ConsumerCount: Integer;
    NumSubjects: Integer;
    NumDeleted: Integer;
    /// <summary>
    ///   RFC3339, kept as text on purpose: an empty stream reports
    ///   '0001-01-01T00:00:00Z', which is outside the range TDateTime handles
    ///   comfortably and would either raise or land on the wrong date
    /// </summary>
    FirstTs: string;
    LastTs: string;
  end;

  TJetStreamStreamInfo = record
    Config: TJetStreamStreamConfig;
    State: TJetStreamStreamState;
    Created: string;
  end;

  /// <summary>
  ///   A consumer's configuration. Same omit-when-unset rule as
  ///   TJetStreamStreamConfig, and for the same reason
  /// </summary>
  TJetStreamConsumerConfig = record
    /// <summary>
    ///   Set this to make the consumer durable - the server then remembers its
    ///   position across restarts. Leave it empty for an ephemeral one, which
    ///   the server discards once nobody is bound to it
    /// </summary>
    [NeonInclude(IncludeIf.NotDefault)]
    DurableName: string;
    [NeonInclude(IncludeIf.NotDefault)]
    Name: string;
    [NeonInclude(IncludeIf.NotDefault)]
    Description: string;
    [NeonInclude(IncludeIf.NotDefault)]
    DeliverPolicy: TJetStreamDeliverPolicy;
    [NeonInclude(IncludeIf.NotDefault)]
    OptStartSeq: UInt64;
    [NeonInclude(IncludeIf.NotDefault)]
    OptStartTime: string;
    /// <remarks>
    ///   Nullable on purpose. The enum's zero value is None, and sending
    ///   "ack_policy":"none" is NOT what nats-server does when the field is
    ///   absent: its default is explicit, and a PULL consumer with ack policy
    ///   "none" is rejected outright. Neon emits enums unconditionally, so a
    ///   plain field would send "none" for every consumer whose policy the
    ///   caller never set. A nullable field is omitted instead, and the
    ///   server's own default (explicit) applies. Serialized by
    ///   TNullableEnumSerializer, registered in JetStreamJSONConfig.
    /// </remarks>
    [NeonInclude(IncludeIf.NotDefault)]
    AckPolicy: Nullable<TJetStreamAckPolicy>;
    /// <summary>
    ///   How long the server waits for an ack before redelivering.
    ///   Nanoseconds; the server's own default is 30 seconds
    /// </summary>
    [NeonInclude(IncludeIf.NotDefault)]
    AckWait: TJetStreamDuration;
    /// -1 for unlimited
    [NeonInclude(IncludeIf.NotDefault)]
    MaxDeliver: Integer;
    [NeonInclude(IncludeIf.NotDefault)]
    FilterSubject: string;
    [NeonInclude(IncludeIf.NotEmpty)]
    FilterSubjects: TArray<string>;
    [NeonInclude(IncludeIf.NotDefault)]
    ReplayPolicy: TJetStreamReplayPolicy;
    [NeonInclude(IncludeIf.NotDefault)]
    RateLimitBps: UInt64;
    [NeonInclude(IncludeIf.NotDefault)]
    SampleFreq: string;
    /// Pull consumers only: how many unfulfilled batch requests may queue up
    [NeonInclude(IncludeIf.NotDefault)]
    MaxWaiting: Integer;
    [NeonInclude(IncludeIf.NotDefault)]
    MaxAckPending: Integer;
    [NeonInclude(IncludeIf.NotDefault)]
    MaxBatch: Integer;
    [NeonInclude(IncludeIf.NotDefault)]
    MaxExpires: TJetStreamDuration;
    [NeonInclude(IncludeIf.NotDefault)]
    InactiveThreshold: TJetStreamDuration;
    [NeonInclude(IncludeIf.NotDefault)]
    NumReplicas: Integer;
    [NeonInclude(IncludeIf.NotDefault)]
    MemStorage: Boolean;
    [NeonInclude(IncludeIf.NotDefault)]
    HeadersOnly: Boolean;
    /// <summary>
    ///   Setting this makes the consumer a PUSH consumer: the server streams to
    ///   this subject instead of waiting to be asked. Leave it empty for pull
    /// </summary>
    [NeonInclude(IncludeIf.NotDefault)]
    DeliverSubject: string;
    [NeonInclude(IncludeIf.NotDefault)]
    DeliverGroup: string;
    /// Push consumers only
    [NeonInclude(IncludeIf.NotDefault)]
    FlowControl: Boolean;
    [NeonInclude(IncludeIf.NotDefault)]
    IdleHeartbeat: TJetStreamDuration;
  end;

  /// A position held in both sequences at once
  TJetStreamSequenceInfo = record
    ConsumerSeq: UInt64;
    StreamSeq: UInt64;
  end;

  TJetStreamConsumerInfo = record
    StreamName: string;
    Name: string;
    Created: string;
    Config: TJetStreamConsumerConfig;
    /// The last message handed out
    Delivered: TJetStreamSequenceInfo;
    /// Everything below this point has been acknowledged
    AckFloor: TJetStreamSequenceInfo;
    NumAckPending: Integer;
    NumRedelivered: Integer;
    NumWaiting: Integer;
    NumPending: UInt64;
    PushBound: Boolean;
  end;

  /// <summary>
  ///   The paging envelope every JetStream list response shares. Total is the
  ///   number of items that exist, not the number in this page, so a caller
  ///   pages until Offset + Length(items) reaches it
  /// </summary>
  TJetStreamStreamListResponse = record
    Total: Integer;
    Offset: Integer;
    Limit: Integer;
    Streams: TArray<TJetStreamStreamInfo>;
  end;

  TJetStreamConsumerListResponse = record
    Total: Integer;
    Offset: Integer;
    Limit: Integer;
    Consumers: TArray<TJetStreamConsumerInfo>;
  end;

  /// <summary>
  ///   The *.NAMES responses, which return bare strings instead of full
  ///   objects. One record for both: only one of the two arrays is ever present
  ///   in a given response, and an absent one simply stays empty
  /// </summary>
  TJetStreamNamesResponse = record
    Total: Integer;
    Offset: Integer;
    Limit: Integer;
    Streams: TArray<string>;
    Consumers: TArray<string>;
  end;

  /// <summary>
  ///   The body of a CONSUMER.CREATE request. The config alone is not enough -
  ///   the stream has to be named in the body as well as in the subject
  /// </summary>
  TJetStreamConsumerCreateRequest = record
    StreamName: string;
    Config: TJetStreamConsumerConfig;
  end;

  /// The body of the paged LIST and NAMES requests
  TJetStreamListRequest = record
    Offset: Integer;
  end;

  /// <summary>
  ///   A pull consumer's batch request - the body published to
  ///   $JS.API.CONSUMER.MSG.NEXT.&lt;stream&gt;.&lt;consumer&gt;
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     This is not a request/reply in the RequestSync sense. The server
  ///     answers with up to Batch SEPARATE messages on the reply-to inbox, and
  ///     then a status message (404 / 408) to close the batch out. A partial
  ///     batch is a perfectly normal result.
  ///   </para>
  ///   <para>
  ///     Expires is the SERVER's own deadline for the request, and setting it
  ///     matters even when the client has its own timeout: without it the
  ///     server holds the request open, and a client that has walked away
  ///     leaves it counting against the consumer's MaxWaiting.
  ///   </para>
  /// </remarks>
  TJetStreamNextRequest = record
    /// How many messages at most. The server may send fewer, never more
    [NeonInclude(IncludeIf.NotDefault)]
    Batch: Integer;
    /// The server's deadline, after which it sends 408 and gives up
    [NeonInclude(IncludeIf.NotDefault)]
    Expires: TJetStreamDuration;
    /// A second ceiling, on total bytes rather than on count
    [NeonInclude(IncludeIf.NotDefault)]
    MaxBytes: Int64;
    /// <summary>
    ///   Answer straight away with whatever is there, 404 included, instead of
    ///   waiting for the batch to fill
    /// </summary>
    NoWait: Boolean;
    /// <summary>
    ///   Ask the server to send a 100 status this often while the request is
    ///   open, so a silent connection can be told from an empty stream
    /// </summary>
    [NeonInclude(IncludeIf.NotDefault)]
    IdleHeartbeat: TJetStreamDuration;
  end;

  /// <summary>
  ///   Narrows a purge. With none of these set the WHOLE stream is emptied,
  ///   which is why the filtered form exists as a separate call
  /// </summary>
  TJetStreamPurgeRequest = record
    /// Only messages on this subject - which may be a wildcard
    [NeonInclude(IncludeIf.NotDefault)]
    Filter: string;
    /// Only messages BELOW this sequence
    [NeonInclude(IncludeIf.NotDefault)]
    Seq: UInt64;
    /// Keep this many of the most recent, purge the rest
    [NeonInclude(IncludeIf.NotDefault)]
    Keep: UInt64;
  end;

  /// <summary>
  ///   Asks for one stored message: by sequence, or the last on a subject, or
  ///   the first at or after a sequence. Exactly one of them is set
  /// </summary>
  TJetStreamMsgGetRequest = record
    [NeonInclude(IncludeIf.NotDefault)]
    Seq: UInt64;
    /// The current value of a KV key is the LAST message on its subject
    [NeonInclude(IncludeIf.NotDefault)]
    LastBySubj: string;
    [NeonInclude(IncludeIf.NotDefault)]
    NextBySubj: string;
  end;

  /// <summary>
  ///   One message as the stream stored it. Reading it needs no consumer, which
  ///   is what makes a key/value get a single request
  /// </summary>
  /// <remarks>
  ///   Data and Hdrs are BASE64, not text: a stored message is arbitrary bytes,
  ///   and JSON has no way to carry those. Hdrs is the whole raw header block -
  ///   'NATS/1.0', the pairs, and the blank line - so it goes back through
  ///   TNatsParser.ParseHeaders rather than being read by hand.
  /// </remarks>
  TJetStreamStoredMsg = record
    Subject: string;
    Seq: UInt64;
    Data: string;
    Hdrs: string;
    /// RFC3339, kept as text for the same reason every other timestamp here is
    Time: string;
  end;

  TJetStreamMsgGetResponse = record
    Message: TJetStreamStoredMsg;
  end;

  /// <summary>
  ///   What DELETE and PURGE answer with. Purged is only meaningful for a purge
  /// </summary>
  TJetStreamSuccessResponse = record
    Success: Boolean;
    Purged: UInt64;
  end;

  TJetStreamApiStats = record
    Total: UInt64;
    Errors: UInt64;
  end;

  /// <summary>
  ///   Account limits. -1 means unlimited, which is why these are signed
  /// </summary>
  TJetStreamAccountLimits = record
    MaxMemory: Int64;
    MaxStorage: Int64;
    MaxStreams: Integer;
    MaxConsumers: Integer;
  end;

  /// <summary>
  ///   What this account is using and what it is allowed. The cheapest call
  ///   there is, so it doubles as "is JetStream even enabled here"
  /// </summary>
  TJetStreamAccountInfo = record
    Memory: UInt64;
    Storage: UInt64;
    Streams: Integer;
    Consumers: Integer;
    Domain: string;
    Api: TJetStreamApiStats;
    Limits: TJetStreamAccountLimits;
  end;

/// <summary>
///   The Neon configuration every JetStream record must be (de)serialized
///   with - always pass it explicitly, never use the parameterless TNeon
///   overloads
/// </summary>
/// <remarks>
///   A sibling of NatsJSONConfig in Nats.Entities, and it fails the same silent
///   way if bypassed: the parameterless overloads resolve to
///   TNeonConfiguration.Default, which is PascalCase, so every field would be
///   renamed, the server would reject or ignore the request, and a response
///   would deserialize to an empty record that reads as a successful default.
///   The nullable serializers are registered here so a Nullable&lt;T&gt; field
///   added later works without anyone having to remember this.
/// </remarks>
function JetStreamJSONConfig: INeonConfiguration;

type
  /// <summary>
  ///   Serializes any JetStream record through JetStreamJSONConfig. A class
  ///   rather than two plain functions only because Delphi has no generic
  ///   standalone routines
  /// </summary>
  TJetStreamJSON = class
  public
    class function ToJSON<T: record>(const AValue: T): string; static;
    class function FromJSON<T: record>(const AJson: string): T; static;
  end;

  /// <summary>
  ///   Neon serializer for Nullable&lt;enum&gt;: a set value goes out as the
  ///   wire name from the enum's NeonEnumNames attribute, an unset one is
  ///   omitted (or null, per the field's NeonInclude). The stock
  ///   RegisterNullableSerializers covers only primitives - without this an
  ///   enum nullable would be serialized as its raw record fields
  /// </summary>
  TNullableEnumSerializer<T> = class(TCustomSerializer)
  protected
    class function GetTargetInfo: PTypeInfo; override;
    class function CanHandle(AType: PTypeInfo): Boolean; override;
  public
    function Serialize(const AValue: TValue; ANeonObject: TNeonRttiObject;
      AContext: ISerializerContext): TJSONValue; override;
    function Deserialize(AValue: TJSONValue; const AData: TValue;
      ANeonObject: TNeonRttiObject; AContext: IDeserializerContext): TValue; override;
  end;

implementation

uses
  Neon.Core.Serializers.Nullables,
  Neon.Core.Utils;

{ TNullableEnumSerializer<T> }

class function TNullableEnumSerializer<T>.GetTargetInfo: PTypeInfo;
begin
  Result := TypeInfo(Nullable<T>);
end;

class function TNullableEnumSerializer<T>.CanHandle(AType: PTypeInfo): Boolean;
begin
  Result := AType = GetTargetInfo;
end;

function TNullableEnumSerializer<T>.Serialize(const AValue: TValue;
  ANeonObject: TNeonRttiObject; AContext: ISerializerContext): TJSONValue;
var
  LValue: Nullable<T>;
begin
  LValue := AValue.AsType<Nullable<T>>;

  if not LValue.HasValue then
  begin
    { Honour NeonInclude exactly as the stock nullable serializers do: an unset
      value is omitted under NotDefault/NotEmpty/NotNull, an explicit null
      otherwise }
    case ANeonObject.NeonInclude.Value of
      IncludeIf.NotNull,
      IncludeIf.NotEmpty,
      IncludeIf.NotDefault: Exit(nil);
    else
      Exit(TJSONNull.Create);
    end;
  end;

  { The wire name comes from the same NeonEnumNames attribute the plain enum
    writer reads, so the two can never drift apart }
  Result := TJSONString.Create(
    TTypeInfoUtils.EnumToString(TypeInfo(T),
      Integer(TValue.From<T>(LValue.Value).AsOrdinal)));
end;

function TNullableEnumSerializer<T>.Deserialize(AValue: TJSONValue;
  const AData: TValue; ANeonObject: TNeonRttiObject; AContext: IDeserializerContext): TValue;
var
  LValue: Nullable<T>;
  LNames: TArray<string>;
  LAttribute: NeonEnumNamesAttribute;
  LTypeData: PTypeData;
  LOrdinal, LIndex: Integer;
begin
  if AValue is TJSONNull then
  begin
    LValue := nil;
    Result := TValue.From<Nullable<T>>(LValue);
    Exit;
  end;

  if not (AValue is TJSONString) then
    raise ENeonException.Create(Self.ClassName + ' expects a JSON string');

  { The same name-to-ordinal mapping Neon's own enum reader uses: the
    NeonEnumNames attribute first, the Delphi name as the fallback }
  LAttribute := TRttiUtils.FindAttribute<NeonEnumNamesAttribute>(
    TRttiUtils.Context.GetType(TypeInfo(T)));
  if Assigned(LAttribute) then
    LNames := LAttribute.Names;

  LOrdinal := -1;
  for LIndex := Low(LNames) to High(LNames) do
    if LNames[LIndex] = AValue.Value then
      LOrdinal := LIndex;
  if LOrdinal = -1 then
    LOrdinal := GetEnumValue(TypeInfo(T), AValue.Value);

  LTypeData := GetTypeData(TypeInfo(T));
  if (LOrdinal < LTypeData.MinValue) or (LOrdinal > LTypeData.MaxValue) then
    raise ENeonException.CreateFmt('Invalid %s value [%s]',
      [TRttiUtils.Context.GetType(TypeInfo(T)).Name, AValue.Value]);

  LValue := TValue.FromOrdinal(TypeInfo(T), LOrdinal).AsType<T>;
  Result := TValue.From<Nullable<T>>(LValue);
end;

function JetStreamJSONConfig: INeonConfiguration;
begin
  Result := TNeonConfiguration.Create.SetMemberCase(TNeonCase.SnakeCase);
  RegisterNullableSerializers(Result.GetSerializers);
  { The stock nullable serializers cover only primitives; an enum nullable
    would otherwise be serialized as its raw record fields. TNullableEnumSerializer
    writes the same NeonEnumNames the plain enum writer reads }
  Result.GetSerializers.RegisterSerializer(TNullableEnumSerializer<TJetStreamAckPolicy>);
end;

{ TJetStreamJSON }

class function TJetStreamJSON.ToJSON<T>(const AValue: T): string;
begin
  Result := TNeon.ValueToJSONString(TValue.From<T>(AValue), JetStreamJSONConfig);
end;

class function TJetStreamJSON.FromJSON<T>(const AJson: string): T;
begin
  Result := TNeon.JSONToValue<T>(AJson, JetStreamJSONConfig);
end;

{ TJetStreamDurationHelper }

class function TJetStreamDurationHelper.FromMillis(AValue: Int64): TJetStreamDuration;
begin
  Result := AValue * NANOS_PER_MILLI;
end;

class function TJetStreamDurationHelper.FromSeconds(AValue: Int64): TJetStreamDuration;
begin
  Result := AValue * NANOS_PER_SECOND;
end;

class function TJetStreamDurationHelper.FromMinutes(AValue: Int64): TJetStreamDuration;
begin
  Result := AValue * NANOS_PER_MINUTE;
end;

function TJetStreamDurationHelper.ToMillis: Int64;
begin
  Result := Int64(Self) div NANOS_PER_MILLI;
end;

function TJetStreamDurationHelper.ToSeconds: Double;
begin
  Result := Int64(Self) / NANOS_PER_SECOND;
end;

{ TJetStreamApiError }

function TJetStreamApiError.HasError: Boolean;
begin
  { Code is the discriminator rather than Description: the server always sets a
    code on a real error, and an absent "error" object leaves this record zeroed }
  Result := Code <> 0;
end;

end.
