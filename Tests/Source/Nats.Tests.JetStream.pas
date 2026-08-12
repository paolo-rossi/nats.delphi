{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Tests.JetStream;

{******************************************************************************}
{                                                                              }
{  JetStream unit tests.                                                        }
{                                                                              }
{  The metadata and entity fixtures are pure and offline. The context fixture   }
{  drives a real TNatsConnection over TNatsMockSocket, so the $JS.API subjects  }
{  and bodies are asserted byte-for-byte with no server running.                }
{                                                                              }
{******************************************************************************}

interface

uses
  System.SysUtils, System.Classes, System.DateUtils, System.JSON,

  DUnitX.TestFramework,

  Nats.Consts,
  Nats.Classes,
  Nats.Connection,
  Nats.Exceptions,
  Nats.JetStream.Consts,
  Nats.JetStream.Client,
  Nats.JetStream.Entities,
  Nats.JetStream.Message,

  Nats.Tests.Mocks;

type
  [TestFixture]
  TJetStreamMetadataTests = class
  public
    { V2 - what any current server sends }

    [Test]
    procedure V2Subject_ParsesEveryField;
    [Test]
    procedure V2Subject_WithExtraTokens_StillParses;
    [Test]
    procedure V2Subject_PlaceholderDomain_ReadsAsEmpty;
    [Test]
    procedure V2Subject_RealDomain_IsKept;

    { V1 - the trap. Nine tokens, no domain and no account hash }

    [Test]
    procedure V1Subject_ParsesEveryField;
    [Test]
    procedure V1Subject_HasNoDomainOrAccountHash;
    // reading a V1 subject at V2 positions does not fail, it silently returns
    // the wrong field for every one of them - so the counts must not be confused
    [Test]
    procedure V1AndV2_WithTheSameValues_ParseIdentically;

    { not an ack subject at all }

    [Test]
    procedure EmptySubject_Fails;
    [Test]
    procedure CoreNatsInbox_Fails;
    [Test]
    procedure WrongPrefix_Fails;
    [Test]
    procedure TooFewTokens_Fails;
    [Test]
    procedure TenTokens_Fails;
    [Test]
    procedure NonNumericSequence_Fails;
    [Test]
    procedure FailedParse_LeavesMetadataEmpty;

    { derived values }

    [Test]
    procedure FirstDelivery_IsNotARedelivery;
    [Test]
    procedure SecondDelivery_IsARedelivery;
    [Test]
    procedure Timestamp_ConvertsToUtc;
  end;

  /// <summary>
  ///   The entity layer, checked against JSON captured from a real nats-server.
  ///   A field name here IS a protocol field - Neon derives the wire spelling
  ///   from the Delphi one - so these tests are what stops a rename from
  ///   silently breaking the wire format
  /// </summary>
  [TestFixture]
  TJetStreamEntityTests = class
  private
    function JsonOf(const AJson: string): TJSONObject;
  public
    { duration }

    [Test]
    procedure Duration_FromSeconds_IsNanoseconds;
    [Test]
    procedure Duration_RoundTripsBackToItsUnit;
    [Test]
    procedure Duration_SerializesAsAPlainNumber;

    { stream config - the write path }

    [Test]
    procedure StreamConfig_FieldNames_AreSnakeCase;
    [Test]
    procedure StreamConfig_UnsetFields_AreOmitted;
    [Test]
    procedure StreamConfig_BooleansAndEnums_AreAlwaysEmitted;
    [Test]
    procedure StreamConfig_Enums_SerializeAsStrings;
    [Test]
    procedure StreamConfig_Enums_DeserializeFromStrings;
    [Test]
    procedure StreamConfig_RoundTrips;

    { stream info - the read path }

    [Test]
    procedure StreamInfo_RealServerJson_ParsesEveryField;
    [Test]
    procedure StreamInfo_UnknownFields_AreIgnored;

    { consumer }

    [Test]
    procedure ConsumerConfig_FieldNames_AreSnakeCase;
    [Test]
    procedure ConsumerConfig_PullConsumer_OmitsDeliverSubject;
    [Test]
    procedure ConsumerInfo_RealServerJson_ParsesEveryField;

    { publish ack and errors }

    [Test]
    procedure PubAck_ParsesRealServerJson;
    [Test]
    procedure PubAck_Duplicate_IsReported;
    [Test]
    procedure ApiError_ParsesRealServerJson;
    [Test]
    procedure ApiResponse_Success_HasNoError;

    { paged lists }

    [Test]
    procedure StreamList_ParsesPagingEnvelope;
    [Test]
    procedure ConsumerList_ParsesPagingEnvelope;
  end;

  /// <summary>
  ///   TJetStreamContext against a mock socket: what goes out on $JS.API, and
  ///   what the client makes of what comes back
  /// </summary>
  [TestFixture]
  TJetStreamContextTests = class
  private
    FConn: TNatsConnection;
    FSocket: TNatsMockSocket;
    FJs: TJetStreamContext;
    FServerThread: TThread;

    procedure OpenAndHandshake;
    /// <summary>
    ///   Runs AProc and swallows the timeout. For tests that only care what the
    ///   client WROTE - nobody has to answer for that to be observable
    /// </summary>
    procedure CaptureRequest(const AProc: TProc);
    /// Plays the server: waits for the inbox SUB, then answers with AJson
    procedure ReplyWith(const AJson: string);
    /// The subject of the PUB the client wrote
    function RequestSubject: string;
    /// The body of the PUB the client wrote
    function RequestBody: string;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    { subjects }

    [Test]
    procedure StreamInfo_UsesTheApiSubject;
    [Test]
    procedure Domain_MovesTheApiUnderTheDomainPrefix;
    [Test]
    procedure AddStream_SendsTheConfigAsTheBody;
    [Test]
    procedure AddConsumer_Durable_UsesTheNamedSubject;
    [Test]
    procedure AddConsumer_Ephemeral_UsesTheUnnamedSubject;
    [Test]
    procedure AddConsumer_SendsStreamNameInTheBodyToo;
    [Test]
    procedure ListStreams_SendsTheOffset;

    { names go into the subject verbatim, so they must be checked }

    [Test]
    procedure StreamName_WithADot_Raises;
    [Test]
    procedure StreamName_Empty_Raises;
    [Test]
    procedure ConsumerName_WithAWildcard_Raises;

    { responses }

    [Test]
    procedure StreamInfo_ParsesTheResponse;
    [Test]
    procedure DeleteStream_ReturnsSuccess;
    [Test]
    procedure PurgeStream_ReturnsThePurgedCount;
    [Test]
    procedure StreamNames_ReturnsTheNames;
    [Test]
    procedure AccountInfo_ParsesLimitsAndUsage;

    { errors - the reason ApiRequest is a choke point }

    [Test]
    procedure ApiError_RaisesInsteadOfReturningAnEmptyRecord;
    [Test]
    procedure ApiError_CarriesErrCodeAndDescription;
    [Test]
    procedure ApiError_NotFound_IsRecognised;
    [Test]
    procedure NoReply_RaisesTimeout;
  end;

implementation

const
  { $JS.ACK.<domain>.<hash>.<stream>.<consumer>.<delivered>.<stream seq>.
    <consumer seq>.<timestamp>.<pending>.<random> }
  V2_SUBJECT = '$JS.ACK.hub.ACCHASH.ORDERS.workers.3.42.7.1700000000123456789.5.rand01';

  { the same delivery as seen from a pre-2.2 server: no domain, no hash, no
    trailing random token }
  V1_SUBJECT = '$JS.ACK.ORDERS.workers.3.42.7.1700000000123456789.5';

  TIMESTAMP_NANOS = UInt64(1700000000123456789);

  { Captured verbatim from nats-server 2.10 - $JS.API.STREAM.INFO.ORDERS }
  STREAM_INFO_JSON =
    '{"type":"io.nats.jetstream.api.v1.stream_info_response",' +
    '"config":{"name":"ORDERS","subjects":["orders.*"],"retention":"limits",' +
    '"max_consumers":-1,"max_msgs":-1,"max_bytes":-1,"max_age":0,' +
    '"max_msgs_per_subject":-1,"max_msg_size":-1,"discard":"old","storage":"file",' +
    '"num_replicas":1,"duplicate_window":120000000000,"allow_direct":false,' +
    '"mirror_direct":false,"sealed":false,"deny_delete":false,"deny_purge":false,' +
    '"allow_rollup_hdrs":false},' +
    '"created":"2023-11-14T22:13:20.123456789Z",' +
    '"state":{"messages":3,"bytes":147,"first_seq":1,' +
    '"first_ts":"2023-11-14T22:13:21Z","last_seq":3,"last_ts":"2023-11-14T22:13:23Z",' +
    '"num_subjects":1,"num_deleted":0,"consumer_count":1}}';

  { $JS.API.CONSUMER.INFO.ORDERS.workers }
  CONSUMER_INFO_JSON =
    '{"type":"io.nats.jetstream.api.v1.consumer_info_response",' +
    '"stream_name":"ORDERS","name":"workers","created":"2023-11-14T22:13:25Z",' +
    '"config":{"durable_name":"workers","name":"workers","deliver_policy":"all",' +
    '"ack_policy":"explicit","ack_wait":30000000000,"max_deliver":-1,' +
    '"filter_subject":"orders.*","replay_policy":"instant","max_waiting":512,' +
    '"max_ack_pending":1000,"num_replicas":0},' +
    '"delivered":{"consumer_seq":2,"stream_seq":2},' +
    '"ack_floor":{"consumer_seq":1,"stream_seq":1},' +
    '"num_ack_pending":1,"num_redelivered":0,"num_waiting":0,"num_pending":1,' +
    '"push_bound":false}';

  { context fixture }

  MOCK_TIMEOUT = 200;
  /// Short, so the tests that only care what was WRITTEN do not sit out a wait
  API_TIMEOUT = 200;

  JS_INFO_JSON =
    '{"server_id":"NDHJZQZ4YQXQ","server_name":"nats-1","version":"2.10.11",' +
    '"proto":1,"host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,' +
    '"jetstream":true,"client_id":7,"client_ip":"127.0.0.1"}';

  { what comes back instead of a result when the call fails }
  ERROR_RESPONSE_JSON =
    '{"type":"io.nats.jetstream.api.v1.stream_info_response",' +
    '"error":{"code":404,"err_code":10059,"description":"stream not found"}}';

{ TJetStreamMetadataTests }

procedure TJetStreamMetadataTests.V2Subject_ParsesEveryField;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT, LMeta), 'a V2 ack subject must parse');

  Assert.AreEqual('hub', LMeta.Domain);
  Assert.AreEqual('ACCHASH', LMeta.AccountHash);
  Assert.AreEqual('ORDERS', LMeta.Stream);
  Assert.AreEqual('workers', LMeta.Consumer);
  Assert.AreEqual(UInt64(3), LMeta.NumDelivered);
  Assert.AreEqual(UInt64(42), LMeta.StreamSeq);
  Assert.AreEqual(UInt64(7), LMeta.ConsumerSeq);
  Assert.AreEqual(TIMESTAMP_NANOS, LMeta.TimestampNanos);
  Assert.AreEqual(UInt64(5), LMeta.NumPending);
end;

procedure TJetStreamMetadataTests.V2Subject_WithExtraTokens_StillParses;
var
  LMeta: TJetStreamMsgMetadata;
begin
  { A later server may append tokens. Appending must not break a client, so the
    V2 count is a minimum and not an equality }
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT + '.future.tokens', LMeta),
    'extra trailing tokens must be tolerated, not rejected');
  Assert.AreEqual('ORDERS', LMeta.Stream);
  Assert.AreEqual(UInt64(42), LMeta.StreamSeq);
end;

procedure TJetStreamMetadataTests.V2Subject_PlaceholderDomain_ReadsAsEmpty;
var
  LMeta: TJetStreamMsgMetadata;
begin
  // a server with no domain configured sends '_', not an empty token
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK._.ACCHASH.ORDERS.workers.1.42.7.1700000000123456789.5.rand01', LMeta));

  Assert.AreEqual('', LMeta.Domain,
    'the placeholder means no domain, not a domain named "_"');
end;

procedure TJetStreamMetadataTests.V2Subject_RealDomain_IsKept;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT, LMeta));
  Assert.AreEqual('hub', LMeta.Domain, 'a real domain must survive');
end;

procedure TJetStreamMetadataTests.V1Subject_ParsesEveryField;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V1_SUBJECT, LMeta), 'a V1 ack subject must parse');

  Assert.AreEqual('ORDERS', LMeta.Stream);
  Assert.AreEqual('workers', LMeta.Consumer);
  Assert.AreEqual(UInt64(3), LMeta.NumDelivered);
  Assert.AreEqual(UInt64(42), LMeta.StreamSeq);
  Assert.AreEqual(UInt64(7), LMeta.ConsumerSeq);
  Assert.AreEqual(TIMESTAMP_NANOS, LMeta.TimestampNanos);
  Assert.AreEqual(UInt64(5), LMeta.NumPending);
end;

procedure TJetStreamMetadataTests.V1Subject_HasNoDomainOrAccountHash;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V1_SUBJECT, LMeta));

  Assert.AreEqual('', LMeta.Domain, 'V1 has no domain token to read');
  Assert.AreEqual('', LMeta.AccountHash, 'nor an account hash');
end;

procedure TJetStreamMetadataTests.V1AndV2_WithTheSameValues_ParseIdentically;
var
  LV1, LV2: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V1_SUBJECT, LV1));
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT, LV2));

  { The two subjects describe the same delivery. Indexing by position without
    checking the token count first would shift every V1 field by two and make
    these disagree - while still looking like perfectly plausible numbers }
  Assert.AreEqual(LV2.Stream, LV1.Stream);
  Assert.AreEqual(LV2.Consumer, LV1.Consumer);
  Assert.AreEqual(LV2.NumDelivered, LV1.NumDelivered);
  Assert.AreEqual(LV2.StreamSeq, LV1.StreamSeq);
  Assert.AreEqual(LV2.ConsumerSeq, LV1.ConsumerSeq);
  Assert.AreEqual(LV2.TimestampNanos, LV1.TimestampNanos);
  Assert.AreEqual(LV2.NumPending, LV1.NumPending);
end;

procedure TJetStreamMetadataTests.EmptySubject_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse('', LMeta));
end;

procedure TJetStreamMetadataTests.CoreNatsInbox_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  { An ordinary request reply-to. Not JetStream, and not an error either - every
    core NATS request produces one of these }
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse('_INBOX.aBcDeFgHiJkLmNoPqRsTuV', LMeta));
end;

procedure TJetStreamMetadataTests.WrongPrefix_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  // right shape, wrong verb: $JS.API is a request, not an acknowledgement
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse(
    '$JS.API.hub.ACCHASH.ORDERS.workers.3.42.7.1700000000123456789.5.rand01', LMeta),
    'the first two tokens must be checked, not assumed');
end;

procedure TJetStreamMetadataTests.TooFewTokens_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse('$JS.ACK.ORDERS.workers', LMeta));
end;

procedure TJetStreamMetadataTests.TenTokens_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  { Ten and eleven tokens are neither layout. Accepting them would mean guessing
    which fields are missing }
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK.ORDERS.workers.3.42.7.1700000000123456789.5.extra', LMeta),
    'a count between the two layouts is not a subject we can read');
end;

procedure TJetStreamMetadataTests.NonNumericSequence_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  { Reporting sequence 0 here would be worse than failing: a consumer would go
    on to acknowledge the wrong message }
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK.ORDERS.workers.3.NOTANUMBER.7.1700000000123456789.5', LMeta));
end;

procedure TJetStreamMetadataTests.FailedParse_LeavesMetadataEmpty;
var
  LMeta: TJetStreamMsgMetadata;
begin
  // seeded, so a half-filled record would be visible
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT, LMeta));
  Assert.AreEqual('ORDERS', LMeta.Stream, 'guard: the seed must have taken');

  Assert.IsFalse(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK.ORDERS.workers.3.NOTANUMBER.7.1700000000123456789.5', LMeta));

  Assert.AreEqual('', LMeta.Stream, 'a failed parse must not leave the old value behind');
  Assert.AreEqual(UInt64(0), LMeta.StreamSeq);
end;

procedure TJetStreamMetadataTests.FirstDelivery_IsNotARedelivery;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK.ORDERS.workers.1.42.7.1700000000123456789.5', LMeta));

  Assert.AreEqual(UInt64(1), LMeta.NumDelivered, 'delivery counting starts at 1, not 0');
  Assert.IsFalse(LMeta.IsRedelivery);
end;

procedure TJetStreamMetadataTests.SecondDelivery_IsARedelivery;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK.ORDERS.workers.2.42.7.1700000000123456789.5', LMeta));

  Assert.IsTrue(LMeta.IsRedelivery, 'a second delivery means the first was never acked');
end;

procedure TJetStreamMetadataTests.Timestamp_ConvertsToUtc;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT, LMeta));

  // 1700000000 seconds after the epoch is 2023-11-14 22:13:20 UTC
  Assert.AreEqual(EncodeDateTime(2023, 11, 14, 22, 13, 20, 0), LMeta.TimestampUTC, 0.0001,
    'the nanosecond timestamp must convert to the right instant');
end;

{ TJetStreamEntityTests }

function TJetStreamEntityTests.JsonOf(const AJson: string): TJSONObject;
begin
  Result := TJSONObject.ParseJSONValue(AJson) as TJSONObject;
  Assert.IsNotNull(Result, 'the serializer did not produce a JSON object: ' + AJson);
end;

procedure TJetStreamEntityTests.Duration_FromSeconds_IsNanoseconds;
begin
  { A factor of 10^6 out and the stream is still created - it just behaves
    nothing like the caller asked. That is why this type exists }
  Assert.AreEqual(Int64(30000000000), Int64(TJetStreamDuration.FromSeconds(30)));
  Assert.AreEqual(Int64(1500000000), Int64(TJetStreamDuration.FromMillis(1500)));   // 1.5 s
  Assert.AreEqual(Int64(120000000000), Int64(TJetStreamDuration.FromMinutes(2)));
end;

procedure TJetStreamEntityTests.Duration_RoundTripsBackToItsUnit;
begin
  Assert.AreEqual(Int64(1500), TJetStreamDuration.FromMillis(1500).ToMillis);
  Assert.AreEqual(Double(30), TJetStreamDuration.FromSeconds(30).ToSeconds, 0.0001);
end;

procedure TJetStreamEntityTests.Duration_SerializesAsAPlainNumber;
var
  LConfig: TJetStreamStreamConfig;
  LObj: TJSONObject;
begin
  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := 'ORDERS';
  LConfig.MaxAge := TJetStreamDuration.FromSeconds(60);

  LObj := JsonOf(TJetStreamJSON.ToJSON<TJetStreamStreamConfig>(LConfig));
  try
    // a distinct Int64 type, so it must land on the wire as a bare integer
    Assert.AreEqual(Int64(60000000000), LObj.GetValue<Int64>('max_age'));
  finally
    LObj.Free;
  end;
end;

procedure TJetStreamEntityTests.StreamConfig_FieldNames_AreSnakeCase;
var
  LConfig: TJetStreamStreamConfig;
  LObj: TJSONObject;
begin
  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := 'ORDERS';
  LConfig.MaxMsgsPerSubject := 10;
  LConfig.AllowRollupHdrs := True;
  LConfig.NumReplicas := 3;
  LConfig.DuplicateWindow := TJetStreamDuration.FromMinutes(2);

  LObj := JsonOf(TJetStreamJSON.ToJSON<TJetStreamStreamConfig>(LConfig));
  try
    { A field name here IS a protocol field: Neon derives the wire spelling from
      the Delphi one, so a rename that stops snake-casing to the NATS name fails
      silently - the server just ignores what it does not recognise }
    Assert.IsNotNull(LObj.GetValue('max_msgs_per_subject'), 'MaxMsgsPerSubject');
    Assert.IsNotNull(LObj.GetValue('allow_rollup_hdrs'), 'AllowRollupHdrs');
    Assert.IsNotNull(LObj.GetValue('num_replicas'), 'NumReplicas');
    Assert.IsNotNull(LObj.GetValue('duplicate_window'), 'DuplicateWindow');
    Assert.IsNotNull(LObj.GetValue('name'), 'Name');
  finally
    LObj.Free;
  end;
end;

procedure TJetStreamEntityTests.StreamConfig_UnsetFields_AreOmitted;
var
  LConfig: TJetStreamStreamConfig;
  LObj: TJSONObject;
begin
  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := 'ORDERS';
  LConfig.Subjects := ['orders.*'];

  LObj := JsonOf(TJetStreamJSON.ToJSON<TJetStreamStreamConfig>(LConfig));
  try
    Assert.IsNotNull(LObj.GetValue('name'));
    Assert.IsNotNull(LObj.GetValue('subjects'));

    { Omitted rather than sent as zero, so the server applies its own default.
      It also keeps a read-modify-write round trip from rewriting fields the
      caller never touched }
    Assert.IsNull(LObj.GetValue('max_msgs'), 'an unset limit must be omitted');
    Assert.IsNull(LObj.GetValue('max_bytes'), 'an unset limit must be omitted');
    Assert.IsNull(LObj.GetValue('max_age'), 'an unset duration must be omitted');
    Assert.IsNull(LObj.GetValue('description'), 'an empty string must be omitted');
  finally
    LObj.Free;
  end;
end;

procedure TJetStreamEntityTests.StreamConfig_BooleansAndEnums_AreAlwaysEmitted;
var
  LConfig: TJetStreamStreamConfig;
  LObj: TJSONObject;
begin
  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := 'ORDERS';

  LObj := JsonOf(TJetStreamJSON.ToJSON<TJetStreamStreamConfig>(LConfig));
  try
    { Neon's WriteBoolean and WriteEnum ignore IncludeIf entirely, so these go
      out even at their default. That is SAFE here, and the reason is worth
      pinning rather than rediscovering: the Delphi zero value of each one is
      exactly the default nats-server applies when the field is absent -
      retention "limits", storage "file", discard "old", every flag false. So
      sending them is indistinguishable from omitting them.

      If a future field breaks that correspondence it cannot use a plain
      Boolean or enum - it needs Nullable<T>, whose serializer IS registered
      in JetStreamJSONConfig for exactly this. }
    Assert.AreEqual('limits', LObj.GetValue<string>('retention'));
    Assert.AreEqual('file', LObj.GetValue<string>('storage'));
    Assert.AreEqual('old', LObj.GetValue<string>('discard'));
    Assert.IsFalse(LObj.GetValue<Boolean>('deny_delete'));
    Assert.IsFalse(LObj.GetValue<Boolean>('sealed'));
  finally
    LObj.Free;
  end;
end;

procedure TJetStreamEntityTests.StreamConfig_Enums_SerializeAsStrings;
var
  LConfig: TJetStreamStreamConfig;
  LObj: TJSONObject;
begin
  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := 'ORDERS';
  LConfig.Retention := TJetStreamRetention.WorkQueue;
  LConfig.Storage := TJetStreamStorage.Memory;
  LConfig.Discard := TJetStreamDiscard.New;

  LObj := JsonOf(TJetStreamJSON.ToJSON<TJetStreamStreamConfig>(LConfig));
  try
    // ordinals would be silently accepted as something else entirely
    Assert.AreEqual('workqueue', LObj.GetValue<string>('retention'));
    Assert.AreEqual('memory', LObj.GetValue<string>('storage'));
    Assert.AreEqual('new', LObj.GetValue<string>('discard'));
  finally
    LObj.Free;
  end;
end;

procedure TJetStreamEntityTests.StreamConfig_Enums_DeserializeFromStrings;
var
  LConfig: TJetStreamStreamConfig;
begin
  LConfig := TJetStreamJSON.FromJSON<TJetStreamStreamConfig>(
    '{"name":"ORDERS","retention":"interest","storage":"memory","discard":"new"}');

  Assert.IsTrue(LConfig.Retention = TJetStreamRetention.Interest, 'retention');
  Assert.IsTrue(LConfig.Storage = TJetStreamStorage.Memory, 'storage');
  Assert.IsTrue(LConfig.Discard = TJetStreamDiscard.New, 'discard');
end;

procedure TJetStreamEntityTests.StreamInfo_RealServerJson_ParsesEveryField;
var
  LInfo: TJetStreamStreamInfo;
begin
  LInfo := TJetStreamJSON.FromJSON<TJetStreamStreamInfo>(STREAM_INFO_JSON);

  Assert.AreEqual('ORDERS', LInfo.Config.Name);
  Assert.AreEqual(1, Length(LInfo.Config.Subjects));
  Assert.AreEqual('orders.*', LInfo.Config.Subjects[0]);
  Assert.IsTrue(LInfo.Config.Retention = TJetStreamRetention.Limits);
  Assert.IsTrue(LInfo.Config.Storage = TJetStreamStorage.Filestore);
  Assert.AreEqual(Int64(-1), LInfo.Config.MaxMsgs);
  Assert.AreEqual(1, LInfo.Config.NumReplicas);
  Assert.AreEqual(Int64(120000000000), Int64(LInfo.Config.DuplicateWindow));

  Assert.AreEqual(UInt64(3), LInfo.State.Messages);
  Assert.AreEqual(UInt64(147), LInfo.State.Bytes);
  Assert.AreEqual(UInt64(1), LInfo.State.FirstSeq);
  Assert.AreEqual(UInt64(3), LInfo.State.LastSeq);
  Assert.AreEqual(1, LInfo.State.ConsumerCount);
  Assert.AreEqual(1, LInfo.State.NumSubjects);
end;

procedure TJetStreamEntityTests.StreamInfo_UnknownFields_AreIgnored;
var
  LInfo: TJetStreamStreamInfo;
begin
  { A real response carries "type", and newer servers add fields this client has
    never heard of. Neither may break the parse }
  LInfo := TJetStreamJSON.FromJSON<TJetStreamStreamInfo>(
    '{"type":"io.nats.jetstream.api.v1.stream_info_response",' +
    '"config":{"name":"ORDERS","some_future_field":42},"invented":{"a":1}}');

  Assert.AreEqual('ORDERS', LInfo.Config.Name);
end;

procedure TJetStreamEntityTests.StreamConfig_RoundTrips;
var
  LBefore, LAfter: TJetStreamStreamConfig;
begin
  LBefore := Default(TJetStreamStreamConfig);
  LBefore.Name := 'ORDERS';
  LBefore.Subjects := ['orders.*', 'orders.priority.>'];
  LBefore.Retention := TJetStreamRetention.WorkQueue;
  LBefore.Storage := TJetStreamStorage.Memory;
  LBefore.MaxMsgs := 1000;
  LBefore.MaxAge := TJetStreamDuration.FromMinutes(5);
  LBefore.NumReplicas := 3;
  LBefore.DenyDelete := True;

  LAfter := TJetStreamJSON.FromJSON<TJetStreamStreamConfig>(
    TJetStreamJSON.ToJSON<TJetStreamStreamConfig>(LBefore));

  Assert.AreEqual(LBefore.Name, LAfter.Name);
  Assert.AreEqual(2, Length(LAfter.Subjects));
  Assert.AreEqual('orders.priority.>', LAfter.Subjects[1]);
  Assert.IsTrue(LBefore.Retention = LAfter.Retention);
  Assert.IsTrue(LBefore.Storage = LAfter.Storage);
  Assert.AreEqual(LBefore.MaxMsgs, LAfter.MaxMsgs);
  Assert.AreEqual(Int64(LBefore.MaxAge), Int64(LAfter.MaxAge));
  Assert.AreEqual(LBefore.NumReplicas, LAfter.NumReplicas);
  Assert.AreEqual(LBefore.DenyDelete, LAfter.DenyDelete);
end;

procedure TJetStreamEntityTests.ConsumerConfig_FieldNames_AreSnakeCase;
var
  LConfig: TJetStreamConsumerConfig;
  LObj: TJSONObject;
begin
  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := 'workers';
  LConfig.AckPolicy := TJetStreamAckPolicy.Explicit;
  LConfig.AckWait := TJetStreamDuration.FromSeconds(30);
  LConfig.MaxAckPending := 1000;
  LConfig.FilterSubject := 'orders.*';
  LConfig.InactiveThreshold := TJetStreamDuration.FromMinutes(5);
  LConfig.RateLimitBps := 1024;

  LObj := JsonOf(TJetStreamJSON.ToJSON<TJetStreamConsumerConfig>(LConfig));
  try
    Assert.IsNotNull(LObj.GetValue('durable_name'), 'DurableName');
    Assert.IsNotNull(LObj.GetValue('ack_wait'), 'AckWait');
    Assert.IsNotNull(LObj.GetValue('max_ack_pending'), 'MaxAckPending');
    Assert.IsNotNull(LObj.GetValue('filter_subject'), 'FilterSubject');
    Assert.IsNotNull(LObj.GetValue('inactive_threshold'), 'InactiveThreshold');
    Assert.IsNotNull(LObj.GetValue('rate_limit_bps'), 'RateLimitBps');
    Assert.AreEqual('explicit', LObj.GetValue<string>('ack_policy'));
  finally
    LObj.Free;
  end;
end;

procedure TJetStreamEntityTests.ConsumerConfig_PullConsumer_OmitsDeliverSubject;
var
  LConfig: TJetStreamConsumerConfig;
  LObj: TJSONObject;
begin
  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := 'workers';

  LObj := JsonOf(TJetStreamJSON.ToJSON<TJetStreamConsumerConfig>(LConfig));
  try
    { deliver_subject is what makes a consumer a PUSH consumer. Sending it empty
      rather than omitting it is the difference between a pull consumer and a
      rejected request }
    Assert.IsNull(LObj.GetValue('deliver_subject'),
      'an empty deliver_subject must be omitted, not sent as ""');
  finally
    LObj.Free;
  end;
end;

procedure TJetStreamEntityTests.ConsumerInfo_RealServerJson_ParsesEveryField;
var
  LInfo: TJetStreamConsumerInfo;
begin
  LInfo := TJetStreamJSON.FromJSON<TJetStreamConsumerInfo>(CONSUMER_INFO_JSON);

  Assert.AreEqual('ORDERS', LInfo.StreamName);
  Assert.AreEqual('workers', LInfo.Name);
  Assert.AreEqual('workers', LInfo.Config.DurableName);
  Assert.IsTrue(LInfo.Config.AckPolicy = TJetStreamAckPolicy.Explicit);
  Assert.IsTrue(LInfo.Config.DeliverPolicy = TJetStreamDeliverPolicy.All);
  Assert.IsTrue(LInfo.Config.ReplayPolicy = TJetStreamReplayPolicy.Instant);
  Assert.AreEqual(Int64(30000000000), Int64(LInfo.Config.AckWait));

  Assert.AreEqual(UInt64(2), LInfo.Delivered.ConsumerSeq);
  Assert.AreEqual(UInt64(2), LInfo.Delivered.StreamSeq);
  Assert.AreEqual(UInt64(1), LInfo.AckFloor.ConsumerSeq);
  Assert.AreEqual(1, LInfo.NumAckPending);
  Assert.AreEqual(UInt64(1), LInfo.NumPending);
  Assert.IsFalse(LInfo.PushBound);
end;

procedure TJetStreamEntityTests.PubAck_ParsesRealServerJson;
var
  LAck: TJetStreamPubAck;
begin
  LAck := TJetStreamJSON.FromJSON<TJetStreamPubAck>(
    '{"stream":"ORDERS","seq":42,"domain":"hub"}');

  Assert.AreEqual('ORDERS', LAck.Stream);
  Assert.AreEqual(UInt64(42), LAck.Seq);
  Assert.AreEqual('hub', LAck.Domain);
  Assert.IsFalse(LAck.Duplicate, 'absent means not a duplicate');
end;

procedure TJetStreamEntityTests.PubAck_Duplicate_IsReported;
var
  LAck: TJetStreamPubAck;
begin
  { The message was NOT stored again - Seq points at the original. A publisher
    that ignores this counts one message twice }
  LAck := TJetStreamJSON.FromJSON<TJetStreamPubAck>(
    '{"stream":"ORDERS","seq":7,"duplicate":true}');

  Assert.IsTrue(LAck.Duplicate);
  Assert.AreEqual(UInt64(7), LAck.Seq);
end;

procedure TJetStreamEntityTests.ApiError_ParsesRealServerJson;
var
  LResponse: TJetStreamApiResponse;
begin
  LResponse := TJetStreamJSON.FromJSON<TJetStreamApiResponse>(ERROR_RESPONSE_JSON);

  Assert.IsTrue(LResponse.Error.HasError, 'the error object must be detected');
  Assert.AreEqual(404, LResponse.Error.Code);
  Assert.AreEqual(10059, LResponse.Error.ErrCode, 'err_code is the one worth branching on');
  Assert.AreEqual('stream not found', LResponse.Error.Description);
end;

procedure TJetStreamEntityTests.ApiResponse_Success_HasNoError;
var
  LResponse: TJetStreamApiResponse;
begin
  { A success carries no error object at all, which deserializes to a zeroed
    record - so HasError, not "is the record assigned", is the test }
  LResponse := TJetStreamJSON.FromJSON<TJetStreamApiResponse>(STREAM_INFO_JSON);

  Assert.IsFalse(LResponse.Error.HasError);
  Assert.AreEqual('io.nats.jetstream.api.v1.stream_info_response', LResponse.ResponseType,
    'the reserved word "type" must still map to its wire name');
end;

procedure TJetStreamEntityTests.StreamList_ParsesPagingEnvelope;
var
  LList: TJetStreamStreamListResponse;
begin
  LList := TJetStreamJSON.FromJSON<TJetStreamStreamListResponse>(
    '{"type":"io.nats.jetstream.api.v1.stream_list_response","total":2,"offset":0,' +
    '"limit":256,"streams":[{"config":{"name":"ORDERS"}},{"config":{"name":"EVENTS"}}]}');

  // Total is how many exist, not how many are in this page
  Assert.AreEqual(2, LList.Total);
  Assert.AreEqual(0, LList.Offset);
  Assert.AreEqual(256, LList.Limit);
  Assert.AreEqual(2, Length(LList.Streams));
  Assert.AreEqual('ORDERS', LList.Streams[0].Config.Name);
  Assert.AreEqual('EVENTS', LList.Streams[1].Config.Name);
end;

procedure TJetStreamEntityTests.ConsumerList_ParsesPagingEnvelope;
var
  LList: TJetStreamConsumerListResponse;
begin
  LList := TJetStreamJSON.FromJSON<TJetStreamConsumerListResponse>(
    '{"total":1,"offset":0,"limit":256,' +
    '"consumers":[{"stream_name":"ORDERS","name":"workers"}]}');

  Assert.AreEqual(1, LList.Total);
  Assert.AreEqual(1, Length(LList.Consumers));
  Assert.AreEqual('workers', LList.Consumers[0].Name);
end;

{ TJetStreamContextTests }

procedure TJetStreamContextTests.Setup;
begin
  UseMockSocket;   // the live fixture flips the process-wide default

  FConn := TNatsConnection.Create;
  FSocket := TNatsMockSocket.LastInstance;
  Assert.IsNotNull(FSocket, 'the connection did not create a mock socket');

  FConn.Name := 'JsTestConn';
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, MOCK_TIMEOUT);

  FJs := TJetStreamContext.Create(FConn);
  FJs.Timeout := API_TIMEOUT;
end;

procedure TJetStreamContextTests.TearDown;
begin
  { Joined before the connection goes: the mock socket is reference counted and
    dies with it, so a server thread still running would touch freed memory }
  if Assigned(FServerThread) then
  begin
    FServerThread.WaitFor;
    FreeAndNil(FServerThread);
  end;

  FJs.Free;      // does not own the connection
  FConn.Free;
end;

procedure TJetStreamContextTests.OpenAndHandshake;
begin
  FConn.Open(nil, nil);
  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + JS_INFO_JSON);

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.CONNECT),
    'the client never sent CONNECT in response to INFO');
  FSocket.ClearClientData;
end;

procedure TJetStreamContextTests.CaptureRequest(const AProc: TProc);
begin
  try
    AProc();
  except
    on E: EJetStreamTimeout do ;   // expected: nobody is playing the server
  end;
end;

procedure TJetStreamContextTests.ReplyWith(const AJson: string);
var
  LSocket: TNatsMockSocket;
begin
  LSocket := FSocket;

  FServerThread := TThread.CreateAnonymousThread(
    procedure
    var
      LLines, LParts: TArray<string>;
    begin
      { The context blocks the test thread, so the reply has to come from here -
        and only once the inbox SUB is on the wire, or it would be delivered to
        a sid that does not exist yet and dropped }
      if not LSocket.WaitForClientText(NatsConstants.Protocol.SUB + ' ' +
           NatsConstants.INBOX_PREFIX) then
        Exit;

      // first line written by RequestSync is "SUB <inbox> <sid>"
      LLines := LSocket.ClientText.Split([NatsConstants.CR_LF]);
      LParts := LLines[0].Split([NatsConstants.SPC]);

      LSocket.ServerSend(Format('MSG %s %s %d'#13#10'%s'#13#10,
        [LParts[1], LParts[2], Length(TEncoding.UTF8.GetBytes(AJson)), AJson]));
    end);

  FServerThread.FreeOnTerminate := False;   // TearDown joins and frees it
  FServerThread.Start;
end;

function TJetStreamContextTests.RequestSubject: string;
var
  LLine: string;
begin
  Result := '';
  for LLine in FSocket.ClientText.Split([NatsConstants.CR_LF]) do
    if LLine.StartsWith(NatsConstants.Protocol.PUB + ' ') then
      Exit(LLine.Split([NatsConstants.SPC])[1]);
end;

function TJetStreamContextTests.RequestBody: string;
var
  LLines: TArray<string>;
  LIndex: Integer;
begin
  Result := '';
  LLines := FSocket.ClientText.Split([NatsConstants.CR_LF]);
  for LIndex := 0 to High(LLines) - 1 do
    if LLines[LIndex].StartsWith(NatsConstants.Protocol.PUB + ' ') then
      Exit(LLines[LIndex + 1]);   // the payload is the line after the control line
end;

procedure TJetStreamContextTests.StreamInfo_UsesTheApiSubject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.StreamInfo('ORDERS');
    end);

  Assert.AreEqual('$JS.API.STREAM.INFO.ORDERS', RequestSubject);
end;

procedure TJetStreamContextTests.Domain_MovesTheApiUnderTheDomainPrefix;
begin
  OpenAndHandshake;

  FJs.Free;
  FJs := TJetStreamContext.Create(FConn, 'hub');
  FJs.Timeout := API_TIMEOUT;

  CaptureRequest(
    procedure
    begin
      FJs.StreamInfo('ORDERS');
    end);

  { A domain is how a leaf node reaches the hub's JetStream instead of its own,
    and it changes the subject rather than the payload }
  Assert.AreEqual('$JS.hub.API.STREAM.INFO.ORDERS', RequestSubject);
end;

procedure TJetStreamContextTests.AddStream_SendsTheConfigAsTheBody;
var
  LConfig: TJetStreamStreamConfig;
begin
  OpenAndHandshake;

  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := 'ORDERS';
  LConfig.Subjects := ['orders.*'];

  CaptureRequest(
    procedure
    begin
      FJs.AddStream(LConfig);
    end);

  Assert.AreEqual('$JS.API.STREAM.CREATE.ORDERS', RequestSubject);
  Assert.IsTrue(RequestBody.Contains('"name":"ORDERS"'),
    'the config is the request body, got: ' + RequestBody);
  Assert.IsTrue(RequestBody.Contains('"subjects":["orders.*"]'),
    'got: ' + RequestBody);
end;

procedure TJetStreamContextTests.AddConsumer_Durable_UsesTheNamedSubject;
var
  LConfig: TJetStreamConsumerConfig;
begin
  OpenAndHandshake;

  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := 'workers';

  CaptureRequest(
    procedure
    begin
      FJs.AddConsumer('ORDERS', LConfig);
    end);

  Assert.AreEqual('$JS.API.CONSUMER.CREATE.ORDERS.workers', RequestSubject);
end;

procedure TJetStreamContextTests.AddConsumer_Ephemeral_UsesTheUnnamedSubject;
var
  LConfig: TJetStreamConsumerConfig;
begin
  OpenAndHandshake;

  // no durable name and no name: the server picks one, and the subject carries
  // no name token at all - a different endpoint, not a blank token
  LConfig := Default(TJetStreamConsumerConfig);

  CaptureRequest(
    procedure
    begin
      FJs.AddConsumer('ORDERS', LConfig);
    end);

  Assert.AreEqual('$JS.API.CONSUMER.CREATE.ORDERS', RequestSubject);
end;

procedure TJetStreamContextTests.AddConsumer_SendsStreamNameInTheBodyToo;
var
  LConfig: TJetStreamConsumerConfig;
begin
  OpenAndHandshake;

  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := 'workers';

  CaptureRequest(
    procedure
    begin
      FJs.AddConsumer('ORDERS', LConfig);
    end);

  { The stream is named in the subject AND in the body - the config alone is
    not a valid CONSUMER.CREATE request }
  Assert.IsTrue(RequestBody.Contains('"stream_name":"ORDERS"'), 'got: ' + RequestBody);
  Assert.IsTrue(RequestBody.Contains('"config":'), 'got: ' + RequestBody);
end;

procedure TJetStreamContextTests.ListStreams_SendsTheOffset;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.ListStreams(256);
    end);

  Assert.AreEqual('$JS.API.STREAM.LIST', RequestSubject);
  Assert.IsTrue(RequestBody.Contains('"offset":256'),
    'paging is driven by the request body, got: ' + RequestBody);
end;

procedure TJetStreamContextTests.StreamName_WithADot_Raises;
begin
  OpenAndHandshake;

  { A dot would add a token to the API subject: "ORDERS.EVIL" turns
    STREAM.INFO.<stream> into an entirely different endpoint rather than
    failing, so this has to be refused at the call site }
  Assert.WillRaise(
    procedure
    begin
      FJs.StreamInfo('ORDERS.EVIL');
    end,
    ENatsException);

  Assert.AreEqual('', FSocket.ClientText, 'nothing may reach the wire');
end;

procedure TJetStreamContextTests.StreamName_Empty_Raises;
begin
  OpenAndHandshake;

  Assert.WillRaise(
    procedure
    begin
      FJs.StreamInfo('');
    end,
    ENatsException);
end;

procedure TJetStreamContextTests.ConsumerName_WithAWildcard_Raises;
begin
  OpenAndHandshake;

  Assert.WillRaise(
    procedure
    begin
      FJs.ConsumerInfo('ORDERS', 'work*');
    end,
    ENatsException);
end;

procedure TJetStreamContextTests.StreamInfo_ParsesTheResponse;
var
  LInfo: TJetStreamStreamInfo;
begin
  OpenAndHandshake;
  ReplyWith(STREAM_INFO_JSON);

  LInfo := FJs.StreamInfo('ORDERS');

  Assert.AreEqual('ORDERS', LInfo.Config.Name);
  Assert.AreEqual(UInt64(3), LInfo.State.Messages);
  Assert.AreEqual(1, LInfo.State.ConsumerCount);
end;

procedure TJetStreamContextTests.DeleteStream_ReturnsSuccess;
begin
  OpenAndHandshake;
  ReplyWith('{"type":"io.nats.jetstream.api.v1.stream_delete_response","success":true}');

  Assert.IsTrue(FJs.DeleteStream('ORDERS'));
  Assert.AreEqual('$JS.API.STREAM.DELETE.ORDERS', RequestSubject);
end;

procedure TJetStreamContextTests.PurgeStream_ReturnsThePurgedCount;
begin
  OpenAndHandshake;
  ReplyWith('{"type":"io.nats.jetstream.api.v1.stream_purge_response",' +
    '"success":true,"purged":17}');

  Assert.AreEqual(UInt64(17), FJs.PurgeStream('ORDERS'));
end;

procedure TJetStreamContextTests.StreamNames_ReturnsTheNames;
var
  LNames: TArray<string>;
begin
  OpenAndHandshake;
  ReplyWith('{"type":"io.nats.jetstream.api.v1.stream_names_response","total":2,' +
    '"offset":0,"limit":1024,"streams":["ORDERS","EVENTS"]}');

  LNames := FJs.StreamNames;

  // NAMES returns bare strings, not the full objects LIST returns
  Assert.AreEqual(2, Length(LNames));
  Assert.AreEqual('ORDERS', LNames[0]);
  Assert.AreEqual('EVENTS', LNames[1]);
end;

procedure TJetStreamContextTests.AccountInfo_ParsesLimitsAndUsage;
var
  LInfo: TJetStreamAccountInfo;
begin
  OpenAndHandshake;
  ReplyWith('{"type":"io.nats.jetstream.api.v1.account_info_response",' +
    '"memory":0,"storage":4096,"streams":2,"consumers":3,"domain":"hub",' +
    '"api":{"total":42,"errors":1},' +
    '"limits":{"max_memory":-1,"max_storage":1073741824,"max_streams":10,' +
    '"max_consumers":-1}}');

  LInfo := FJs.AccountInfo;

  Assert.AreEqual('$JS.API.INFO', RequestSubject);
  Assert.AreEqual(UInt64(4096), LInfo.Storage);
  Assert.AreEqual(2, LInfo.Streams);
  Assert.AreEqual('hub', LInfo.Domain);
  Assert.AreEqual(UInt64(42), LInfo.Api.Total);
  Assert.AreEqual(Int64(-1), LInfo.Limits.MaxMemory, 'unlimited is -1, not 0');
  Assert.AreEqual(Int64(1073741824), LInfo.Limits.MaxStorage);
end;

procedure TJetStreamContextTests.ApiError_RaisesInsteadOfReturningAnEmptyRecord;
begin
  OpenAndHandshake;
  ReplyWith(ERROR_RESPONSE_JSON);

  { The whole reason ApiRequest is a choke point. An error response carries no
    result, so deserializing it as TJetStreamStreamInfo yields an empty record -
    which reads exactly like a stream that happens to have no messages }
  Assert.WillRaise(
    procedure
    begin
      FJs.StreamInfo('ORDERS');
    end,
    EJetStreamApiError,
    'a server-side error must never come back as a successful empty record');
end;

procedure TJetStreamContextTests.ApiError_CarriesErrCodeAndDescription;
var
  LRaised: Boolean;
begin
  OpenAndHandshake;
  ReplyWith(ERROR_RESPONSE_JSON);

  LRaised := False;
  try
    FJs.StreamInfo('ORDERS');
  except
    on E: EJetStreamApiError do
    begin
      LRaised := True;
      // err_code is the specific failure; code is only the broad class
      Assert.AreEqual(10059, E.ErrCode);
      Assert.AreEqual(404, E.Code);
      Assert.AreEqual('stream not found', E.Error.Description);
    end;
  end;

  Assert.IsTrue(LRaised, 'EJetStreamApiError was never raised');
end;

procedure TJetStreamContextTests.ApiError_NotFound_IsRecognised;
var
  LNotFound: Boolean;
begin
  OpenAndHandshake;
  ReplyWith(ERROR_RESPONSE_JSON);

  LNotFound := False;
  try
    FJs.StreamInfo('ORDERS');
  except
    on E: EJetStreamApiError do
      LNotFound := E.IsNotFound;
  end;

  // "create it if it isn't there" is the commonest reason to catch this
  Assert.IsTrue(LNotFound);
end;

procedure TJetStreamContextTests.NoReply_RaisesTimeout;
begin
  OpenAndHandshake;

  { Distinct from an API error on purpose: silence usually means JetStream is
    not enabled at all, which is a different problem from a rejected request }
  Assert.WillRaise(
    procedure
    begin
      FJs.StreamInfo('ORDERS');
    end,
    EJetStreamTimeout);
end;

initialization
  TDUnitX.RegisterTestFixture(TJetStreamMetadataTests);
  TDUnitX.RegisterTestFixture(TJetStreamEntityTests);
  TDUnitX.RegisterTestFixture(TJetStreamContextTests);

end.
