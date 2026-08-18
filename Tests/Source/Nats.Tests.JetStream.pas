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
  System.Diagnostics, System.NetEncoding, System.Generics.Collections,

  DUnitX.TestFramework,

  Nats.Consts,
  Nats.Classes,
  Nats.Connection,
  Nats.Exceptions,
  Nats.JetStream.Consts,
  Nats.JetStream.Client,
  Nats.JetStream.Entities,
  Nats.JetStream.Message,
  Nats.JetStream.KV,
  Nats.JetStream.ObjectStore,

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
    procedure ConsumerConfig_UnsetAckPolicy_IsOmitted;
    [Test]
    procedure ConsumerConfig_AckPolicy_RoundTripsThroughTheNullable;
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
    /// <summary>
    ///   As ReplyWith, but the answer is a header status line and no body -
    ///   which is what a status message always is
    /// </summary>
    procedure ReplyWithStatus(AStatus: Integer; const ADescription: string);
    /// <summary>
    ///   Plays the server for a PULL batch: waits for the inbox SUB, delivers
    ///   one message per payload with a real $JS.ACK reply-to, then closes the
    ///   batch with AStatus - which is how the server always ends one
    /// </summary>
    procedure ReplyWithBatch(const APayloads: TArray<string>; AStatus: Integer;
      const ADescription: string; ALeadingStatus: Integer = 0);
    /// The subject of the PUB the client wrote
    function RequestSubject: string;
    /// The body of the PUB the client wrote
    function RequestBody: string;
    /// The whole control line the client wrote for AVerb, or '' if it wrote none
    function ControlLine(const AVerb: string): string;
    /// The header block of the HPUB the client wrote, blank line excluded
    function RequestHeaders: string;
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
    [Test]
    procedure EmptyReply_Raises;
    [Test]
    procedure StatusReply_RaisesInsteadOfAnEmptyRecord;

    { publishing with an ack - Phase 3 }

    [Test]
    procedure Publish_WithoutOptions_UsesPub;
    [Test]
    procedure Publish_WithOptions_UsesHpub;
    [Test]
    procedure Publish_SendsThePayloadAndAnInboxReplyTo;
    [Test]
    procedure Publish_ParsesThePubAck;
    [Test]
    procedure Publish_Duplicate_IsReported;
    [Test]
    procedure Publish_MsgId_IsOnTheWire;
    [Test]
    procedure Publish_ExpectedLastSeqZero_IsOnTheWire;
    [Test]
    procedure Publish_FailedExpectation_RaisesApiError;
    [Test]
    procedure Publish_NoStream_RaisesTimeoutNotSilence;
    [Test]
    procedure PublishBytes_SendsTheBytesUnchanged;

    { pull consumption - Phase 4 }

    [Test]
    procedure Fetch_AsksTheMsgNextSubject;
    [Test]
    procedure Fetch_SendsTheBatchSize;
    [Test]
    procedure Fetch_ExpiryIsShorterThanTheCallersWait;
    [Test]
    procedure Fetch_CollectsTheWholeBatch;
    [Test]
    procedure Fetch_MessagesCarryTheirMetadata;
    [Test]
    procedure Fetch_StatusClosesTheBatchEarly;
    [Test]
    procedure Fetch_IdleHeartbeat_DoesNotCloseTheBatch;
    [Test]
    procedure Fetch_NothingThere_ReturnsAnEmptyArray;
    [Test]
    procedure Fetch_UnsubscribesTheInbox;
    [Test]
    procedure Fetch_EmptyBatch_Raises;
    [Test]
    procedure FetchNoWait_SetsNoWaitAndNoExpiry;
    [Test]
    procedure Next_ReturnsTheFirstMessage;
    [Test]
    procedure Next_NothingThere_ReturnsFalse;
    [Test]
    procedure Fetch_ConnectionClosedMidWait_RaisesInsteadOfTimingOut;

    { push consumption - Phase 4 }

    [Test]
    procedure SubscribePush_SubscribesToTheDeliverSubject;
    [Test]
    procedure SubscribePush_PullConsumer_Raises;
    [Test]
    procedure SubscribePush_DeliversWrappedMessages;
    [Test]
    procedure SubscribePush_FlowControl_IsAnswered;
    [Test]
    procedure SubscribePush_StatusNeverReachesTheHandler;
  end;

  /// <summary>
  ///   The message wrapper: what a delivered message can answer for itself.
  ///   Needs a connection, because acking means publishing
  /// </summary>
  [TestFixture]
  TJetStreamMsgTests = class
  private
    FConn: TNatsConnection;
    FSocket: TNatsMockSocket;

    procedure OpenAndHandshake;
    /// A delivered message with AAckSubject as its reply-to
    function Delivery(const AAckSubject: string): TNatsArgsMSG;
    /// What the client wrote to the ack subject, payload included
    function AckPayload: string;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    { what can be wrapped at all }

    [Test]
    procedure TryWrap_JetStreamDelivery_Succeeds;
    [Test]
    procedure TryWrap_OrdinaryCoreReply_Fails;
    [Test]
    procedure TryWrap_StatusMessage_Fails;
    [Test]
    procedure TryWrap_NoReplyTo_Fails;
    [Test]
    procedure Create_NonJetStreamMessage_Raises;

    { the message itself }

    [Test]
    procedure Msg_ExposesPayloadSubjectAndMetadata;

    { acking }

    [Test]
    procedure Ack_PublishesPlusAckToTheAckSubject;
    [Test]
    procedure Nak_PublishesMinusNak;
    [Test]
    procedure Nak_WithDelay_CarriesNanoseconds;
    [Test]
    procedure Term_PublishesPlusTerm;
    [Test]
    procedure InProgress_PublishesPlusWpi;
    [Test]
    procedure Ack_MarksTheMessageAcknowledged;
    [Test]
    procedure Ack_Twice_Raises;
    [Test]
    procedure Nak_AfterAck_Raises;
    [Test]
    procedure InProgress_DoesNotSettleTheMessage;
    [Test]
    procedure InProgress_MayRepeatAndStillBeAcked;
  end;

  /// <summary>
  ///   Key/Value. Everything here is one request/reply, so it fits the mock -
  ///   the multi-exchange operations (Keys, History) are proved live instead
  /// </summary>
  [TestFixture]
  TJetStreamKVTests = class
  private
    FConn: TNatsConnection;
    FSocket: TNatsMockSocket;
    FJs: TJetStreamContext;
    FKV: TJetStreamKV;
    FServerThread: TThread;

    procedure OpenAndHandshake;
    procedure CaptureRequest(const AProc: TProc);
    procedure ReplyWith(const AJson: string);
    function RequestSubject: string;
    function RequestBody: string;
    /// The header block of the HPUB the client wrote
    function RequestHeaders: string;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    { names, which become stream names and subjects }

    [Test]
    procedure Bucket_DerivesTheStreamName;
    [Test]
    procedure CheckBucket_Empty_Raises;
    [Test]
    procedure CheckBucket_WithADot_Raises;
    [Test]
    procedure CheckKey_Empty_Raises;
    [Test]
    procedure CheckKey_WithAWildcard_Raises;
    [Test]
    procedure CheckKey_LeadingOrTrailingDot_Raises;
    [Test]
    procedure CheckKey_DotsInTheMiddle_AreAllowed;

    { a bucket is a stream }

    [Test]
    procedure CreateBucket_UsesTheKvNameAndSubject;
    [Test]
    procedure CreateBucket_HistoryIsMaxMsgsPerSubject;
    [Test]
    procedure CreateBucket_SetsTheFourSettingsThatMakeItAKvStore;
    [Test]
    procedure CreateBucket_HistoryBeyondTheCeiling_Raises;
    [Test]
    procedure ListBuckets_StripsTheStreamPrefix;

    { writing }

    [Test]
    procedure Put_PublishesToTheKeySubject;
    [Test]
    procedure Put_ReturnsTheRevisionFromThePubAck;
    [Test]
    procedure PutIfAbsent_ExpectsSubjectSequenceZero;
    [Test]
    procedure Update_ExpectsTheRevisionGiven;
    [Test]
    procedure PutIfAbsent_KeyExists_RaisesKvError;
    [Test]
    procedure PutIfAbsent_MissingBucket_PropagatesTheApiError;
    [Test]
    procedure Update_LostTheRace_RaisesKvError;
    [Test]
    procedure Update_MissingBucket_PropagatesTheApiError;
    [Test]
    procedure Delete_WritesATombstoneRatherThanRemovingAnything;
    [Test]
    procedure Purge_AddsTheRollupHeader;

    { reading }

    [Test]
    procedure Get_AsksForTheLastMessageOnTheKeySubject;
    [Test]
    procedure Get_DecodesTheStoredValue;
    [Test]
    procedure Get_MissingKey_ReportsNotFound;
    [Test]
    procedure Get_Tombstone_ReportsNotFoundAndNoValue;
    [Test]
    procedure Get_MissingBucket_PropagatesTheApiError;
    [Test]
    procedure GetRevision_AsksBySequence;
    [Test]
    procedure GetRevision_OfAnotherKey_ReportsNotFound;
  end;

  /// <summary>
  ///   Object Store. The chunking and the digest are proved live - they take
  ///   many exchanges - so what is here is the naming, the bucket layout and
  ///   the metadata round trip
  /// </summary>
  [TestFixture]
  TJetStreamObjectStoreTests = class
  private
    FConn: TNatsConnection;
    FSocket: TNatsMockSocket;
    FJs: TJetStreamContext;
    FOs: TJetStreamObjectStore;
    FServerThread: TThread;

    procedure OpenAndHandshake;
    procedure CaptureRequest(const AProc: TProc);
    procedure ReplyWith(const AJson: string);
    function RequestSubject: string;
    function RequestBody: string;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    { base64url, which every object name goes through }

    [Test]
    procedure Encode_HasNoPadding;
    [Test]
    procedure Encode_UsesTheUrlSafeAlphabet;
    [Test]
    procedure Encode_LongInput_HasNoLineBreaks;

    { names and layout }

    [Test]
    procedure Bucket_DerivesTheStreamName;
    [Test]
    procedure CheckName_Empty_Raises;
    [Test]
    procedure CheckName_AllowsSpacesAndSlashes;
    [Test]
    procedure CreateBucket_CapturesBothSubjectSpaces;
    [Test]
    procedure CreateBucket_AllowsRollupForTheMetadata;
    [Test]
    procedure CreateBucket_SetsNoPerSubjectLimit;
    [Test]
    procedure ChunkSize_DefaultsAndCanBeOverridden;

    { metadata }

    [Test]
    procedure Info_AsksForTheLastMessageOnTheEncodedMetaSubject;
    [Test]
    procedure Info_DecodesTheStoredMetadata;
    [Test]
    procedure Info_MissingObject_ReportsNotFound;
    [Test]
    procedure Info_Deleted_ReportsNotFound;
    [Test]
    procedure Info_MissingBucket_PropagatesTheApiError;
  end;

  /// <summary>
  ///   TJetStreamPubOptions on its own. Pure: it only builds headers, so none
  ///   of this needs a connection
  /// </summary>
  [TestFixture]
  TJetStreamPubOptionsTests = class
  public
    [Test]
    procedure Empty_HasNoHeaders;
    [Test]
    procedure MsgId_UsesTheSpecHeaderName;
    [Test]
    procedure Expectations_UseTheSpecHeaderNames;
    [Test]
    procedure ExpectedLastSeq_Zero_IsStillEmitted;
    [Test]
    procedure SameOptionTwice_ReplacesRatherThanDuplicates;
    [Test]
    procedure Chaining_KeepsEveryOption;
    [Test]
    procedure BranchingFromAnOptionsValue_DoesNotShareState;
    [Test]
    procedure BranchingAndReplacingAnOption_DoesNotShareState;
    [Test]
    procedure CustomHeader_IsAppended;
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
  /// <summary>
  ///   Deliberately LONG, for the fetch tests that assert the wait was cut
  ///   short. The margin between "the status ended it" and "the whole wait
  ///   elapsed" is the assertion, so it has to be wide enough to be unambiguous
  /// </summary>
  LONG_WAIT = 3000;
  /// <summary>
  ///   How long ReplyWithBatch pauses after a leading status before sending any
  ///   data. Long enough that a fetch which wrongly ended on the status is back
  ///   with nothing before the message is even sent
  /// </summary>
  HEARTBEAT_GAP = 300;

  JS_INFO_JSON =
    '{"server_id":"NDHJZQZ4YQXQ","server_name":"nats-1","version":"2.10.11",' +
    '"proto":1,"host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,' +
    '"jetstream":true,"client_id":7,"client_ip":"127.0.0.1"}';

  { what comes back instead of a result when the call fails }
  ERROR_RESPONSE_JSON =
    '{"type":"io.nats.jetstream.api.v1.stream_info_response",' +
    '"error":{"code":404,"err_code":10059,"description":"stream not found"}}';

  { publishing - captured from nats-server 2.10 }

  PUB_ACK_JSON =
    '{"stream":"ORDERS","seq":42,"domain":"hub"}';

  /// Same Nats-Msg-Id inside the duplicate window: stored once, acked twice
  PUB_ACK_DUPLICATE_JSON =
    '{"stream":"ORDERS","seq":7,"duplicate":true}';

  /// Nats-Expected-Last-Sequence did not hold
  PUB_ACK_ERROR_JSON =
    '{"type":"io.nats.jetstream.api.v1.pub_ack_response",' +
    '"error":{"code":400,"err_code":10071,"description":"wrong last sequence: 5"}}';

  { consuming }

  /// The same consumer as CONSUMER_INFO_JSON but PUSH: it has a deliver_subject
  CONSUMER_INFO_PUSH_JSON =
    '{"type":"io.nats.jetstream.api.v1.consumer_info_response",' +
    '"stream_name":"ORDERS","name":"pushers","created":"2023-11-14T22:13:25Z",' +
    '"config":{"durable_name":"pushers","name":"pushers","deliver_policy":"all",' +
    '"ack_policy":"explicit","deliver_subject":"deliver.pushers",' +
    '"max_deliver":-1,"replay_policy":"instant"},' +
    '"delivered":{"consumer_seq":0,"stream_seq":0},' +
    '"ack_floor":{"consumer_seq":0,"stream_seq":0},' +
    '"num_pending":0,"push_bound":true}';

  /// A well-formed V2 ack subject for stream ORDERS, consumer workers, seq 42
  ACK_SUBJECT =
    '$JS.ACK.hub.ACCHASH.ORDERS.workers.1.42.7.1700000000123456789.0.rnd01';

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

procedure TJetStreamEntityTests.ConsumerConfig_UnsetAckPolicy_IsOmitted;
var
  LConfig: TJetStreamConsumerConfig;
  LObj: TJSONObject;
begin
  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := 'workers';

  LObj := JsonOf(TJetStreamJSON.ToJSON<TJetStreamConsumerConfig>(LConfig));
  try
    { The fix this pins: a consumer whose ack policy the caller never set must
      NOT arrive as "ack_policy":"none". Neon emits plain enums
      unconditionally, and "none" is neither the server's default (explicit)
      nor accepted for a PULL consumer - so an unset policy is omitted and the
      server applies its own default }
    Assert.IsNull(LObj.GetValue('ack_policy'),
      'an unset ack policy must be omitted, not sent as "none"');
  finally
    LObj.Free;
  end;
end;

procedure TJetStreamEntityTests.ConsumerConfig_AckPolicy_RoundTripsThroughTheNullable;
var
  LConfig: TJetStreamConsumerConfig;
  LJson: string;
begin
  { An explicitly set policy still travels under the same wire name, in both
    directions - None included, which is the one value that used to leak out
    of a Default config }
  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := 'workers';
  LConfig.AckPolicy := TJetStreamAckPolicy.None;

  LJson := TJetStreamJSON.ToJSON<TJetStreamConsumerConfig>(LConfig);
  Assert.IsTrue(LJson.Contains('"ack_policy":"none"'),
    'explicitly setting None must still emit it: ' + LJson);

  LConfig := TJetStreamJSON.FromJSON<TJetStreamConsumerConfig>(LJson);
  Assert.IsTrue(LConfig.AckPolicy = TJetStreamAckPolicy.None,
    'None must survive a round trip');
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

procedure TJetStreamContextTests.ReplyWithStatus(AStatus: Integer; const ADescription: string);
var
  LSocket: TNatsMockSocket;
  LBlock: string;
begin
  LSocket := FSocket;
  LBlock := Format('%s %d %s'#13#10,
    [NatsConstants.CLIENT_HEADER_VERSION, AStatus, ADescription]);

  FServerThread := TThread.CreateAnonymousThread(
    procedure
    var
      LLines, LParts: TArray<string>;
      LLen: Integer;
    begin
      if not LSocket.WaitForClientText(NatsConstants.Protocol.SUB + ' ' +
           NatsConstants.INBOX_PREFIX) then
        Exit;

      LLines := LSocket.ClientText.Split([NatsConstants.CR_LF]);
      LParts := LLines[0].Split([NatsConstants.SPC]);

      { <#header bytes> counts the blank line that closes the block, and a
        status message has no payload at all - so <#total bytes> is the same }
      LLen := Length(TEncoding.UTF8.GetBytes(LBlock)) + NatsConstants.CR_LF_LEN;

      LSocket.ServerSend(Format('%s %s %s %d %d'#13#10,
        [NatsConstants.Protocol.HMSG, LParts[1], LParts[2], LLen, LLen]) +
        LBlock + #13#10 + #13#10);
    end);

  FServerThread.FreeOnTerminate := False;
  FServerThread.Start;
end;

/// <summary>
///   One HMSG carrying nothing but a status line, ready to hand to ServerSend.
///   Unit level rather than nested: an anonymous method cannot capture a local
///   function, and every caller here is inside one
/// </summary>
function StatusFrame(const AInbox, ASid: string; ACode: Integer;
  const AText: string): string;
var
  LBlock: string;
  LLen: Integer;
begin
  LBlock := Format('%s %d %s'#13#10,
    [NatsConstants.CLIENT_HEADER_VERSION, ACode, AText]);

  { <#header bytes> counts the blank line that closes the block, and a status
    message has no payload at all - so <#total bytes> is the same }
  LLen := Length(TEncoding.UTF8.GetBytes(LBlock)) + NatsConstants.CR_LF_LEN;

  Result := Format('%s %s %s %d %d'#13#10,
    [NatsConstants.Protocol.HMSG, AInbox, ASid, LLen, LLen]) +
    LBlock + #13#10 + #13#10;
end;

procedure TJetStreamContextTests.ReplyWithBatch(const APayloads: TArray<string>;
  AStatus: Integer; const ADescription: string; ALeadingStatus: Integer);
var
  LSocket: TNatsMockSocket;
  LPayloads: TArray<string>;
  LStatus, LLeading: Integer;
  LDescription: string;
begin
  LSocket := FSocket;
  LPayloads := APayloads;
  LStatus := AStatus;
  LLeading := ALeadingStatus;
  LDescription := ADescription;

  FServerThread := TThread.CreateAnonymousThread(
    procedure
    var
      LLines, LParts: TArray<string>;
      LInbox, LSid, LAckSubject: string;
      LIndex: Integer;
    begin
      if not LSocket.WaitForClientText(NatsConstants.Protocol.SUB + ' ' +
           NatsConstants.INBOX_PREFIX) then
        Exit;

      LLines := LSocket.ClientText.Split([NatsConstants.CR_LF]);
      LParts := LLines[0].Split([NatsConstants.SPC]);
      LInbox := LParts[1];
      LSid := LParts[2];

      { Sent BEFORE any data, and then a real pause. Both halves are needed to
        tell a status that closes the batch from one that is swallowed: without
        the pause the message follows so closely that the batch fills either
        way, and the test cannot fail. The pause IS the experiment }
      if LLeading > 0 then
      begin
        LSocket.ServerSend(StatusFrame(LInbox, LSid, LLeading, 'Idle Heartbeat'));
        Sleep(HEARTBEAT_GAP);
      end;

      for LIndex := 0 to High(LPayloads) do
      begin
        { Every delivery carries a $JS.ACK reply-to - that IS the metadata, and
          without one the message is not a JetStream delivery at all }
        LAckSubject := Format(
          '$JS.ACK.hub.ACCHASH.ORDERS.workers.1.%d.%d.1700000000123456789.%d.rnd',
          [LIndex + 1, LIndex + 1, High(LPayloads) - LIndex]);

        LSocket.ServerSend(Format('%s %s %s %s %d'#13#10'%s'#13#10,
          [NatsConstants.Protocol.MSG, LInbox, LSid, LAckSubject,
           Length(TEncoding.UTF8.GetBytes(LPayloads[LIndex])), LPayloads[LIndex]]));
      end;

      { The server always closes a batch out with a status, even a full one }
      if LStatus > 0 then
        LSocket.ServerSend(StatusFrame(LInbox, LSid, LStatus, LDescription));
    end);

  FServerThread.FreeOnTerminate := False;
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

function TJetStreamContextTests.ControlLine(const AVerb: string): string;
var
  LLine: string;
begin
  Result := '';
  for LLine in FSocket.ClientText.Split([NatsConstants.CR_LF]) do
    if LLine.StartsWith(AVerb + NatsConstants.SPC) then
      Exit(LLine);
end;

function TJetStreamContextTests.RequestHeaders: string;
var
  LLines: TArray<string>;
  LIndex, LNext: Integer;
begin
  Result := '';
  LLines := FSocket.ClientText.Split([NatsConstants.CR_LF]);

  for LIndex := 0 to High(LLines) do
    if LLines[LIndex].StartsWith(NatsConstants.Protocol.HPUB + NatsConstants.SPC) then
    begin
      { the block runs from the line after the control line to the blank line
        that closes it }
      for LNext := LIndex + 1 to High(LLines) do
      begin
        if LLines[LNext].IsEmpty then
          Exit;
        Result := Result + LLines[LNext] + NatsConstants.CR_LF;
      end;
      Exit;
    end;
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

procedure TJetStreamContextTests.EmptyReply_Raises;
begin
  OpenAndHandshake;
  ReplyWith('');

  { An empty body deserializes into a record of zeroes, which reads exactly
    like a stream whose every field happens to be a default. Better to say so }
  Assert.WillRaise(
    procedure
    begin
      FJs.StreamInfo('ORDERS');
    end,
    ENatsException);
end;

procedure TJetStreamContextTests.StatusReply_RaisesInsteadOfAnEmptyRecord;
var
  LRaised: EJetStreamStatusError;
begin
  OpenAndHandshake;
  ReplyWithStatus(NatsConstants.Status.NO_RESPONDERS, 'No Responders');

  LRaised := nil;
  try
    try
      FJs.StreamInfo('ORDERS');
      Assert.Fail('a status line is not a response body and must not be parsed as one');
    except
      on E: EJetStreamStatusError do
        LRaised := EJetStreamStatusError(AcquireExceptionObject);
    end;

    Assert.IsNotNull(LRaised, 'the status must surface as EJetStreamStatusError');
    Assert.AreEqual(NatsConstants.Status.NO_RESPONDERS, LRaised.Status);
    Assert.AreEqual('No Responders', LRaised.Description);
  finally
    LRaised.Free;
  end;
end;

{ publishing with an ack }

procedure TJetStreamContextTests.Publish_WithoutOptions_UsesPub;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.Publish('orders.new', '{"id":1}');
    end);

  { Nothing about a plain JetStream publish needs headers, and HPUB with an
    empty block would only cost bytes against max_payload }
  Assert.AreEqual('orders.new', RequestSubject, 'a publish goes to the subject, not to $JS.API');
  Assert.AreEqual('', ControlLine(NatsConstants.Protocol.HPUB), 'no options means no headers, so PUB');
end;

procedure TJetStreamContextTests.Publish_WithOptions_UsesHpub;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.Publish('orders.new', '{"id":1}', TJetStreamPubOptions.New.WithMsgId('order-1'));
    end);

  Assert.IsTrue(ControlLine(NatsConstants.Protocol.HPUB).StartsWith('HPUB orders.new '),
    'options are headers, and headers mean HPUB, wrote: ' + FSocket.ClientText);
end;

procedure TJetStreamContextTests.Publish_SendsThePayloadAndAnInboxReplyTo;
var
  LParts: TArray<string>;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.Publish('orders.new', '{"id":1}');
    end);

  { PUB <subject> <reply-to> <#bytes>. The reply-to is what makes this a
    JetStream publish rather than a fire-and-forget one }
  LParts := ControlLine(NatsConstants.Protocol.PUB).Split([NatsConstants.SPC]);
  Assert.AreEqual(4, Length(LParts), 'a JetStream publish must carry a reply-to');
  Assert.IsTrue(LParts[2].StartsWith(NatsConstants.INBOX_PREFIX),
    'the reply-to must be an inbox, was: ' + LParts[2]);
  Assert.AreEqual('{"id":1}', RequestBody);
end;

procedure TJetStreamContextTests.Publish_ParsesThePubAck;
var
  LAck: TJetStreamPubAck;
begin
  OpenAndHandshake;
  ReplyWith(PUB_ACK_JSON);

  LAck := FJs.Publish('orders.new', '{"id":1}');

  Assert.AreEqual('ORDERS', LAck.Stream, 'the ack names the stream that captured it');
  Assert.AreEqual(UInt64(42), LAck.Seq);
  Assert.AreEqual('hub', LAck.Domain);
  Assert.IsFalse(LAck.Duplicate);
end;

procedure TJetStreamContextTests.Publish_Duplicate_IsReported;
var
  LAck: TJetStreamPubAck;
begin
  OpenAndHandshake;
  ReplyWith(PUB_ACK_DUPLICATE_JSON);

  LAck := FJs.Publish('orders.new', '{"id":1}',
    TJetStreamPubOptions.New.WithMsgId('order-1'));

  { The publish SUCCEEDED and stored nothing. Without this flag the caller
    cannot tell that from a first-time store, and Seq points at the original }
  Assert.IsTrue(LAck.Duplicate, 'a deduplicated publish must say so');
  Assert.AreEqual(UInt64(7), LAck.Seq, 'Seq points at the message already stored');
end;

procedure TJetStreamContextTests.Publish_MsgId_IsOnTheWire;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.Publish('orders.new', '{"id":1}', TJetStreamPubOptions.New.WithMsgId('order-1'));
    end);

  { The server reads this header by its exact name - a misspelling silently
    turns deduplication off rather than failing }
  Assert.IsTrue(RequestHeaders.Contains('Nats-Msg-Id: order-1'),
    'the dedup id must be on the wire under its spec name, block was: ' + RequestHeaders);
end;

procedure TJetStreamContextTests.Publish_ExpectedLastSeqZero_IsOnTheWire;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.Publish('orders.new', '{"id":1}',
        TJetStreamPubOptions.New.WithExpectedLastSeq(0));
    end);

  { Zero is an assertion that the stream is EMPTY, which is how a caller
    publishes the first message of a sequence and no other. A record of plain
    UInt64s could not tell that apart from "left alone", which is why the
    options are fluent }
  Assert.IsTrue(RequestHeaders.Contains('Nats-Expected-Last-Sequence: 0'),
    'an expectation of zero is still an expectation, block was: ' + RequestHeaders);
end;

procedure TJetStreamContextTests.Publish_FailedExpectation_RaisesApiError;
var
  LRaised: EJetStreamApiError;
begin
  OpenAndHandshake;
  ReplyWith(PUB_ACK_ERROR_JSON);

  LRaised := nil;
  try
    try
      FJs.Publish('orders.new', '{"id":1}',
        TJetStreamPubOptions.New.WithExpectedLastSeq(3));
      Assert.Fail('a rejected publish must raise, not return an ack of zeroes');
    except
      on E: EJetStreamApiError do
        LRaised := EJetStreamApiError(AcquireExceptionObject);
    end;

    Assert.IsNotNull(LRaised);
    Assert.AreEqual(10071, LRaised.ErrCode, 'err_code identifies the failed expectation');
    Assert.AreEqual(400, LRaised.Code);
  finally
    LRaised.Free;
  end;
end;

procedure TJetStreamContextTests.Publish_NoStream_RaisesTimeoutNotSilence;
begin
  OpenAndHandshake;

  { This is the whole point of publishing through JetStream: core NATS would
    have accepted the same publish, dropped it and told nobody. Nothing answers
    a subject no stream captures, so the timeout IS the diagnosis }
  Assert.WillRaise(
    procedure
    begin
      FJs.Publish('nowhere.at.all', 'x');
    end,
    EJetStreamTimeout);
end;

procedure TJetStreamContextTests.PublishBytes_SendsTheBytesUnchanged;
var
  LData, LWire: TBytes;
  LIndex, LPayloadAt: Integer;
  LControl: string;
begin
  OpenAndHandshake;

  { Not valid UTF-8: a byte sequence a string round trip would destroy. That is
    also why this test never touches ClientText - decoding these bytes raises }
  LData := [$00, $FF, $FE, $41];

  CaptureRequest(
    procedure
    begin
      FJs.PublishBytes('orders.blob', LData);
    end);

  LWire := FSocket.ClientBytes;

  LPayloadAt := -1;
  for LIndex := 0 to Length(LWire) - Length(LData) do
    if CompareMem(@LWire[LIndex], @LData[0], Length(LData)) then
    begin
      LPayloadAt := LIndex;
      Break;
    end;

  Assert.IsTrue(LPayloadAt > 0, 'the payload bytes must reach the wire untouched');

  { The control line is ASCII even when the payload is not, so it can be read
    on its own - and it must declare 4, the BYTE count }
  LControl := TEncoding.ASCII.GetString(LWire, 0, LPayloadAt);
  Assert.IsTrue(LControl.Contains(NatsConstants.Protocol.PUB + ' orders.blob '),
    'the publish must go to the subject, wrote: ' + LControl);
  Assert.IsTrue(LControl.TrimRight.EndsWith(' 4'),
    'the declared length must be the byte count, wrote: ' + LControl);
end;

{ pull consumption }

procedure TJetStreamContextTests.Fetch_AsksTheMsgNextSubject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.Fetch('ORDERS', 'workers');
    end);

  Assert.AreEqual('$JS.API.CONSUMER.MSG.NEXT.ORDERS.workers', RequestSubject);
end;

procedure TJetStreamContextTests.Fetch_SendsTheBatchSize;
var
  LBody: TJSONObject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.Fetch('ORDERS', 'workers', 10);
    end);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    Assert.AreEqual(10, LBody.GetValue<Integer>('batch'));
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamContextTests.Fetch_ExpiryIsShorterThanTheCallersWait;
var
  LBody: TJSONObject;
  LExpiresNanos: Int64;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.Fetch('ORDERS', 'workers', 1, 1000);
    end);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    LExpiresNanos := LBody.GetValue<Int64>('expires');

    { The SERVER has to give up first. If it does not, an empty consumer leaves
      the client timing out blind and the request pending against MaxWaiting }
    Assert.IsTrue(LExpiresNanos > 0, 'a batch request must carry an expiry');
    Assert.IsTrue(LExpiresNanos < TJetStreamDuration.FromMillis(1000),
      'the expiry must be shorter than the caller''s own wait');

    { and nanoseconds, not milliseconds - a factor of a million }
    Assert.AreEqual(Int64(TJetStreamDuration.FromMillis(900)), LExpiresNanos);
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamContextTests.Fetch_CollectsTheWholeBatch;
var
  LMsgs: TArray<IJetStreamMsg>;
begin
  OpenAndHandshake;
  ReplyWithBatch(['one', 'two', 'three'], 0, '');

  LMsgs := FJs.Fetch('ORDERS', 'workers', 3);

  Assert.AreEqual(3, Length(LMsgs), 'a full batch must return as soon as it is full');
  Assert.AreEqual('one', LMsgs[0].Payload);
  Assert.AreEqual('three', LMsgs[2].Payload);
end;

procedure TJetStreamContextTests.Fetch_MessagesCarryTheirMetadata;
var
  LMsgs: TArray<IJetStreamMsg>;
begin
  OpenAndHandshake;
  ReplyWithBatch(['one'], 0, '');

  LMsgs := FJs.Fetch('ORDERS', 'workers', 1);

  Assert.AreEqual(1, Length(LMsgs));

  { The reply-to is both the metadata and the ack address, so a fetched message
    that lost it could not be acked either }
  Assert.AreEqual('ORDERS', LMsgs[0].Metadata.Stream);
  Assert.AreEqual('workers', LMsgs[0].Metadata.Consumer);
  Assert.AreEqual(UInt64(1), LMsgs[0].Metadata.StreamSeq);
  Assert.IsTrue(LMsgs[0].AckSubject.StartsWith(JetStreamConstants.Ack.PREFIX));
end;

procedure TJetStreamContextTests.Fetch_StatusClosesTheBatchEarly;
var
  LMsgs: TArray<IJetStreamMsg>;
  LClock: TStopwatch;
begin
  OpenAndHandshake;

  { Two of the three asked for, then the server gives up. This is the normal
    shape of a pull against a nearly empty stream }
  ReplyWithBatch(['one', 'two'], NatsConstants.Status.REQUEST_TIMEOUT, 'Request Timeout');

  LClock := TStopwatch.StartNew;
  LMsgs := FJs.Fetch('ORDERS', 'workers', 3, LONG_WAIT);
  LClock.Stop;

  Assert.AreEqual(2, Length(LMsgs), 'the status must close the batch, keeping what arrived');

  { The count alone proves nothing - a fetch that ignored the status would
    return the same two messages, just LONG_WAIT later. The point of the status
    is that it ends the wait }
  Assert.IsTrue(LClock.ElapsedMilliseconds < LONG_WAIT div 2,
    Format('the status must end the wait, but the fetch took %d ms of %d',
      [LClock.ElapsedMilliseconds, LONG_WAIT]));
end;

procedure TJetStreamContextTests.Fetch_IdleHeartbeat_DoesNotCloseTheBatch;
var
  LMsgs: TArray<IJetStreamMsg>;
begin
  OpenAndHandshake;

  { The heartbeat arrives BEFORE any data, which is the only ordering that can
    tell the two behaviours apart: swallowed, the batch stays open and the
    message that follows is collected; treated like a 404, the fetch is already
    over and comes back empty }
  ReplyWithBatch(['one'], 0, '', NatsConstants.Status.IDLE_HEARTBEAT);

  LMsgs := FJs.Fetch('ORDERS', 'workers', 1, LONG_WAIT);

  Assert.AreEqual(1, Length(LMsgs),
    'a 100 says "still here" - it must not end the batch');
end;

procedure TJetStreamContextTests.Fetch_NothingThere_ReturnsAnEmptyArray;
var
  LMsgs: TArray<IJetStreamMsg>;
  LClock: TStopwatch;
begin
  OpenAndHandshake;
  ReplyWithBatch([], NatsConstants.Status.NO_MESSAGES, 'No Messages');

  LClock := TStopwatch.StartNew;
  LMsgs := FJs.Fetch('ORDERS', 'workers', 5, LONG_WAIT);
  LClock.Stop;

  { An empty consumer is an ordinary thing, not an error. Raising here would
    make every polling loop an exception handler }
  Assert.AreEqual(0, Length(LMsgs));
  Assert.IsTrue(LClock.ElapsedMilliseconds < LONG_WAIT div 2,
    Format('a 404 must end the wait, but the fetch took %d ms of %d',
      [LClock.ElapsedMilliseconds, LONG_WAIT]));
end;

procedure TJetStreamContextTests.Fetch_UnsubscribesTheInbox;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.Fetch('ORDERS', 'workers', 1, 200);
    end);

  { Same defect RequestSync exists to avoid: without this every fetch leaves an
    inbox subscribed here and on the server }
  Assert.IsTrue(FSocket.ClientText.Contains(NatsConstants.Protocol.UNSUB + ' '),
    'a fetch must tear its inbox down on every path, wrote: ' + FSocket.ClientText);
end;

procedure TJetStreamContextTests.Fetch_EmptyBatch_Raises;
begin
  OpenAndHandshake;

  Assert.WillRaise(
    procedure
    begin
      FJs.Fetch('ORDERS', 'workers', 0);
    end,
    ENatsException, 'a batch of zero asks the server for nothing');
end;

procedure TJetStreamContextTests.FetchNoWait_SetsNoWaitAndNoExpiry;
var
  LBody: TJSONObject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FJs.FetchNoWait('ORDERS', 'workers', 5);
    end);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    Assert.IsTrue(LBody.GetValue<Boolean>('no_wait'), 'no_wait must be set');

    { Nothing is held open, so there is nothing to expire }
    Assert.IsNull(LBody.GetValue('expires'), 'a no-wait request needs no expiry');
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamContextTests.Next_ReturnsTheFirstMessage;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  ReplyWithBatch(['only'], 0, '');

  Assert.IsTrue(FJs.Next('ORDERS', 'workers', LMsg));
  Assert.IsNotNull(LMsg);
  Assert.AreEqual('only', LMsg.Payload);
end;

procedure TJetStreamContextTests.Next_NothingThere_ReturnsFalse;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  ReplyWithBatch([], NatsConstants.Status.NO_MESSAGES, 'No Messages');

  { False, not an exception: "nothing waiting" is the expected answer half the
    time in a polling loop }
  Assert.IsFalse(FJs.Next('ORDERS', 'workers', LMsg, 5000));
  Assert.IsNull(LMsg);
end;

procedure TJetStreamContextTests.Fetch_ConnectionClosedMidWait_RaisesInsteadOfTimingOut;
var
  LSocket: TNatsMockSocket;
  LStopwatch: TStopwatch;
begin
  OpenAndHandshake;

  LSocket := FSocket;

  { Plays the server: waits for the fetch's inbox SUB, then kills the socket.
    The reader sees the disconnect, tears the connection down, and the teardown
    must RELEASE the blocked fetch - that is the whole point of this test }
  FServerThread := TThread.CreateAnonymousThread(
    procedure
    begin
      if not LSocket.WaitForClientText(NatsConstants.Protocol.SUB + ' ' +
           NatsConstants.INBOX_PREFIX) then
        Exit;

      LSocket.Close;
    end);
  FServerThread.FreeOnTerminate := False;
  FServerThread.Start;

  LStopwatch := TStopwatch.StartNew;

  { A dead connection is not an empty stream: the fetch must RAISE, and it must
    do so long before its own timeout. Before this fix it sat out the whole
    LONG_WAIT and came back with an empty array that read exactly like a
    healthy consumer with nothing to say }
  Assert.WillRaise(
    procedure
    begin
      FJs.Fetch('ORDERS', 'workers', 10, LONG_WAIT);
    end,
    ENatsException);

  Assert.IsTrue(LStopwatch.ElapsedMilliseconds < (LONG_WAIT div 2),
    'the teardown must release the fetch, not its own timeout - elapsed ' +
    LStopwatch.ElapsedMilliseconds.ToString + ' ms');
end;

{ push consumption }

procedure TJetStreamContextTests.SubscribePush_SubscribesToTheDeliverSubject;
begin
  OpenAndHandshake;
  ReplyWith(CONSUMER_INFO_PUSH_JSON);

  FJs.SubscribePush('ORDERS', 'pushers',
    procedure (const AMsg: IJetStreamMsg)
    begin
    end);

  { The delivery subject is the server's to choose, so it has to be read from
    ConsumerInfo rather than guessed }
  Assert.IsTrue(FSocket.ClientText.Contains(
    NatsConstants.Protocol.SUB + ' deliver.pushers '),
    'the push subscription must go to the consumer''s own deliver subject, wrote: ' +
    FSocket.ClientText);
end;

procedure TJetStreamContextTests.SubscribePush_PullConsumer_Raises;
begin
  OpenAndHandshake;
  ReplyWith(CONSUMER_INFO_JSON);   // a pull consumer: no deliver_subject

  { Silently subscribing to nothing would look like a consumer that never
    receives anything, which is a miserable thing to debug }
  Assert.WillRaise(
    procedure
    begin
      FJs.SubscribePush('ORDERS', 'workers',
        procedure (const AMsg: IJetStreamMsg)
        begin
        end);
    end,
    ENatsException);
end;

procedure TJetStreamContextTests.SubscribePush_DeliversWrappedMessages;
var
  LSid: Integer;
  LSeen: TNatsTestLog;
begin
  OpenAndHandshake;
  ReplyWith(CONSUMER_INFO_PUSH_JSON);

  LSeen := TNatsTestLog.Create;
  try
    LSid := FJs.SubscribePush('ORDERS', 'pushers',
      procedure (const AMsg: IJetStreamMsg)
      begin
        LSeen.AddFmt('%s|%d', [AMsg.Payload, AMsg.Metadata.StreamSeq]);
      end);

    FSocket.ServerSend(Format('%s deliver.pushers %d %s %d'#13#10'%s'#13#10,
      [NatsConstants.Protocol.MSG, LSid, ACK_SUBJECT, 5, 'hello']));

    Assert.IsTrue(WaitForCondition(
      function: Boolean
      begin
        Result := LSeen.Count > 0;
      end),
      'the push delivery never reached the handler');

    Assert.AreEqual('hello|42', LSeen.Item(0));
  finally
    LSeen.Free;
  end;
end;

procedure TJetStreamContextTests.SubscribePush_FlowControl_IsAnswered;
var
  LSid: Integer;
  LBlock: string;
  LLen: Integer;
begin
  OpenAndHandshake;
  ReplyWith(CONSUMER_INFO_PUSH_JSON);

  LSid := FJs.SubscribePush('ORDERS', 'pushers',
    procedure (const AMsg: IJetStreamMsg)
    begin
    end);

  FSocket.ClearClientData;

  { A flow-control request is a status message WITH a reply-to, and the server
    stops sending until it is answered - so ignoring it stalls the consumer }
  LBlock := NatsConstants.CLIENT_HEADER_VERSION + ' 100 FlowControl Request'#13#10;
  LLen := Length(TEncoding.UTF8.GetBytes(LBlock)) + NatsConstants.CR_LF_LEN;

  FSocket.ServerSend(Format('%s deliver.pushers %d $JS.FC.token %d %d'#13#10,
    [NatsConstants.Protocol.HMSG, LSid, LLen, LLen]) + LBlock + #13#10 + #13#10);

  Assert.IsTrue(FSocket.WaitForClientText(
    NatsConstants.Protocol.PUB + ' $JS.FC.token'),
    'a flow-control request must be answered, wrote: ' + FSocket.ClientText);
end;

procedure TJetStreamContextTests.SubscribePush_StatusNeverReachesTheHandler;
var
  LSid: Integer;
  LSeen: TNatsTestLog;
  LBlock: string;
  LLen: Integer;
begin
  OpenAndHandshake;
  ReplyWith(CONSUMER_INFO_PUSH_JSON);

  LSeen := TNatsTestLog.Create;
  try
    LSid := FJs.SubscribePush('ORDERS', 'pushers',
      procedure (const AMsg: IJetStreamMsg)
      begin
        LSeen.AddFmt('%s', [AMsg.Payload]);
      end);

    { An idle heartbeat: no reply-to, no body. Handing it on would give the
      application a phantom empty message with no ack subject }
    LBlock := NatsConstants.CLIENT_HEADER_VERSION + ' 100 Idle Heartbeat'#13#10;
    LLen := Length(TEncoding.UTF8.GetBytes(LBlock)) + NatsConstants.CR_LF_LEN;

    FSocket.ServerSend(Format('%s deliver.pushers %d %d %d'#13#10,
      [NatsConstants.Protocol.HMSG, LSid, LLen, LLen]) + LBlock + #13#10 + #13#10);

    { then a real one, so the assertion is not just "nothing happened yet" }
    FSocket.ServerSend(Format('%s deliver.pushers %d %s %d'#13#10'%s'#13#10,
      [NatsConstants.Protocol.MSG, LSid, ACK_SUBJECT, 4, 'real']));

    Assert.IsTrue(WaitForCondition(
      function: Boolean
      begin
        Result := LSeen.Count > 0;
      end),
      'the real message never arrived');

    Assert.AreEqual(1, LSeen.Count, 'only the real message may reach the handler');
    Assert.AreEqual('real', LSeen.Item(0));
  finally
    LSeen.Free;
  end;
end;

{ TJetStreamMsgTests }

procedure TJetStreamMsgTests.Setup;
begin
  UseMockSocket;

  FConn := TNatsConnection.Create;
  FSocket := TNatsMockSocket.LastInstance;
  Assert.IsNotNull(FSocket, 'the connection did not create a mock socket');

  FConn.Name := 'JsMsgConn';
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, MOCK_TIMEOUT);
end;

procedure TJetStreamMsgTests.TearDown;
begin
  FConn.Free;
end;

procedure TJetStreamMsgTests.OpenAndHandshake;
begin
  FConn.Open(nil, nil);
  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + JS_INFO_JSON);

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.CONNECT),
    'the client never sent CONNECT in response to INFO');
  FSocket.ClearClientData;
end;

function TJetStreamMsgTests.Delivery(const AAckSubject: string): TNatsArgsMSG;
begin
  Result := Default(TNatsArgsMSG);
  Result.Subject := 'orders.new';
  Result.ReplyTo := AAckSubject;
  Result.Payload := '{"id":1}';
  Result.PayloadData := TEncoding.UTF8.GetBytes('{"id":1}');
  Result.PayloadBytes := Length(Result.PayloadData);
end;

function TJetStreamMsgTests.AckPayload: string;
var
  LLines: TArray<string>;
  LIndex: Integer;
begin
  Result := '';
  LLines := FSocket.ClientText.Split([NatsConstants.CR_LF]);

  for LIndex := 0 to High(LLines) - 1 do
    if LLines[LIndex].StartsWith(NatsConstants.Protocol.PUB + ' ' + ACK_SUBJECT) then
      Exit(LLines[LIndex + 1]);
end;

procedure TJetStreamMsgTests.TryWrap_JetStreamDelivery_Succeeds;
var
  LMsg: IJetStreamMsg;
begin
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));
  Assert.IsNotNull(LMsg);
  Assert.AreEqual(ACK_SUBJECT, LMsg.AckSubject);
end;

procedure TJetStreamMsgTests.TryWrap_OrdinaryCoreReply_Fails;
var
  LMsg: IJetStreamMsg;
begin
  { A core request/reply message has an inbox as its reply-to, which is a
    perfectly good subject and an utterly wrong thing to ack to }
  Assert.IsFalse(TJetStreamMsg.TryWrap(FConn, Delivery('_INBOX.abc.1'), LMsg));
  Assert.IsNull(LMsg);
end;

procedure TJetStreamMsgTests.TryWrap_StatusMessage_Fails;
var
  LData: TNatsArgsMSG;
  LMsg: IJetStreamMsg;
begin
  LData := Delivery(ACK_SUBJECT);
  LData.Status := NatsConstants.Status.NO_MESSAGES;

  { Even with a plausible reply-to: a status is control flow with an empty
    body, so wrapping it would hand the application a phantom message }
  Assert.IsFalse(TJetStreamMsg.TryWrap(FConn, LData, LMsg));
end;

procedure TJetStreamMsgTests.TryWrap_NoReplyTo_Fails;
var
  LMsg: IJetStreamMsg;
begin
  Assert.IsFalse(TJetStreamMsg.TryWrap(FConn, Delivery(''), LMsg));
end;

procedure TJetStreamMsgTests.Create_NonJetStreamMessage_Raises;
begin
  { TryWrap reports, the constructor raises - the difference is whether the
    caller already knows this is a JetStream delivery }
  Assert.WillRaise(
    procedure
    begin
      TJetStreamMsg.Create(FConn, Delivery('_INBOX.abc.1'));
    end,
    EJetStreamAckError);
end;

procedure TJetStreamMsgTests.Msg_ExposesPayloadSubjectAndMetadata;
var
  LMsg: IJetStreamMsg;
begin
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  Assert.AreEqual('orders.new', LMsg.Subject, 'the subject is the message''s own, not the ack''s');
  Assert.AreEqual('{"id":1}', LMsg.Payload);
  Assert.AreEqual('ORDERS', LMsg.Metadata.Stream);
  Assert.AreEqual(UInt64(42), LMsg.Metadata.StreamSeq);
  Assert.IsFalse(LMsg.Acknowledged, 'a freshly delivered message is not acked');
end;

procedure TJetStreamMsgTests.Ack_PublishesPlusAckToTheAckSubject;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  LMsg.Ack;

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.PUB + ' ' + ACK_SUBJECT),
    'the ack must go to the message''s own reply-to, wrote: ' + FSocket.ClientText);
  Assert.AreEqual('+ACK', AckPayload);
end;

procedure TJetStreamMsgTests.Nak_PublishesMinusNak;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  LMsg.Nak;

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.PUB + ' ' + ACK_SUBJECT));
  Assert.AreEqual('-NAK', AckPayload);
end;

procedure TJetStreamMsgTests.Nak_WithDelay_CarriesNanoseconds;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  LMsg.Nak(TJetStreamDuration.FromSeconds(30));

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.PUB + ' ' + ACK_SUBJECT));

  { Nanoseconds like every other duration here: 30 seconds is 3e10, and sending
    30000 would ask for a redelivery a million times sooner than meant }
  Assert.AreEqual('-NAK {"delay":30000000000}', AckPayload);
end;

procedure TJetStreamMsgTests.Term_PublishesPlusTerm;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  LMsg.Term;

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.PUB + ' ' + ACK_SUBJECT));
  Assert.AreEqual('+TERM', AckPayload);
end;

procedure TJetStreamMsgTests.InProgress_PublishesPlusWpi;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  LMsg.InProgress;

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.PUB + ' ' + ACK_SUBJECT));
  Assert.AreEqual('+WPI', AckPayload);
end;

procedure TJetStreamMsgTests.Ack_MarksTheMessageAcknowledged;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  LMsg.Ack;

  Assert.IsTrue(LMsg.Acknowledged);
end;

procedure TJetStreamMsgTests.Ack_Twice_Raises;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  LMsg.Ack;

  { The second ack lands on a subject the server has already retired, so it
    does nothing at all - and the code that sent it goes on believing it
    settled something }
  Assert.WillRaise(
    procedure
    begin
      LMsg.Ack;
    end,
    EJetStreamAckError);
end;

procedure TJetStreamMsgTests.Nak_AfterAck_Raises;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  LMsg.Ack;

  { Not just the same call twice: any second settlement is the same mistake }
  Assert.WillRaise(
    procedure
    begin
      LMsg.Nak;
    end,
    EJetStreamAckError);
end;

procedure TJetStreamMsgTests.InProgress_DoesNotSettleTheMessage;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  LMsg.InProgress;

  { It says "not yet", which settles nothing }
  Assert.IsFalse(LMsg.Acknowledged);
end;

procedure TJetStreamMsgTests.InProgress_MayRepeatAndStillBeAcked;
var
  LMsg: IJetStreamMsg;
begin
  OpenAndHandshake;
  Assert.IsTrue(TJetStreamMsg.TryWrap(FConn, Delivery(ACK_SUBJECT), LMsg));

  { The point of +WPI: long work keeps resetting AckWait, then acks once }
  LMsg.InProgress;
  LMsg.InProgress;
  LMsg.InProgress;

  LMsg.Ack;

  Assert.IsTrue(LMsg.Acknowledged);
end;

{ TJetStreamKVTests }

/// <summary>
///   A STREAM.MSG.GET response. Data and Hdrs are base64 on the wire, encoded
///   here rather than written out by hand so the test stays readable
/// </summary>
function StoredMsgJson(const ASubject: string; ASeq: UInt64;
  const AData, AHeaderBlock: string): string;
var
  LFields: string;
begin
  LFields := Format('"subject":"%s","seq":%d,"time":"2023-11-14T22:13:20Z"',
    [ASubject, ASeq]);

  if not AData.IsEmpty then
    LFields := LFields + Format(',"data":"%s"',
      [TNetEncoding.Base64.EncodeBytesToString(TEncoding.UTF8.GetBytes(AData))]);

  if not AHeaderBlock.IsEmpty then
    LFields := LFields + Format(',"hdrs":"%s"',
      [TNetEncoding.Base64.EncodeBytesToString(TEncoding.UTF8.GetBytes(AHeaderBlock))]);

  Result := Format(
    '{"type":"io.nats.jetstream.api.v1.stream_msg_get_response","message":{%s}}',
    [LFields]);
end;

procedure TJetStreamKVTests.Setup;
begin
  UseMockSocket;

  FConn := TNatsConnection.Create;
  FSocket := TNatsMockSocket.LastInstance;
  Assert.IsNotNull(FSocket, 'the connection did not create a mock socket');

  FConn.Name := 'KvTestConn';
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, MOCK_TIMEOUT);

  FJs := TJetStreamContext.Create(FConn);
  FJs.Timeout := API_TIMEOUT;
  FKV := TJetStreamKV.Create(FJs, 'cfg');
end;

procedure TJetStreamKVTests.TearDown;
begin
  if Assigned(FServerThread) then
  begin
    FServerThread.WaitFor;
    FreeAndNil(FServerThread);
  end;

  FKV.Free;
  FJs.Free;
  FConn.Free;
end;

procedure TJetStreamKVTests.OpenAndHandshake;
begin
  FConn.Open(nil, nil);
  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + JS_INFO_JSON);

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.CONNECT),
    'the client never sent CONNECT in response to INFO');
  FSocket.ClearClientData;
end;

procedure TJetStreamKVTests.CaptureRequest(const AProc: TProc);
begin
  try
    AProc();
  except
    on E: EJetStreamTimeout do ;   // expected: nobody is playing the server
  end;
end;

procedure TJetStreamKVTests.ReplyWith(const AJson: string);
var
  LSocket: TNatsMockSocket;
begin
  LSocket := FSocket;

  FServerThread := TThread.CreateAnonymousThread(
    procedure
    var
      LLines, LParts: TArray<string>;
    begin
      if not LSocket.WaitForClientText(NatsConstants.Protocol.SUB + ' ' +
           NatsConstants.INBOX_PREFIX) then
        Exit;

      LLines := LSocket.ClientText.Split([NatsConstants.CR_LF]);
      LParts := LLines[0].Split([NatsConstants.SPC]);

      LSocket.ServerSend(Format('%s %s %s %d'#13#10'%s'#13#10,
        [NatsConstants.Protocol.MSG, LParts[1], LParts[2],
         Length(TEncoding.UTF8.GetBytes(AJson)), AJson]));
    end);

  FServerThread.FreeOnTerminate := False;
  FServerThread.Start;
end;

function TJetStreamKVTests.RequestSubject: string;
var
  LLine: string;
begin
  Result := '';
  for LLine in FSocket.ClientText.Split([NatsConstants.CR_LF]) do
    if LLine.StartsWith(NatsConstants.Protocol.PUB + ' ') or
       LLine.StartsWith(NatsConstants.Protocol.HPUB + ' ') then
      Exit(LLine.Split([NatsConstants.SPC])[1]);
end;

function TJetStreamKVTests.RequestBody: string;
var
  LLines: TArray<string>;
  LIndex: Integer;
begin
  Result := '';
  LLines := FSocket.ClientText.Split([NatsConstants.CR_LF]);

  for LIndex := 0 to High(LLines) - 1 do
    if LLines[LIndex].StartsWith(NatsConstants.Protocol.PUB + ' ') then
      Exit(LLines[LIndex + 1]);
end;

function TJetStreamKVTests.RequestHeaders: string;
var
  LLines: TArray<string>;
  LIndex, LNext: Integer;
begin
  Result := '';
  LLines := FSocket.ClientText.Split([NatsConstants.CR_LF]);

  for LIndex := 0 to High(LLines) do
    if LLines[LIndex].StartsWith(NatsConstants.Protocol.HPUB + NatsConstants.SPC) then
    begin
      for LNext := LIndex + 1 to High(LLines) do
      begin
        if LLines[LNext].IsEmpty then
          Exit;
        Result := Result + LLines[LNext] + NatsConstants.CR_LF;
      end;
      Exit;
    end;
end;

procedure TJetStreamKVTests.Bucket_DerivesTheStreamName;
begin
  { A bucket IS a stream, and this is the whole of that relationship }
  Assert.AreEqual('cfg', FKV.Bucket);
  Assert.AreEqual('KV_cfg', FKV.StreamName);
end;

procedure TJetStreamKVTests.CheckBucket_Empty_Raises;
begin
  Assert.WillRaise(
    procedure
    begin
      TJetStreamKV.CheckBucket('');
    end,
    EJetStreamKVError);
end;

procedure TJetStreamKVTests.CheckBucket_WithADot_Raises;
begin
  { 'my.bucket' would become the stream 'KV_my.bucket', and a dot in a stream
    name addresses a different API endpoint entirely }
  Assert.WillRaise(
    procedure
    begin
      TJetStreamKV.CheckBucket('my.bucket');
    end,
    EJetStreamKVError);
end;

procedure TJetStreamKVTests.CheckKey_Empty_Raises;
begin
  Assert.WillRaise(
    procedure
    begin
      TJetStreamKV.CheckKey('');
    end,
    EJetStreamKVError);
end;

procedure TJetStreamKVTests.CheckKey_WithAWildcard_Raises;
begin
  { '*' in a key would make the publish address a wildcard subject, which is
    not a key at all }
  Assert.WillRaise(
    procedure
    begin
      TJetStreamKV.CheckKey('a.*');
    end,
    EJetStreamKVError);

  Assert.WillRaise(
    procedure
    begin
      TJetStreamKV.CheckKey('a.>');
    end,
    EJetStreamKVError);
end;

procedure TJetStreamKVTests.CheckKey_LeadingOrTrailingDot_Raises;
begin
  { Either one produces an EMPTY subject token, which is a different subject }
  Assert.WillRaise(
    procedure
    begin
      TJetStreamKV.CheckKey('.leading');
    end,
    EJetStreamKVError);

  Assert.WillRaise(
    procedure
    begin
      TJetStreamKV.CheckKey('trailing.');
    end,
    EJetStreamKVError);
end;

procedure TJetStreamKVTests.CheckKey_DotsInTheMiddle_AreAllowed;
begin
  { A key spanning several tokens is legitimate and common - 'app.db.host' -
    so the check must not be a blanket ban on dots }
  TJetStreamKV.CheckKey('app.db.host');
  Assert.Pass('a multi-token key is a valid key');
end;

procedure TJetStreamKVTests.CreateBucket_UsesTheKvNameAndSubject;
var
  LBody: TJSONObject;
  LSubjects: TJSONArray;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      TJetStreamKV.CreateBucket(FJs, 'orders').Free;
    end);

  Assert.AreEqual('$JS.API.STREAM.CREATE.KV_orders', RequestSubject);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    Assert.AreEqual('KV_orders', LBody.GetValue<string>('name'));

    LSubjects := LBody.GetValue<TJSONArray>('subjects');
    Assert.AreEqual(1, LSubjects.Count);
    Assert.AreEqual('$KV.orders.>', LSubjects.Items[0].Value);
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamKVTests.CreateBucket_HistoryIsMaxMsgsPerSubject;
var
  LConfig: TJetStreamKVConfig;
  LBody: TJSONObject;
begin
  OpenAndHandshake;

  LConfig := Default(TJetStreamKVConfig);
  LConfig.Bucket := 'orders';
  LConfig.History := 5;

  CaptureRequest(
    procedure
    begin
      TJetStreamKV.CreateBucket(FJs, LConfig).Free;
    end);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    { Keeping 5 revisions of a key IS keeping 5 messages on its subject -
      there is no separate notion of history anywhere in the server }
    Assert.AreEqual(Int64(5), LBody.GetValue<Int64>('max_msgs_per_subject'));
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamKVTests.CreateBucket_SetsTheFourSettingsThatMakeItAKvStore;
var
  LBody: TJSONObject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      TJetStreamKV.CreateBucket(FJs, 'orders').Free;
    end);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    { Discard New so a full bucket REFUSES a write instead of silently dropping
      somebody else's key - the default, Old, would lose data with no error }
    Assert.AreEqual('new', LBody.GetValue<string>('discard'));
    Assert.IsTrue(LBody.GetValue<Boolean>('deny_delete'), 'deny_delete');
    { without this Purge cannot erase a key's history }
    Assert.IsTrue(LBody.GetValue<Boolean>('allow_rollup_hdrs'), 'allow_rollup_hdrs');
    Assert.IsTrue(LBody.GetValue<Boolean>('allow_direct'), 'allow_direct');
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamKVTests.CreateBucket_HistoryBeyondTheCeiling_Raises;
var
  LConfig: TJetStreamKVConfig;
begin
  LConfig := Default(TJetStreamKVConfig);
  LConfig.Bucket := 'orders';
  LConfig.History := JetStreamConstants.KV.MAX_HISTORY + 1;

  { Caught here rather than at the server, where it comes back as a generic
    stream error that says nothing about history }
  Assert.WillRaise(
    procedure
    begin
      TJetStreamKV.CreateBucket(FJs, LConfig).Free;
    end,
    EJetStreamKVError);
end;

procedure TJetStreamKVTests.ListBuckets_StripsTheStreamPrefix;
var
  LBuckets: TArray<string>;
begin
  OpenAndHandshake;
  ReplyWith('{"type":"io.nats.jetstream.api.v1.stream_names_response",' +
    '"total":3,"offset":0,"limit":256,"streams":["KV_cfg","ORDERS","KV_users"]}');

  LBuckets := TJetStreamKV.ListBuckets(FJs);

  { Every bucket is a stream, but not every stream is a bucket - and the caller
    asked for bucket names, not stream names }
  Assert.AreEqual(2, Length(LBuckets));
  Assert.AreEqual('cfg', LBuckets[0]);
  Assert.AreEqual('users', LBuckets[1]);
end;

procedure TJetStreamKVTests.Put_PublishesToTheKeySubject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FKV.Put('name', 'delphi');
    end);

  { A key is a subject. A put is an ordinary JetStream publish }
  Assert.AreEqual('$KV.cfg.name', RequestSubject);
  Assert.AreEqual('delphi', RequestBody);
end;

procedure TJetStreamKVTests.Put_ReturnsTheRevisionFromThePubAck;
var
  LRevision: UInt64;
begin
  OpenAndHandshake;
  ReplyWith('{"stream":"KV_cfg","seq":7}');

  LRevision := FKV.Put('name', 'delphi');

  { A revision IS the stream sequence the value landed at - there is no
    separate counter }
  Assert.AreEqual(UInt64(7), LRevision);
end;

procedure TJetStreamKVTests.PutIfAbsent_ExpectsSubjectSequenceZero;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FKV.PutIfAbsent('name', 'delphi');
    end);

  { Zero means "nothing has ever been written to this subject", which is
    exactly "this key does not exist". It is also why the publish options had
    to be able to express an expectation OF zero rather than treating it as
    unset - a create would otherwise become an unconditional put }
  Assert.IsTrue(RequestHeaders.Contains('Nats-Expected-Last-Subject-Sequence: 0'),
    'block was: ' + RequestHeaders);
end;

procedure TJetStreamKVTests.Update_ExpectsTheRevisionGiven;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FKV.Update('name', 'delphi', 42);
    end);

  { Compare-and-set, and the compare is the server's to do }
  Assert.IsTrue(RequestHeaders.Contains('Nats-Expected-Last-Subject-Sequence: 42'),
    'block was: ' + RequestHeaders);
end;

procedure TJetStreamKVTests.PutIfAbsent_KeyExists_RaisesKvError;
begin
  OpenAndHandshake;
  { The server's answer to "Nats-Expected-Last-Subject-Sequence: 0" when the
    key already holds a value - the ordinary CAS defeat, which keeps its
    friendly KV error }
  ReplyWith('{"type":"io.nats.jetstream.api.v1.pub_ack_response",' +
    '"error":{"code":400,"err_code":10072,"description":"wrong last subject sequence: 3"}}');

  Assert.WillRaise(
    procedure
    begin
      FKV.PutIfAbsent('name', 'delphi');
    end,
    EJetStreamKVError);
end;

procedure TJetStreamKVTests.PutIfAbsent_MissingBucket_PropagatesTheApiError;
begin
  OpenAndHandshake;
  { A bucket that was never created answers 10059 "stream not found" - and that
    is NOT "the key exists". Before the fix this was rephrased into the exact
    opposite of what happened }
  ReplyWith('{"type":"io.nats.jetstream.api.v1.pub_ack_response",' +
    '"error":{"code":404,"err_code":10059,"description":"stream not found"}}');

  Assert.WillRaise(
    procedure
    begin
      FKV.PutIfAbsent('name', 'delphi');
    end,
    EJetStreamApiError);
end;

procedure TJetStreamKVTests.Update_LostTheRace_RaisesKvError;
begin
  OpenAndHandshake;
  { Newer servers report the failed Nats-Expected-Last-Subject-Sequence under
    10164 rather than 10072 - it must still map to the CAS error, not leak out
    as a raw API error }
  ReplyWith('{"type":"io.nats.jetstream.api.v1.pub_ack_response",' +
    '"error":{"code":400,"err_code":10164,"description":"wrong last subject sequence"}}');

  Assert.WillRaise(
    procedure
    begin
      FKV.Update('name', 'delphi', 7);
    end,
    EJetStreamKVError);
end;

procedure TJetStreamKVTests.Update_MissingBucket_PropagatesTheApiError;
begin
  OpenAndHandshake;
  ReplyWith('{"type":"io.nats.jetstream.api.v1.pub_ack_response",' +
    '"error":{"code":404,"err_code":10059,"description":"stream not found"}}');

  { Same rule as PutIfAbsent: a missing bucket is a real error, not a lost race }
  Assert.WillRaise(
    procedure
    begin
      FKV.Update('name', 'delphi', 7);
    end,
    EJetStreamApiError);
end;

procedure TJetStreamKVTests.Delete_WritesATombstoneRatherThanRemovingAnything;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FKV.Delete('name');
    end);

  { The stream is created with DenyDelete, so a KV delete cannot be the removal
    of a message - it is a marker appended after it, and History still shows
    what the key used to hold }
  Assert.AreEqual('$KV.cfg.name', RequestSubject);
  Assert.IsTrue(RequestHeaders.Contains('KV-Operation: DEL'),
    'block was: ' + RequestHeaders);
  Assert.IsFalse(RequestHeaders.Contains('Nats-Rollup'),
    'a delete keeps the history - only a purge rolls it up');
end;

procedure TJetStreamKVTests.Purge_AddsTheRollupHeader;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      FKV.Purge('name');
    end);

  { Nats-Rollup: sub is what erases the history: it tells the server this
    message REPLACES every earlier one on the subject }
  Assert.IsTrue(RequestHeaders.Contains('KV-Operation: PURGE'),
    'block was: ' + RequestHeaders);
  Assert.IsTrue(RequestHeaders.Contains('Nats-Rollup: sub'),
    'block was: ' + RequestHeaders);
end;

procedure TJetStreamKVTests.Get_AsksForTheLastMessageOnTheKeySubject;
var
  LBody: TJSONObject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    var
      LEntry: TKVEntry;
    begin
      FKV.Get('name', LEntry);
    end);

  Assert.AreEqual('$JS.API.STREAM.MSG.GET.KV_cfg', RequestSubject);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    { "The current value" is "the last message on the subject", and that is the
      entire implementation of a KV read }
    Assert.AreEqual('$KV.cfg.name', LBody.GetValue<string>('last_by_subj'));
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamKVTests.Get_DecodesTheStoredValue;
var
  LEntry: TKVEntry;
begin
  OpenAndHandshake;
  ReplyWith(StoredMsgJson('$KV.cfg.name', 7, 'delphi', ''));

  Assert.IsTrue(FKV.Get('name', LEntry));

  { A stored message comes back base64'd, because JSON cannot carry bytes }
  Assert.AreEqual('delphi', LEntry.ValueString);
  Assert.AreEqual('name', LEntry.Key, 'the key is the subject minus the bucket prefix');
  Assert.AreEqual('cfg', LEntry.Bucket);
  Assert.AreEqual(UInt64(7), LEntry.Revision);
  Assert.IsTrue(LEntry.Operation = TKVOperation.Put);
end;

procedure TJetStreamKVTests.Get_MissingKey_ReportsNotFound;
var
  LEntry: TKVEntry;
begin
  OpenAndHandshake;
  ReplyWith('{"type":"io.nats.jetstream.api.v1.stream_msg_get_response",' +
    '"error":{"code":404,"err_code":10037,"description":"no message found"}}');

  { False, not an exception: asking for a key that was never set is the most
    ordinary thing a key/value store is asked to do }
  Assert.IsFalse(FKV.Get('name', LEntry));
  Assert.AreEqual('', LEntry.ValueString);
end;

procedure TJetStreamKVTests.Get_Tombstone_ReportsNotFoundAndNoValue;
var
  LEntry: TKVEntry;
begin
  OpenAndHandshake;
  ReplyWith(StoredMsgJson('$KV.cfg.name', 8, '',
    'NATS/1.0'#13#10'KV-Operation: DEL'#13#10#13#10));

  { A tombstone IS the last message on the subject, so the read succeeds at the
    stream level and the key is still not set. Reporting it as a hit would hand
    back an empty value indistinguishable from one somebody stored }
  Assert.IsFalse(FKV.Get('name', LEntry), 'a deleted key is not set');
end;

procedure TJetStreamKVTests.Get_MissingBucket_PropagatesTheApiError;
var
  LEntry: TKVEntry;
begin
  OpenAndHandshake;
  { "Stream not found" (10059) also carries HTTP code 404, so discriminating on
    the code would read a missing bucket as "the key is not set". Only 10037 -
    "no message found" - is the key being absent; a bucket that was never
    created (or was deleted) is a real error and must propagate }
  ReplyWith('{"type":"io.nats.jetstream.api.v1.stream_msg_get_response",' +
    '"error":{"code":404,"err_code":10059,"description":"stream not found"}}');

  Assert.WillRaise(
    procedure
    begin
      FKV.Get('name', LEntry);
    end,
    EJetStreamApiError);
end;

procedure TJetStreamKVTests.GetRevision_AsksBySequence;
var
  LBody: TJSONObject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    var
      LEntry: TKVEntry;
    begin
      FKV.GetRevision('name', 7, LEntry);
    end);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    Assert.AreEqual(Int64(7), LBody.GetValue<Int64>('seq'));
    Assert.IsNull(LBody.GetValue('last_by_subj'), 'by sequence, not by subject');
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamKVTests.GetRevision_OfAnotherKey_ReportsNotFound;
var
  LEntry: TKVEntry;
begin
  OpenAndHandshake;
  ReplyWith(StoredMsgJson('$KV.cfg.other', 7, 'not yours', ''));

  { A revision is a STREAM sequence, so it is unique across the whole bucket
    rather than per key. Sequence 7 may well belong to a different key, and
    handing its value back under this key's name would be a silent data leak }
  Assert.IsFalse(FKV.GetRevision('name', 7, LEntry));
end;

{ TJetStreamObjectStoreTests }

procedure TJetStreamObjectStoreTests.Setup;
begin
  UseMockSocket;

  FConn := TNatsConnection.Create;
  FSocket := TNatsMockSocket.LastInstance;
  Assert.IsNotNull(FSocket, 'the connection did not create a mock socket');

  FConn.Name := 'ObjTestConn';
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, MOCK_TIMEOUT);

  FJs := TJetStreamContext.Create(FConn);
  FJs.Timeout := API_TIMEOUT;
  FOs := TJetStreamObjectStore.Create(FJs, 'files');
end;

procedure TJetStreamObjectStoreTests.TearDown;
begin
  if Assigned(FServerThread) then
  begin
    FServerThread.WaitFor;
    FreeAndNil(FServerThread);
  end;

  FOs.Free;
  FJs.Free;
  FConn.Free;
end;

procedure TJetStreamObjectStoreTests.OpenAndHandshake;
begin
  FConn.Open(nil, nil);
  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + JS_INFO_JSON);

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.CONNECT),
    'the client never sent CONNECT in response to INFO');
  FSocket.ClearClientData;
end;

procedure TJetStreamObjectStoreTests.CaptureRequest(const AProc: TProc);
begin
  try
    AProc();
  except
    on E: EJetStreamTimeout do ;
  end;
end;

procedure TJetStreamObjectStoreTests.ReplyWith(const AJson: string);
var
  LSocket: TNatsMockSocket;
begin
  LSocket := FSocket;

  FServerThread := TThread.CreateAnonymousThread(
    procedure
    var
      LLines, LParts: TArray<string>;
    begin
      if not LSocket.WaitForClientText(NatsConstants.Protocol.SUB + ' ' +
           NatsConstants.INBOX_PREFIX) then
        Exit;

      LLines := LSocket.ClientText.Split([NatsConstants.CR_LF]);
      LParts := LLines[0].Split([NatsConstants.SPC]);

      LSocket.ServerSend(Format('%s %s %s %d'#13#10'%s'#13#10,
        [NatsConstants.Protocol.MSG, LParts[1], LParts[2],
         Length(TEncoding.UTF8.GetBytes(AJson)), AJson]));
    end);

  FServerThread.FreeOnTerminate := False;
  FServerThread.Start;
end;

function TJetStreamObjectStoreTests.RequestSubject: string;
var
  LLine: string;
begin
  Result := '';
  for LLine in FSocket.ClientText.Split([NatsConstants.CR_LF]) do
    if LLine.StartsWith(NatsConstants.Protocol.PUB + ' ') or
       LLine.StartsWith(NatsConstants.Protocol.HPUB + ' ') then
      Exit(LLine.Split([NatsConstants.SPC])[1]);
end;

function TJetStreamObjectStoreTests.RequestBody: string;
var
  LLines: TArray<string>;
  LIndex: Integer;
begin
  Result := '';
  LLines := FSocket.ClientText.Split([NatsConstants.CR_LF]);

  for LIndex := 0 to High(LLines) - 1 do
    if LLines[LIndex].StartsWith(NatsConstants.Protocol.PUB + ' ') then
      Exit(LLines[LIndex + 1]);
end;

procedure TJetStreamObjectStoreTests.Encode_HasNoPadding;
begin
  { NATS uses the RAW (unpadded) base64url alphabet. An RTL encoder that pads
    would put '=' in a subject token, which is legal but simply wrong - the
    object would live at a subject no other client looks at }
  Assert.AreEqual('aGVsbG8', TObjectStoreEncoding.Encode('hello'));
  Assert.AreEqual('YQ', TObjectStoreEncoding.Encode('a'), 'two pad chars stripped');
  Assert.AreEqual('YWI', TObjectStoreEncoding.Encode('ab'), 'one pad char stripped');
  Assert.AreEqual('YWJj', TObjectStoreEncoding.Encode('abc'), 'nothing to strip');
end;

procedure TJetStreamObjectStoreTests.Encode_UsesTheUrlSafeAlphabet;
var
  LData: TBytes;
begin
  { These two bytes encode to '+/8=' in plain base64. Neither '+' nor '/' can
    appear in a subject token, so both have to be substituted }
  LData := [$FB, $FF];

  Assert.AreEqual('-_8', TObjectStoreEncoding.Encode(LData));
end;

procedure TJetStreamObjectStoreTests.Encode_LongInput_HasNoLineBreaks;
var
  LEncoded: string;
begin
  { TNetEncoding.Base64 wraps at 76 characters, and a line break in a subject
    would be a protocol violation rather than a wrong subject - it could inject
    a second command }
  LEncoded := TObjectStoreEncoding.Encode(StringOfChar('x', 500));

  Assert.IsFalse(LEncoded.Contains(#13), 'no CR');
  Assert.IsFalse(LEncoded.Contains(#10), 'no LF');
  Assert.IsTrue(Length(LEncoded) > 76, 'the input was long enough to have wrapped');
end;

procedure TJetStreamObjectStoreTests.Bucket_DerivesTheStreamName;
begin
  Assert.AreEqual('files', FOs.Bucket);
  Assert.AreEqual('OBJ_files', FOs.StreamName);
end;

procedure TJetStreamObjectStoreTests.CheckName_Empty_Raises;
begin
  Assert.WillRaise(
    procedure
    begin
      TJetStreamObjectStore.CheckName('');
    end,
    EJetStreamObjectError);
end;

procedure TJetStreamObjectStoreTests.CheckName_AllowsSpacesAndSlashes;
begin
  { An object name is usually a file name, and it is base64url-encoded into the
    subject rather than used raw - so none of this needs rejecting }
  TJetStreamObjectStore.CheckName('reports/2024 Q1.pdf');
  TJetStreamObjectStore.CheckName('a name with * and > in it');
  Assert.Pass('an object name is arbitrary text');
end;

procedure TJetStreamObjectStoreTests.CreateBucket_CapturesBothSubjectSpaces;
var
  LBody: TJSONObject;
  LSubjects: TJSONArray;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      TJetStreamObjectStore.CreateBucket(FJs, 'docs').Free;
    end);

  Assert.AreEqual('$JS.API.STREAM.CREATE.OBJ_docs', RequestSubject);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    LSubjects := LBody.GetValue<TJSONArray>('subjects');

    { Chunks and metadata share one stream, so no retention policy can apply to
      one and not the other and leave an object without its description }
    Assert.AreEqual(2, LSubjects.Count);
    Assert.AreEqual('$O.docs.C.>', LSubjects.Items[0].Value, 'the chunk space');
    Assert.AreEqual('$O.docs.M.>', LSubjects.Items[1].Value, 'the metadata space');
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamObjectStoreTests.CreateBucket_AllowsRollupForTheMetadata;
var
  LBody: TJSONObject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      TJetStreamObjectStore.CreateBucket(FJs, 'docs').Free;
    end);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    { Without this a metadata record cannot replace its predecessor, and every
      version of every object's metadata would pile up forever }
    Assert.IsTrue(LBody.GetValue<Boolean>('allow_rollup_hdrs'), 'allow_rollup_hdrs');
    Assert.AreEqual('new', LBody.GetValue<string>('discard'),
      'a full bucket must refuse a write, not shed another object''s chunks');
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamObjectStoreTests.CreateBucket_SetsNoPerSubjectLimit;
var
  LBody: TJSONObject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    begin
      TJetStreamObjectStore.CreateBucket(FJs, 'docs').Free;
    end);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    { The trap this store shares with none of the others: EVERY chunk of an
      object is on ONE subject, so a max_msgs_per_subject would silently delete
      the beginning of every object large enough to exceed it. A KV bucket sets
      exactly this field, which is what makes it worth pinning here }
    Assert.IsNull(LBody.GetValue('max_msgs_per_subject'),
      'a per-subject limit would truncate every large object');
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamObjectStoreTests.ChunkSize_DefaultsAndCanBeOverridden;
var
  LStore: TJetStreamObjectStore;
begin
  Assert.AreEqual(JetStreamConstants.Obj.DEFAULT_CHUNK_SIZE, FOs.ChunkSize);

  LStore := TJetStreamObjectStore.Create(FJs, 'files', 4096);
  try
    Assert.AreEqual(4096, LStore.ChunkSize);
  finally
    LStore.Free;
  end;
end;

procedure TJetStreamObjectStoreTests.Info_AsksForTheLastMessageOnTheEncodedMetaSubject;
var
  LBody: TJSONObject;
begin
  OpenAndHandshake;

  CaptureRequest(
    procedure
    var
      LInfo: TJetStreamObjectInfo;
    begin
      FOs.Info('reports/q1.pdf', LInfo);
    end);

  Assert.AreEqual('$JS.API.STREAM.MSG.GET.OBJ_files', RequestSubject);

  LBody := TJSONObject.ParseJSONValue(RequestBody) as TJSONObject;
  try
    { The name is ENCODED into the subject: used raw, 'reports/q1.pdf' would be
      a perfectly valid but completely different subject }
    Assert.AreEqual('$O.files.M.' + TObjectStoreEncoding.Encode('reports/q1.pdf'),
      LBody.GetValue<string>('last_by_subj'));
  finally
    LBody.Free;
  end;
end;

procedure TJetStreamObjectStoreTests.Info_DecodesTheStoredMetadata;
var
  LInfo: TJetStreamObjectInfo;
begin
  OpenAndHandshake;
  ReplyWith(StoredMsgJson('$O.files.M.' + TObjectStoreEncoding.Encode('a.txt'), 9,
    '{"name":"a.txt","bucket":"files","nuid":"ABC123","size":2048,"chunks":2,' +
    '"digest":"SHA-256=deadbeef","mtime":"2023-11-14T22:13:20Z","deleted":false,' +
    '"options":{"max_chunk_size":1024}}', ''));

  Assert.IsTrue(FOs.Info('a.txt', LInfo));

  Assert.AreEqual('a.txt', LInfo.Name);
  Assert.AreEqual('files', LInfo.Bucket);
  Assert.AreEqual('ABC123', LInfo.Nuid, 'the nuid is what names the chunk subject');
  Assert.AreEqual(UInt64(2048), LInfo.Size);
  Assert.AreEqual(2, LInfo.Chunks);
  Assert.AreEqual('SHA-256=deadbeef', LInfo.Digest);
  Assert.AreEqual(1024, LInfo.Options.MaxChunkSize);
end;

procedure TJetStreamObjectStoreTests.Info_MissingObject_ReportsNotFound;
var
  LInfo: TJetStreamObjectInfo;
begin
  OpenAndHandshake;
  ReplyWith('{"type":"io.nats.jetstream.api.v1.stream_msg_get_response",' +
    '"error":{"code":404,"err_code":10037,"description":"no message found"}}');

  Assert.IsFalse(FOs.Info('never_stored', LInfo));
end;

procedure TJetStreamObjectStoreTests.Info_Deleted_ReportsNotFound;
var
  LInfo: TJetStreamObjectInfo;
begin
  OpenAndHandshake;
  ReplyWith(StoredMsgJson('$O.files.M.' + TObjectStoreEncoding.Encode('gone.txt'), 9,
    '{"name":"gone.txt","bucket":"files","nuid":"ABC123","size":0,"chunks":0,' +
    '"deleted":true}', ''));

  { A deleted object KEEPS its metadata record - that is how a name that was
    removed stays distinguishable from one that never existed - so the read
    succeeds at the stream level and there is still no object }
  Assert.IsFalse(FOs.Info('gone.txt', LInfo), 'a deleted object is not an object');
end;

procedure TJetStreamObjectStoreTests.Info_MissingBucket_PropagatesTheApiError;
var
  LInfo: TJetStreamObjectInfo;
begin
  OpenAndHandshake;
  { As in the KV case: "stream not found" (10059) is a 404 too, and it must NOT
    read as "no such object" - a bucket that does not exist is a real error }
  ReplyWith('{"type":"io.nats.jetstream.api.v1.stream_msg_get_response",' +
    '"error":{"code":404,"err_code":10059,"description":"stream not found"}}');

  Assert.WillRaise(
    procedure
    begin
      FOs.Info('a.txt', LInfo);
    end,
    EJetStreamApiError);
end;

{ TJetStreamPubOptionsTests }

procedure TJetStreamPubOptionsTests.Empty_HasNoHeaders;
var
  LHeaders: TNatsHeaders;
begin
  { No options must mean no header block at all, so the publish stays a PUB }
  LHeaders := TJetStreamPubOptions.New.Headers;
  Assert.AreEqual(0, LHeaders.Count);
end;

procedure TJetStreamPubOptionsTests.MsgId_UsesTheSpecHeaderName;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := TJetStreamPubOptions.New.WithMsgId('order-1').Headers;

  Assert.AreEqual(1, LHeaders.Count);
  Assert.AreEqual('Nats-Msg-Id', LHeaders[0].Key);
  Assert.AreEqual('order-1', LHeaders[0].Value);
end;

procedure TJetStreamPubOptionsTests.Expectations_UseTheSpecHeaderNames;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := TJetStreamPubOptions.New
    .WithExpectedStream('ORDERS')
    .WithExpectedLastSeq(9)
    .WithExpectedLastSubjectSeq(4)
    .WithExpectedLastMsgId('order-0')
    .Headers;

  { Spelled out rather than read back from the constants: a test that uses the
    same constant as the code cannot catch the constant being wrong }
  Assert.AreEqual('ORDERS', LHeaders.GetHeader('Nats-Expected-Stream'));
  Assert.AreEqual('9', LHeaders.GetHeader('Nats-Expected-Last-Sequence'));
  Assert.AreEqual('4', LHeaders.GetHeader('Nats-Expected-Last-Subject-Sequence'));
  Assert.AreEqual('order-0', LHeaders.GetHeader('Nats-Expected-Last-Msg-Id'));
end;

procedure TJetStreamPubOptionsTests.ExpectedLastSeq_Zero_IsStillEmitted;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := TJetStreamPubOptions.New.WithExpectedLastSeq(0).Headers;

  { The reason this record is fluent at all. Zero asserts the stream is empty,
    so it can never be treated as "unset" }
  Assert.AreEqual(1, LHeaders.Count, 'an expectation of zero must survive');
  Assert.AreEqual('0', LHeaders.GetHeader('Nats-Expected-Last-Sequence'));
end;

procedure TJetStreamPubOptionsTests.SameOptionTwice_ReplacesRatherThanDuplicates;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := TJetStreamPubOptions.New.WithMsgId('first').WithMsgId('second').Headers;

  Assert.AreEqual(1, LHeaders.Count, 'one header, not two');
  Assert.AreEqual('second', LHeaders.GetHeader('Nats-Msg-Id'), 'the last call wins');
end;

procedure TJetStreamPubOptionsTests.Chaining_KeepsEveryOption;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := TJetStreamPubOptions.New
    .WithMsgId('order-1')
    .WithExpectedStream('ORDERS')
    .WithHeader('X-Trace', 'abc')
    .Headers;

  Assert.AreEqual(3, LHeaders.Count);
end;

procedure TJetStreamPubOptionsTests.BranchingFromAnOptionsValue_DoesNotShareState;
var
  LBase, LLeft, LRight: TJetStreamPubOptions;
  LBaseHeaders, LLeftHeaders, LRightHeaders: TNatsHeaders;
begin
  LBase := TJetStreamPubOptions.New.WithExpectedStream('ORDERS');

  LLeft := LBase.WithMsgId('left');
  LRight := LBase.WithMsgId('right');

  LLeftHeaders := LLeft.Headers;
  LRightHeaders := LRight.Headers;
  LBaseHeaders := LBase.Headers;

  { A record wrapping a dynamic array is a value only because SetLength
    uniquifies the array before writing to it. Get that wrong and the two
    branches quietly share one header list }
  Assert.AreEqual('left', LLeftHeaders.GetHeader('Nats-Msg-Id'));
  Assert.AreEqual('right', LRightHeaders.GetHeader('Nats-Msg-Id'));
  Assert.AreEqual(1, LBaseHeaders.Count, 'the base must be untouched by either branch');
end;

procedure TJetStreamPubOptionsTests.BranchingAndReplacingAnOption_DoesNotShareState;
var
  LBase, LLeft, LRight: TJetStreamPubOptions;
  LBaseHeaders, LLeftHeaders, LRightHeaders: TNatsHeaders;
begin
  { The nastier half of the same problem, and the one appending hides. Here the
    branches REPLACE a value the base already carries, and replacing writes
    into the array in place - it never goes near SetLength, so it never
    uniquifies the way appending does }
  LBase := TJetStreamPubOptions.New.WithMsgId('base');

  LLeft := LBase.WithMsgId('left');
  LRight := LBase.WithMsgId('right');

  LLeftHeaders := LLeft.Headers;
  LRightHeaders := LRight.Headers;
  LBaseHeaders := LBase.Headers;

  Assert.AreEqual('left', LLeftHeaders.GetHeader('Nats-Msg-Id'));
  Assert.AreEqual('right', LRightHeaders.GetHeader('Nats-Msg-Id'));
  Assert.AreEqual('base', LBaseHeaders.GetHeader('Nats-Msg-Id'),
    'the base must still say what it said');
end;

procedure TJetStreamPubOptionsTests.CustomHeader_IsAppended;
var
  LHeaders: TNatsHeaders;
begin
  { Unlike the JetStream options, an application header may legitimately
    repeat: NATS headers are multi-value }
  LHeaders := TJetStreamPubOptions.New
    .WithHeader('X-Tag', 'a')
    .WithHeader('X-Tag', 'b')
    .Headers;

  Assert.AreEqual(2, LHeaders.Count);
end;

initialization
  TDUnitX.RegisterTestFixture(TJetStreamMetadataTests);
  TDUnitX.RegisterTestFixture(TJetStreamEntityTests);
  TDUnitX.RegisterTestFixture(TJetStreamContextTests);
  TDUnitX.RegisterTestFixture(TJetStreamPubOptionsTests);
  TDUnitX.RegisterTestFixture(TJetStreamMsgTests);
  TDUnitX.RegisterTestFixture(TJetStreamKVTests);
  TDUnitX.RegisterTestFixture(TJetStreamObjectStoreTests);

end.
