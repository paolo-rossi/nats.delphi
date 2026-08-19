{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Tests.Live;

{******************************************************************************}
{                                                                              }
{  End-to-end tests against a real nats-server on localhost:4222, over the      }
{  Indy socket adapter. Everything here is [Category('Live')]:                  }
{                                                                              }
{      Nats.Tests.Framework.exe --exclude:Live                                  }
{                                                                              }
{  skips the whole file when no server is running.                             }
{                                                                              }
{  Every test uses a NUID-suffixed subject so runs cannot collide with each     }
{  other or with anything else on the server.                                   }
{                                                                              }
{  Tests marked [KNOWN BUG §n] assert correct NATS behaviour and currently      }
{  FAIL. "n" refers to the section in Docs\Core-Protocol-Review.md.            }
{                                                                              }
{******************************************************************************}

interface

uses
  System.SysUtils, System.Classes, System.Diagnostics, System.IOUtils,

  DUnitX.TestFramework,

  Nats.Consts,
  Nats.Classes,
  Nats.Entities,
  Nats.Socket,
  Nats.Socket.Indy,
  Nats.Connection,
  Nats.Nuid,
  Nats.Exceptions,
  Nats.JetStream.Client,
  Nats.JetStream.Consts,
  Nats.JetStream.Entities,
  Nats.JetStream.Message,
  Nats.JetStream.KV,
  Nats.JetStream.ObjectStore,

  Nats.Tests.Mocks;

type
  [TestFixture]
  [Category('Live')]
  TNatsLiveServerTests = class
  private
    FConn: TNatsConnection;
    FMsgLog: TNatsTestLog;
    FServerName: string;
    FServerVersion: string;
    FHandshakeDone: Boolean;
    FSubject: string;
    procedure Connect;
    function ReceivedCount: Integer;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure Handshake_CompletesAgainstARealServer;
    [Test]
    procedure PublishSubscribe_RoundTrip;
    [Test]
    procedure Publish_WithReplyTo_DeliversReplySubject;
    [Test]
    procedure Subscribe_WithQueueGroup_ReceivesMessages;
    [Test]
    procedure Publish_MultiBytePayload_SurvivesTheRoundTrip;
    [Test]
    procedure Publish_LargePayload_SurvivesTheRoundTrip;
    [Test]
    procedure Unsubscribe_StopsDelivery;
    [Test]
    procedure Wildcard_Subscription_ReceivesMatchingSubjects;
    [Test]
    procedure Request_ReceivesTheResponderReply;
    [Test]
    procedure RequestSync_ReceivesTheResponderReply;
    [Test]
    procedure RequestSync_NoResponder_TimesOutAndCleansUp;
    // §5: the whole point of the client-side check is that the connection lives
    [Test]
    procedure Publish_OversizedPayload_IsRefusedAndTheConnectionSurvives;

    // Covers §1, §2, §3, §4 and §19 together: publishing headers and getting
    // them back is only possible if all five are right
    [Test]
    procedure Headers_RoundTripThroughTheServer;
  end;

  /// <summary>
  ///   The JetStream management API against a real server with JetStream
  ///   enabled
  /// </summary>
  /// <remarks>
  ///   These are the tests the mock cannot write. The mock proves the client
  ///   sends what this library THINKS the API expects; only a real server
  ///   proves that is what the API actually expects - a misspelled field is
  ///   accepted and ignored, and comes back as the server's default.
  /// </remarks>
  [TestFixture]
  [Category('Live')]
  TJetStreamLiveTests = class
  private
    FConn: TNatsConnection;
    FJs: TJetStreamContext;
    /// Unique per test, so a crashed run never blocks the next one
    FStream: string;
    procedure Connect;
    /// A memory-backed stream named FStream, capturing FStream.>
    procedure CreateTestStream;
    /// <summary>
    ///   As CreateTestStream but with work-queue retention, where an ACKED
    ///   message is REMOVED - which is what makes an ack observable
    /// </summary>
    procedure CreateWorkQueueStream;
    /// A durable pull consumer on FStream, with explicit acks
    procedure CreatePullConsumer(const AName: string);
    /// Deletes FStream, ignoring "it was not there"
    procedure DropStream;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure AccountInfo_AnswersOnARealServer;
    [Test]
    procedure Stream_CreateInfoNamesDelete;
    // the one that catches a misspelled field: the server echoes the config
    // back, so anything it did not understand returns as ITS default
    [Test]
    procedure StreamConfig_SurvivesTheServerUnchanged;
    /// The F7 fix: a page size smaller than the account must not hide streams
    [Test]
    procedure StreamNames_PagesThroughEverything;
    [Test]
    procedure Stream_CapturesPublishedMessages;
    [Test]
    procedure PurgeStream_EmptiesIt;
    [Test]
    procedure Consumer_CreateInfoNamesDelete;
    [Test]
    procedure StreamInfo_UnknownStream_RaisesNotFound;

    { publish with ack - Phase 3 }

    [Test]
    procedure Publish_ReturnsAnAckWithTheStreamAndSequence;
    /// The one that proves Nats-Msg-Id is spelled the way the server reads it
    [Test]
    procedure Publish_SameMsgIdTwice_IsDeduplicated;
    /// ...and the same for Nats-Expected-Last-Sequence
    [Test]
    procedure Publish_WrongExpectedLastSeq_IsRejected;
    [Test]
    procedure Publish_ExpectedLastSeqZero_HoldsOnlyWhileEmpty;
    [Test]
    procedure Publish_NoStreamForTheSubject_TimesOut;

    { consuming - §4 and Phase 4 }

    [Test]
    procedure Fetch_ReturnsWhatWasPublished;
    [Test]
    procedure Fetch_EmptyConsumer_ComesBackEmptyAndOnTime;
    [Test]
    procedure FetchedMessage_CarriesTheServersOwnMetadata;
    /// The one that proves '+ACK' is the payload the server acts on
    [Test]
    procedure Ack_RemovesTheMessageFromAWorkQueue;
    /// ...and '-NAK'
    [Test]
    procedure Nak_CausesImmediateRedelivery;
    /// ...and '+WPI', which must NOT be taken for an ack
    [Test]
    procedure InProgress_LeavesTheMessageUnacknowledged;
    [Test]
    procedure SubscribePush_ReceivesWhatWasPublished;
  end;

  /// <summary>
  ///   Key/Value against a real server. This is where the multi-exchange
  ///   operations live - Keys and History each drive a throwaway consumer, so
  ///   the mock cannot say much about them
  /// </summary>
  [TestFixture]
  [Category('Live')]
  TJetStreamKVLiveTests = class
  private
    FConn: TNatsConnection;
    FJs: TJetStreamContext;
    FKV: TJetStreamKV;
    FBucket: string;
    procedure Connect;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure Bucket_CreateStatusAndDelete;
    [Test]
    procedure ListBuckets_FindsTheNewBucket;
    [Test]
    procedure PutAndGet_RoundTrip;
    [Test]
    procedure Put_Again_OverwritesAndBumpsTheRevision;
    [Test]
    procedure Get_MissingKey_ReportsNotFound;
    /// The F5 fix: a missing bucket is a 10059, and it must raise, not read as an unset key
    [Test]
    procedure Get_MissingBucket_Raises;
    [Test]
    procedure BinaryValue_SurvivesTheBase64RoundTrip;
    [Test]
    procedure MultiTokenKey_RoundTrips;
    /// The create-only path: proves the expectation of ZERO reaches the server
    [Test]
    procedure PutIfAbsent_SecondTime_Raises;
    /// Compare-and-set
    [Test]
    procedure Update_WithAStaleRevision_Raises;
    [Test]
    procedure Delete_HidesTheKeyButKeepsItsHistory;
    /// The one that proves Nats-Rollup reaches the server
    [Test]
    procedure Purge_ErasesTheHistoryToo;
    [Test]
    procedure Keys_ListsLiveKeysOnly;
    /// A scan must not sit out a pull timeout to discover it has finished
    [Test]
    procedure Keys_DoesNotWaitOutAPullTimeout;
    [Test]
    procedure History_ShowsEveryRevisionOldestFirst;
    [Test]
    procedure History_IsBoundedByTheBucketsHistorySetting;
    /// The F6 fix: Values counts keys, not messages, tombstones included
    [Test]
    procedure Status_Values_CountsKeysNotMessages;
  end;

  /// <summary>
  ///   Object Store against a real server. Everything that makes this store
  ///   different from Key/Value - chunking, ordering, the digest, purging the
  ///   old chunks on replace - only happens across many exchanges, so this is
  ///   where it is proved
  /// </summary>
  [TestFixture]
  [Category('Live')]
  TJetStreamObjectStoreLiveTests = class
  private
    FConn: TNatsConnection;
    FJs: TJetStreamContext;
    FOs: TJetStreamObjectStore;
    FBucket: string;
    procedure Connect;
    /// A memory-backed bucket whose chunks are deliberately tiny
    procedure CreateBucket(AChunkSize: Integer);
    /// ACount bytes with a repeating but non-uniform pattern
    function Pattern(ACount: Integer): TBytes;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure Bucket_CreateStatusAndDelete;
    [Test]
    procedure SmallObject_RoundTrips;
    /// The one that proves chunking, ordering and reassembly
    [Test]
    procedure LargeObject_IsSplitAndPutBackTogether;
    [Test]
    procedure BinaryObject_SurvivesIntact;
    [Test]
    procedure ObjectNameWithSpacesAndSlashes_RoundTrips;
    [Test]
    procedure Get_MissingObject_ReportsNotFound;
    [Test]
    procedure Delete_RemovesTheObjectAndItsBytes;
    /// The one that proves the old chunks are purged rather than left behind
    [Test]
    procedure Put_Again_ReplacesTheObjectAndDropsTheOldChunks;
    [Test]
    procedure List_OmitsDeletedObjects;
    /// A truncated object must be reported, not returned short
    [Test]
    procedure Get_WithItsChunksPurged_Raises;
    /// The F13 fix: a failed read must not leave a stream that looks complete
    [Test]
    procedure Get_FailedRead_LeavesTheDestinationRolledBack;
    [Test]
    procedure PutFileAndGetFile_RoundTrip;
    /// The F4 fix: short reads must not truncate an upload
    [Test]
    procedure Put_PartialReads_StillStoresTheWholeObject;
    /// A stream that says EOF while claiming more must be refused, not stored short
    [Test]
    procedure Put_StreamEndingEarly_Raises;
  end;

implementation

const
  LIVE_HOST = '127.0.0.1';
  LIVE_TIMEOUT = 5000;
  WAIT_MS = 5000;

  { InProgress_LeavesTheMessageUnacknowledged. The margin between the interval
    and AckWait is what keeps the test from going red on a stalled machine:
    every report has to land before the previous window runs out }
  WPI_ACK_WAIT = 2000;
  WPI_INTERVAL = 500;
  /// Enough reports to outlast AckWait several times over
  WPI_SENDS = 6;

  /// <summary>
  ///   Keys_DoesNotWaitOutAPullTimeout. Comfortably under the context's own
  ///   5 s request timeout, which is what a scan would burn per batch if it
  ///   pulled with an expiry instead of no_wait
  /// </summary>
  SCAN_BUDGET_MS = 2000;

var
  GLiveSwitchCount: Integer = 0;

procedure UseIndySocket;
begin
  // see UseMockSocket: the registry default is process-wide
  Inc(GLiveSwitchCount);
  TNatsSocketRegistry.Register<TNatsSocketIndy>(Format('IndyLive#%d', [GLiveSwitchCount]), True);
end;

type
  { Hands out at most ALimit bytes per Read call. A Put that stops on the first
    short read - the defect Put_PartialReads_StillStoresTheWholeObject pins -
    would store only ALimit bytes, with the digest computed over exactly those,
    so a Get would come back short with nothing having raised. Size is honest,
    so the store's own completeness check is satisfied once the whole object
    really has been read. }
  TShortReadStream = class(TBytesStream)
  private
    FLimit: Integer;
  public
    constructor Create(const AData: TBytes; ALimit: Integer);
    function Read(var Buffer; Count: Longint): Longint; override;
  end;

  { Claims more bytes than it will ever hand out: Read returns 0 while
    Position is still short of the declared Size. The store must refuse the
    upload rather than record a digest of the bytes it did manage to read. }
  TShortStream = class(TBytesStream)
  public
    function GetSize: Int64; override;
  end;

constructor TShortReadStream.Create(const AData: TBytes; ALimit: Integer);
begin
  inherited Create(AData);
  FLimit := ALimit;
end;

function TShortReadStream.Read(var Buffer; Count: Longint): Longint;
begin
  if Count > FLimit then
    Count := FLimit;
  Result := inherited Read(Buffer, Count);
end;

function TShortStream.GetSize: Int64;
begin
  Result := inherited GetSize + 1000;
end;

{ TNatsLiveServerTests }

procedure TNatsLiveServerTests.Setup;
begin
  UseIndySocket;

  FMsgLog := TNatsTestLog.Create;
  FHandshakeDone := False;
  FServerName := '';
  FServerVersion := '';
  FSubject := 'nats.delphi.test.' + TNUID.NextNuid;

  FConn := TNatsConnection.Create;
  FConn.Name := 'LiveConn';
  FConn.SetChannel(LIVE_HOST, NatsConstants.DEFAULT_PORT, LIVE_TIMEOUT);
end;

procedure TNatsLiveServerTests.TearDown;
begin
  FConn.Free;
  FMsgLog.Free;
  UseMockSocket;   // leave the registry as the rest of the suite expects it
end;

function TNatsLiveServerTests.ReceivedCount: Integer;
begin
  Result := FMsgLog.Count;
end;

procedure TNatsLiveServerTests.Connect;
begin
  FConn.Open(
    procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
    begin
      FServerName := AInfo.ServerName;
      FServerVersion := AInfo.Version;
      FHandshakeDone := True;
    end);

  { Open only starts the handshake. WaitForReady is what guarantees CONNECT is
    actually on the wire before anything is published over it (§16) - waiting
    on the connect handler is not enough, because that runs *before* CONNECT is
    written.

    There used to be a 250 ms sleep here instead. Before the §7-§12 work these
    tests were flaky without it - 3 consecutive runs gave 9 passed / 2 failed /
    hung - because writes were not serialized (§10) and the resulting -ERR
    tripped the §9 self-join deadlock. }
  Assert.IsTrue(FConn.WaitForReady(WAIT_MS),
    Format('handshake with nats-server at %s:%d did not complete - is one running? %s',
      [LIVE_HOST, NatsConstants.DEFAULT_PORT, FConn.LastError]));

  Assert.IsTrue(FHandshakeDone, 'the connect handler must have run by then');
end;

procedure TNatsLiveServerTests.Handshake_CompletesAgainstARealServer;
begin
  Connect;

  Assert.IsTrue(FConn.Connected, 'the connection must report Connected');
  Assert.IsNotEmpty(FServerName, 'the server must identify itself');
  Assert.IsNotEmpty(FServerVersion, 'the server must report its version');
end;

procedure TNatsLiveServerTests.PublishSubscribe_RoundTrip;
begin
  Connect;

  FConn.Subscribe(FSubject,
    procedure (const AMsg: TNatsArgsMSG)
    begin
      FMsgLog.AddFmt('%s|%s', [AMsg.Subject, AMsg.Payload]);
    end);

  FConn.Publish(FSubject, 'hello from delphi');

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 0;
    end,
    WAIT_MS),
    'the published message never came back');
  Assert.AreEqual(FSubject + '|hello from delphi', FMsgLog.Item(0));
end;

procedure TNatsLiveServerTests.Publish_WithReplyTo_DeliversReplySubject;
begin
  Connect;

  FConn.Subscribe(FSubject,
    procedure (const AMsg: TNatsArgsMSG)
    begin
      FMsgLog.Add(AMsg.ReplyTo);
    end);

  FConn.Publish(FSubject, 'ping', '_INBOX.live.reply');

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 0;
    end,
    WAIT_MS),
    'the message never came back');
  Assert.AreEqual('_INBOX.live.reply', FMsgLog.Item(0));
end;

procedure TNatsLiveServerTests.Subscribe_WithQueueGroup_ReceivesMessages;
begin
  Connect;

  FConn.Subscribe(FSubject, 'workers',
    procedure (const AMsg: TNatsArgsMSG)
    begin
      FMsgLog.Add(AMsg.Payload);
    end);

  FConn.Publish(FSubject, 'queued');

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 0;
    end,
    WAIT_MS),
    'a lone queue group member must receive the message');
  Assert.AreEqual('queued', FMsgLog.Item(0));
end;

procedure TNatsLiveServerTests.Publish_MultiBytePayload_SurvivesTheRoundTrip;
const
  // escaped, not literal: this file has no BOM
  PAYLOAD = 'caff' + #$00E8 + ' ' + #$00E0 + #$00E8 + #$00EC + ' ' + #$20AC;
begin
  Connect;

  FConn.Subscribe(FSubject,
    procedure (const AMsg: TNatsArgsMSG)
    begin
      FMsgLog.Add(AMsg.Payload);
    end);

  FConn.Publish(FSubject, PAYLOAD);

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 0;
    end,
    WAIT_MS),
    'the UTF-8 message never came back');
  Assert.AreEqual(PAYLOAD, FMsgLog.Item(0), 'UTF-8 payloads must survive intact');
end;

procedure TNatsLiveServerTests.Publish_LargePayload_SurvivesTheRoundTrip;
var
  LPayload: string;
begin
  Connect;

  // well under the server's 1 MB max_payload, but far past Indy's default
  // 16 KB MaxLineLength, so this exercises the length-prefixed read path
  LPayload := StringOfChar('x', 200 * 1024);

  FConn.Subscribe(FSubject,
    procedure (const AMsg: TNatsArgsMSG)
    begin
      FMsgLog.Add(Length(AMsg.Payload).ToString);
    end);

  FConn.Publish(FSubject, LPayload);

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 0;
    end,
    WAIT_MS),
    'the large message never came back');
  Assert.AreEqual((200 * 1024).ToString, FMsgLog.Item(0), 'the payload must arrive complete');
end;

procedure TNatsLiveServerTests.Unsubscribe_StopsDelivery;
var
  LSid: Integer;
begin
  Connect;

  LSid := FConn.Subscribe(FSubject,
    procedure (const AMsg: TNatsArgsMSG)
    begin
      FMsgLog.Add(AMsg.Payload);
    end);

  FConn.Publish(FSubject, 'first');
  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 0;
    end,
    WAIT_MS),
    'the first message never came back');

  FConn.Unsubscribe(LSid);
  FConn.Publish(FSubject, 'second');

  // give the server a chance to (wrongly) deliver it
  WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 1;
    end,
    1000);

  Assert.AreEqual(1, ReceivedCount, 'nothing must arrive after UNSUB');
end;

procedure TNatsLiveServerTests.Wildcard_Subscription_ReceivesMatchingSubjects;
begin
  Connect;

  FConn.Subscribe(FSubject + '.>',
    procedure (const AMsg: TNatsArgsMSG)
    begin
      FMsgLog.Add(AMsg.Subject);
    end);

  FConn.Publish(FSubject + '.one.two', 'x');

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 0;
    end,
    WAIT_MS),
    'a ">" subscription must receive matching subjects');
  Assert.AreEqual(FSubject + '.one.two', FMsgLog.Item(0));
end;

procedure TNatsLiveServerTests.Publish_OversizedPayload_IsRefusedAndTheConnectionSurvives;
var
  LTooBig: string;
begin
  Connect;

  Assert.IsTrue(FConn.MaxPayload > 0,
    'a real server always declares max_payload in INFO');
  LTooBig := StringOfChar('x', FConn.MaxPayload + 1);

  FConn.Subscribe(FSubject,
    procedure (const AMsg: TNatsArgsMSG)
    begin
      FMsgLog.AddFmt('%s|%s', [AMsg.Subject, AMsg.Payload]);
    end);

  Assert.WillRaise(
    procedure
    begin
      FConn.Publish(FSubject, LTooBig);
    end,
    ENatsMaxPayloadError,
    'one byte over the server''s own declared limit must be refused');

  { This is what §5 is FOR. Left to the server, an oversized publish is answered
    with -ERR 'Maximum Payload Violation' and the connection is closed, taking
    this subscription down with it. Refusing at the call site costs one
    exception and keeps everything else working }
  Assert.IsTrue(FConn.Connected, 'the connection must survive a refused publish');

  FConn.Publish(FSubject, 'still here');
  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 0;
    end,
    WAIT_MS),
    'the subscription must still be delivering after a refused publish');
  Assert.AreEqual(FSubject + '|still here', FMsgLog.Item(0));
end;

procedure TNatsLiveServerTests.RequestSync_ReceivesTheResponderReply;
var
  LConn: TNatsConnection;
  LReply: TNatsArgsMSG;
begin
  Connect;
  LConn := FConn;

  // responder: echo back on the reply subject
  FConn.Subscribe(FSubject,
    procedure (const AMsg: TNatsArgsMSG)
    begin
      if AMsg.ReplyTo <> '' then
        LConn.Publish(AMsg.ReplyTo, 'pong:' + AMsg.Payload);
    end);

  { Safe even though the responder shares this connection: RequestSync blocks
    the CALLING thread, never the consumer thread that has to deliver both the
    request and the reply, and it holds no lock while waiting }
  Assert.IsTrue(FConn.RequestSync(FSubject, 'ping', LReply, WAIT_MS),
    'the synchronous request never got a reply from a real server');
  Assert.AreEqual('pong:ping', LReply.Payload);

  { By the time RequestSync returns, the inbox is already gone: the consumer
    removes it before invoking the handler that releases the caller }
  Assert.AreEqual(1, Length(FConn.GetSubscriptionList),
    'only the responder subscription should be left, the inbox must not linger');
end;

procedure TNatsLiveServerTests.RequestSync_NoResponder_TimesOutAndCleansUp;
var
  LReply: TNatsArgsMSG;
begin
  Connect;

  // nobody is subscribed to FSubject, so the server has nowhere to route this
  Assert.IsFalse(FConn.RequestSync(FSubject, 'ping', LReply, 300),
    'a request with no responder must time out');
  Assert.AreEqual(0, Length(FConn.GetSubscriptionList),
    'a timed-out request must leave no subscription behind');
end;

procedure TNatsLiveServerTests.Request_ReceivesTheResponderReply;
var
  LConn: TNatsConnection;
begin
  Connect;
  LConn := FConn;

  // responder: echo back on the reply subject
  FConn.Subscribe(FSubject,
    procedure (const AMsg: TNatsArgsMSG)
    begin
      if AMsg.ReplyTo <> '' then
        LConn.Publish(AMsg.ReplyTo, 'pong:' + AMsg.Payload);
    end);

  FConn.Request(FSubject, 'ping',
    procedure (const AMsg: TNatsArgsMSG)
    begin
      FMsgLog.Add(AMsg.Payload);
    end);

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 0;
    end,
    WAIT_MS),
    'the request never got a reply');
  Assert.AreEqual('pong:ping', FMsgLog.Item(0));

  // §6: the inbox subscription must not outlive the reply - only the responder
  // subscription should be left
  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := Length(FConn.GetSubscriptionList) = 1;
    end,
    WAIT_MS),
    Format('the request inbox leaked: %d subscriptions still open',
      [Length(FConn.GetSubscriptionList)]));
end;

procedure TNatsLiveServerTests.Headers_RoundTripThroughTheServer;
var
  LHeaders: TNatsHeaders;
begin
  Connect;

  FConn.Subscribe(FSubject,
    procedure (const AMsg: TNatsArgsMSG)
    var
      LReceived: TNatsHeaders;
    begin
      LReceived := AMsg.Headers;
      FMsgLog.AddFmt('%s|%d|%s', [AMsg.Payload, LReceived.Count, LReceived.GetHeader('X-Test')]);
    end);

  LHeaders := nil;
  LHeaders.Add('X-Test', 'delphi');
  FConn.Publish(FSubject, 'hello', '', LHeaders);

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := ReceivedCount > 0;
    end,
    WAIT_MS),
    'the message with headers never came back');
  Assert.AreEqual('hello|1|delphi', FMsgLog.Item(0), 'headers must survive the round trip');
end;

{ TJetStreamLiveTests }

procedure TJetStreamLiveTests.Setup;
begin
  UseIndySocket;

  { Uppercase and NUID-suffixed: a stream name goes into the API subject
    verbatim, and NUID is base62 so it can never contain a dot }
  FStream := 'DELPHI_TEST_' + TNUID.NextNuid;

  FConn := TNatsConnection.Create;
  FConn.Name := 'JsLiveConn';
  FConn.SetChannel(LIVE_HOST, NatsConstants.DEFAULT_PORT, LIVE_TIMEOUT);

  FJs := TJetStreamContext.Create(FConn);
end;

procedure TJetStreamLiveTests.TearDown;
begin
  { Best effort: a test that never created the stream, or that already deleted
    it, must not fail here }
  if Assigned(FConn) and FConn.Connected then
    DropStream;

  FJs.Free;
  FConn.Free;
end;

procedure TJetStreamLiveTests.Connect;
begin
  FConn.Open(nil);
  Assert.IsTrue(FConn.WaitForReady(WAIT_MS),
    Format('handshake with nats-server at %s:%d did not complete: %s',
      [LIVE_HOST, NatsConstants.DEFAULT_PORT, FConn.LastError]));
end;

procedure TJetStreamLiveTests.DropStream;
begin
  try
    FJs.DeleteStream(FStream);
  except
    on E: EJetStreamApiError do ;   // already gone, or never created
    on E: EJetStreamTimeout do ;    // JetStream not enabled - the test says so
  end;
end;

procedure TJetStreamLiveTests.AccountInfo_AnswersOnARealServer;
var
  LInfo: TJetStreamAccountInfo;
begin
  Connect;

  { The cheapest possible JetStream call, and the one that fails outright if
    JetStream is not enabled - so it doubles as the guard for everything below }
  LInfo := FJs.AccountInfo;

  Assert.IsTrue(LInfo.Limits.MaxMemory <> 0,
    'a real account reports a memory limit, -1 meaning unlimited');
end;

procedure TJetStreamLiveTests.Stream_CreateInfoNamesDelete;
var
  LConfig: TJetStreamStreamConfig;
  LInfo: TJetStreamStreamInfo;
  LNames: TArray<string>;
  LFound: Boolean;
  LName: string;
begin
  Connect;

  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := FStream;
  LConfig.Subjects := [FStream + '.>'];
  LConfig.Storage := TJetStreamStorage.Memory;   // no files left behind

  LInfo := FJs.AddStream(LConfig);
  Assert.AreEqual(FStream, LInfo.Config.Name, 'the server must echo the stream back');

  LInfo := FJs.StreamInfo(FStream);
  Assert.AreEqual(FStream, LInfo.Config.Name);
  Assert.AreEqual(UInt64(0), LInfo.State.Messages, 'a fresh stream is empty');

  LNames := FJs.StreamNames;
  LFound := False;
  for LName in LNames do
    if LName = FStream then
      LFound := True;
  Assert.IsTrue(LFound, 'the new stream must appear in STREAM.NAMES');

  Assert.IsTrue(FJs.DeleteStream(FStream));

  // and it must really be gone
  Assert.WillRaise(
    procedure
    begin
      FJs.StreamInfo(FStream);
    end,
    EJetStreamApiError);
end;

procedure TJetStreamLiveTests.StreamNames_PagesThroughEverything;
const
  EXTRA = 256;   // one more than the server's fixed page size
var
  LConfig: TJetStreamStreamConfig;
  LNames: TArray<string>;
  LName: string;
  LIndex: Integer;
  LCount: Integer;
begin
  Connect;

  { The server fixes the page size at 256 and accepts only offset in the
    request, so the only honest way to force a second page is to own more
    streams than one page holds. 256 extra plus FStream = 257 }
  try
    for LIndex := 1 to EXTRA do
    begin
      LConfig := Default(TJetStreamStreamConfig);
      LConfig.Name := FStream + '_' + LIndex.ToString;
      LConfig.Subjects := [LConfig.Name + '.>'];
      LConfig.Storage := TJetStreamStorage.Memory;
      FJs.AddStream(LConfig);
    end;

    LConfig := Default(TJetStreamStreamConfig);
    LConfig.Name := FStream;
    LConfig.Subjects := [FStream + '.>'];
    LConfig.Storage := TJetStreamStorage.Memory;
    FJs.AddStream(LConfig);

    LNames := FJs.StreamNames;

    { Every one of the 257 must come back, not just the first page of 256.
      Leftover streams from a crashed run only add names beyond these, so it is
      OUR streams that are counted, not the total }
    LCount := 0;
    for LName in LNames do
      if LName.StartsWith(FStream + '_') or (LName = FStream) then
        Inc(LCount);

    Assert.AreEqual(EXTRA + 1, LCount,
      'a stream past the first page must not be silently missing');
  finally
    { FStream itself is dropped by TearDown }
    for LIndex := 1 to EXTRA do
      try
        FJs.DeleteStream(FStream + '_' + LIndex.ToString);
      except
        on E: EJetStreamApiError do ;   // never created, or already gone
      end;
  end;
end;

procedure TJetStreamLiveTests.StreamConfig_SurvivesTheServerUnchanged;
var
  LConfig: TJetStreamStreamConfig;
  LInfo: TJetStreamStreamInfo;
begin
  Connect;

  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := FStream;
  LConfig.Subjects := [FStream + '.>'];
  LConfig.Storage := TJetStreamStorage.Memory;
  LConfig.Retention := TJetStreamRetention.WorkQueue;
  LConfig.Discard := TJetStreamDiscard.New;
  LConfig.MaxMsgs := 500;
  LConfig.MaxMsgsPerSubject := 50;
  LConfig.MaxMsgSize := 4096;
  LConfig.MaxAge := TJetStreamDuration.FromMinutes(30);
  LConfig.DuplicateWindow := TJetStreamDuration.FromSeconds(90);
  LConfig.DenyDelete := True;

  LInfo := FJs.AddStream(LConfig);

  { This is what the mock cannot check. A field whose JSON name the server does
    not recognise is silently ignored, and comes back as the server's own
    default - so every assertion here is really testing that the snake_case
    name Neon derived is the one nats-server reads }
  Assert.IsTrue(LInfo.Config.Retention = TJetStreamRetention.WorkQueue, 'retention');
  Assert.IsTrue(LInfo.Config.Storage = TJetStreamStorage.Memory, 'storage');
  Assert.IsTrue(LInfo.Config.Discard = TJetStreamDiscard.New, 'discard');
  Assert.AreEqual(Int64(500), LInfo.Config.MaxMsgs, 'max_msgs');
  Assert.AreEqual(Int64(50), LInfo.Config.MaxMsgsPerSubject, 'max_msgs_per_subject');
  Assert.AreEqual(4096, LInfo.Config.MaxMsgSize, 'max_msg_size');
  Assert.AreEqual(Int64(TJetStreamDuration.FromMinutes(30)), Int64(LInfo.Config.MaxAge),
    'max_age - and that it is nanoseconds, not millis');
  Assert.AreEqual(Int64(TJetStreamDuration.FromSeconds(90)), Int64(LInfo.Config.DuplicateWindow),
    'duplicate_window');
  Assert.IsTrue(LInfo.Config.DenyDelete, 'deny_delete');
end;

procedure TJetStreamLiveTests.Stream_CapturesPublishedMessages;
var
  LConfig: TJetStreamStreamConfig;
  LConn: TNatsConnection;
  LJs: TJetStreamContext;
  LStream: string;
begin
  Connect;

  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := FStream;
  LConfig.Subjects := [FStream + '.>'];
  LConfig.Storage := TJetStreamStorage.Memory;
  FJs.AddStream(LConfig);

  // an ordinary core publish: the stream captures it because its filter matches
  FConn.Publish(FStream + '.one', 'first');
  FConn.Publish(FStream + '.two', 'second');

  { A core publish has no ack, so the store happens asynchronously from this
    thread's point of view - poll rather than sleep. Phase 3's publish-with-ack
    is what removes this race for real code }
  LJs := FJs;
  LConn := FConn;
  LStream := FStream;
  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := LConn.Connected and (LJs.StreamInfo(LStream).State.Messages >= 2);
    end,
    WAIT_MS),
    'the stream never captured the published messages');
end;

procedure TJetStreamLiveTests.PurgeStream_EmptiesIt;
var
  LConfig: TJetStreamStreamConfig;
  LJs: TJetStreamContext;
  LStream: string;
  LPurged: UInt64;
begin
  Connect;

  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := FStream;
  LConfig.Subjects := [FStream + '.>'];
  LConfig.Storage := TJetStreamStorage.Memory;
  FJs.AddStream(LConfig);

  FConn.Publish(FStream + '.one', 'first');

  LJs := FJs;
  LStream := FStream;
  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := LJs.StreamInfo(LStream).State.Messages >= 1;
    end,
    WAIT_MS),
    'nothing was stored to purge');

  LPurged := FJs.PurgeStream(FStream);

  Assert.IsTrue(LPurged >= 1, 'purge must report how many it removed');
  Assert.AreEqual(UInt64(0), FJs.StreamInfo(FStream).State.Messages,
    'the stream must be empty afterwards, but still exist');
end;

procedure TJetStreamLiveTests.Consumer_CreateInfoNamesDelete;
var
  LStreamCfg: TJetStreamStreamConfig;
  LConsumerCfg: TJetStreamConsumerConfig;
  LInfo: TJetStreamConsumerInfo;
  LNames: TArray<string>;
  LFound: Boolean;
  LName: string;
begin
  Connect;

  LStreamCfg := Default(TJetStreamStreamConfig);
  LStreamCfg.Name := FStream;
  LStreamCfg.Subjects := [FStream + '.>'];
  LStreamCfg.Storage := TJetStreamStorage.Memory;
  FJs.AddStream(LStreamCfg);

  { A durable PULL consumer: DeliverSubject stays empty, which is what makes it
    pull rather than push - and the server rejects an empty one, so this also
    proves the field really was omitted rather than sent blank }
  LConsumerCfg := Default(TJetStreamConsumerConfig);
  LConsumerCfg.DurableName := 'workers';
  LConsumerCfg.AckPolicy := TJetStreamAckPolicy.Explicit;
  LConsumerCfg.AckWait := TJetStreamDuration.FromSeconds(20);
  LConsumerCfg.MaxDeliver := 3;

  LInfo := FJs.AddConsumer(FStream, LConsumerCfg);
  Assert.AreEqual('workers', LInfo.Name);
  Assert.AreEqual(FStream, LInfo.StreamName);
  Assert.IsTrue(LInfo.Config.AckPolicy = TJetStreamAckPolicy.Explicit, 'ack_policy');
  Assert.AreEqual(Int64(TJetStreamDuration.FromSeconds(20)), Int64(LInfo.Config.AckWait),
    'ack_wait must survive as nanoseconds');
  Assert.AreEqual(3, LInfo.Config.MaxDeliver, 'max_deliver');

  LInfo := FJs.ConsumerInfo(FStream, 'workers');
  Assert.AreEqual('workers', LInfo.Name);

  LNames := FJs.ConsumerNames(FStream);
  LFound := False;
  for LName in LNames do
    if LName = 'workers' then
      LFound := True;
  Assert.IsTrue(LFound, 'the consumer must appear in CONSUMER.NAMES');

  Assert.IsTrue(FJs.DeleteConsumer(FStream, 'workers'));
end;

procedure TJetStreamLiveTests.StreamInfo_UnknownStream_RaisesNotFound;
var
  LRaised: Boolean;
begin
  Connect;

  { The error path against the real error table, not one this repo made up }
  LRaised := False;
  try
    FJs.StreamInfo('DELPHI_NO_SUCH_STREAM_' + TNUID.NextNuid);
  except
    on E: EJetStreamApiError do
    begin
      LRaised := True;
      Assert.AreEqual(404, E.Code, 'a missing stream is a 404');
      Assert.IsTrue(E.IsNotFound);
      Assert.IsTrue(E.ErrCode > 0, 'the server always sets a specific err_code');
    end;
  end;

  Assert.IsTrue(LRaised, 'asking for a stream that does not exist must raise');
end;

{ publish with ack }

procedure TJetStreamLiveTests.CreateTestStream;
var
  LConfig: TJetStreamStreamConfig;
begin
  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := FStream;
  LConfig.Subjects := [FStream + '.>'];
  LConfig.Storage := TJetStreamStorage.Memory;   // no files left behind

  FJs.AddStream(LConfig);
end;

procedure TJetStreamLiveTests.Publish_ReturnsAnAckWithTheStreamAndSequence;
var
  LAck: TJetStreamPubAck;
begin
  Connect;
  CreateTestStream;

  LAck := FJs.Publish(FStream + '.one', 'first');

  { The ack is what makes this different from a core publish: no polling, no
    race - the call does not return until the message is stored }
  Assert.AreEqual(FStream, LAck.Stream, 'the ack names the stream that captured it');
  Assert.AreEqual(UInt64(1), LAck.Seq, 'the first message of a fresh stream is seq 1');
  Assert.IsFalse(LAck.Duplicate);

  LAck := FJs.Publish(FStream + '.two', 'second');
  Assert.AreEqual(UInt64(2), LAck.Seq, 'sequences run on');

  Assert.AreEqual(UInt64(2), FJs.StreamInfo(FStream).State.Messages,
    'both messages must already be stored by the time the ack came back');
end;

procedure TJetStreamLiveTests.Publish_SameMsgIdTwice_IsDeduplicated;
var
  LFirst, LSecond: TJetStreamPubAck;
  LMsgId: string;
begin
  Connect;
  CreateTestStream;

  LMsgId := 'delphi-' + TNUID.NextNuid;

  LFirst := FJs.Publish(FStream + '.one', 'first',
    TJetStreamPubOptions.New.WithMsgId(LMsgId));
  LSecond := FJs.Publish(FStream + '.one', 'first again',
    TJetStreamPubOptions.New.WithMsgId(LMsgId));

  { What the mock cannot prove: the server reads Nats-Msg-Id by that exact
    name. Misspell it and both publishes succeed, both are stored, and every
    offline test still passes }
  Assert.IsTrue(LSecond.Duplicate, 'the second publish must be recognised as a duplicate');
  Assert.AreEqual(LFirst.Seq, LSecond.Seq, 'the ack points back at the message already stored');

  Assert.AreEqual(UInt64(1), FJs.StreamInfo(FStream).State.Messages,
    'a duplicate must not be stored a second time');
end;

procedure TJetStreamLiveTests.Publish_WrongExpectedLastSeq_IsRejected;
var
  LRaised: Boolean;
begin
  Connect;
  CreateTestStream;

  FJs.Publish(FStream + '.one', 'first');

  LRaised := False;
  try
    { The stream is at 1, so this expectation cannot hold. As with the msg id,
      a misspelled header would make the server ignore it and ACCEPT the
      publish - the test would then fail here rather than pass quietly }
    FJs.Publish(FStream + '.one', 'second',
      TJetStreamPubOptions.New.WithExpectedLastSeq(99));
  except
    on E: EJetStreamApiError do
    begin
      LRaised := True;
      Assert.IsTrue(E.ErrCode > 0, 'the server sets a specific err_code for a failed expectation');
    end;
  end;

  Assert.IsTrue(LRaised, 'a violated Nats-Expected-Last-Sequence must be rejected');
  Assert.AreEqual(UInt64(1), FJs.StreamInfo(FStream).State.Messages,
    'a rejected publish must not be stored');
end;

procedure TJetStreamLiveTests.Publish_ExpectedLastSeqZero_HoldsOnlyWhileEmpty;
var
  LAck: TJetStreamPubAck;
begin
  Connect;
  CreateTestStream;

  { Zero is a real assertion - "the stream is still empty" - and is why the
    options are fluent rather than a record of numbers where 0 means "unset".
    If it were dropped as unset this first publish would still succeed and the
    second one would too, so only the pair of them proves it went out }
  LAck := FJs.Publish(FStream + '.one', 'first',
    TJetStreamPubOptions.New.WithExpectedLastSeq(0));
  Assert.AreEqual(UInt64(1), LAck.Seq, 'it holds on an empty stream');

  Assert.WillRaise(
    procedure
    begin
      FJs.Publish(FStream + '.one', 'second',
        TJetStreamPubOptions.New.WithExpectedLastSeq(0));
    end,
    EJetStreamApiError,
    'the same expectation must fail once the stream is no longer empty');
end;

procedure TJetStreamLiveTests.Publish_NoStreamForTheSubject_TimesOut;
begin
  Connect;

  { No stream captures this subject, so nothing acks. Core NATS would have
    accepted the publish, dropped it and told nobody - waiting for the ack is
    the only thing that tells the two apart }
  FJs.Timeout := 1000;   // no point sitting out the full default

  Assert.WillRaise(
    procedure
    begin
      FJs.Publish('delphi.no.stream.' + TNUID.NextNuid, 'x');
    end,
    EJetStreamTimeout);
end;

{ consuming }

procedure TJetStreamLiveTests.CreateWorkQueueStream;
var
  LConfig: TJetStreamStreamConfig;
begin
  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := FStream;
  LConfig.Subjects := [FStream + '.>'];
  LConfig.Storage := TJetStreamStorage.Memory;
  LConfig.Retention := TJetStreamRetention.WorkQueue;

  FJs.AddStream(LConfig);
end;

procedure TJetStreamLiveTests.CreatePullConsumer(const AName: string);
var
  LConfig: TJetStreamConsumerConfig;
begin
  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := AName;
  LConfig.AckPolicy := TJetStreamAckPolicy.Explicit;
  LConfig.AckWait := TJetStreamDuration.FromSeconds(30);

  { DeliverSubject left empty is what makes it a PULL consumer }
  FJs.AddConsumer(FStream, LConfig);
end;

procedure TJetStreamLiveTests.Fetch_ReturnsWhatWasPublished;
var
  LMsgs: TArray<IJetStreamMsg>;
begin
  Connect;
  CreateTestStream;
  CreatePullConsumer('workers');

  FJs.Publish(FStream + '.one', 'first');
  FJs.Publish(FStream + '.two', 'second');

  LMsgs := FJs.Fetch(FStream, 'workers', 2, WAIT_MS);

  { Every field name in the batch request has to be right for this to work at
    all: a misspelled "batch" means the server sends its default of one }
  Assert.AreEqual(2, Length(LMsgs), 'both published messages must come back');
  Assert.AreEqual('first', LMsgs[0].Payload);
  Assert.AreEqual('second', LMsgs[1].Payload);
end;

procedure TJetStreamLiveTests.Fetch_EmptyConsumer_ComesBackEmptyAndOnTime;
var
  LMsgs: TArray<IJetStreamMsg>;
  LClock: TStopwatch;
begin
  Connect;
  CreateTestStream;
  CreatePullConsumer('workers');

  LClock := TStopwatch.StartNew;
  LMsgs := FJs.Fetch(FStream, 'workers', 5, 1000);
  LClock.Stop;

  Assert.AreEqual(0, Length(LMsgs), 'an empty consumer is not an error');

  { The expiry we send has to be in NANOSECONDS and shorter than our own wait.
    Send milliseconds by mistake and it is a million times too small, so the
    server gives up instantly; leave it out and the request hangs on and counts
    against MaxWaiting. Both show up here as the wrong elapsed time }
  Assert.IsTrue(LClock.ElapsedMilliseconds > 500,
    Format('the server gave up after only %d ms - is the expiry in the right unit?',
      [LClock.ElapsedMilliseconds]));
  Assert.IsTrue(LClock.ElapsedMilliseconds < 2500,
    Format('the fetch overran its own timeout at %d ms', [LClock.ElapsedMilliseconds]));
end;

procedure TJetStreamLiveTests.FetchedMessage_CarriesTheServersOwnMetadata;
var
  LMsgs: TArray<IJetStreamMsg>;
begin
  Connect;
  CreateTestStream;
  CreatePullConsumer('workers');

  FJs.Publish(FStream + '.one', 'first');

  LMsgs := FJs.Fetch(FStream, 'workers', 1, WAIT_MS);
  Assert.AreEqual(1, Length(LMsgs));

  { The $JS.ACK layout parsed against a subject a real server built, not one
    this repo wrote out by hand }
  Assert.AreEqual(FStream, LMsgs[0].Metadata.Stream);
  Assert.AreEqual('workers', LMsgs[0].Metadata.Consumer);
  Assert.AreEqual(UInt64(1), LMsgs[0].Metadata.StreamSeq);
  Assert.AreEqual(UInt64(1), LMsgs[0].Metadata.NumDelivered, 'delivery counting starts at 1');
  Assert.IsFalse(LMsgs[0].Metadata.IsRedelivery);
  Assert.IsTrue(LMsgs[0].Metadata.TimestampNanos > 0, 'the server stamps every delivery');
end;

procedure TJetStreamLiveTests.Ack_RemovesTheMessageFromAWorkQueue;
var
  LMsgs: TArray<IJetStreamMsg>;
begin
  Connect;
  CreateWorkQueueStream;
  CreatePullConsumer('workers');

  FJs.Publish(FStream + '.one', 'first');
  Assert.AreEqual(UInt64(1), FJs.StreamInfo(FStream).State.Messages);

  LMsgs := FJs.Fetch(FStream, 'workers', 1, WAIT_MS);
  Assert.AreEqual(1, Length(LMsgs));

  LMsgs[0].AckSync(WAIT_MS);

  { A work queue DELETES what has been acknowledged, which is the only way to
    see from the client side that the ack was understood. AckSync means the
    server has recorded it by the time this returns, so there is no poll here }
  Assert.AreEqual(UInt64(0), FJs.StreamInfo(FStream).State.Messages,
    'an acked message must leave a work queue');
end;

procedure TJetStreamLiveTests.Nak_CausesImmediateRedelivery;
var
  LFirst, LSecond: TArray<IJetStreamMsg>;
begin
  Connect;
  CreateTestStream;
  CreatePullConsumer('workers');

  FJs.Publish(FStream + '.one', 'first');

  LFirst := FJs.Fetch(FStream, 'workers', 1, WAIT_MS);
  Assert.AreEqual(1, Length(LFirst));
  Assert.AreEqual(UInt64(1), LFirst[0].Metadata.NumDelivered);

  LFirst[0].Nak;

  { AckWait is 30 seconds, so anything arriving now got there because of the
    NAK and nothing else. nats-server IGNORES an ack payload it does not
    recognise - verified by mutation, it does not fall back to treating it as
    an ack - so a misspelling means no redelivery at all and this comes back
    empty. That is what pins the literal }
  LSecond := FJs.Fetch(FStream, 'workers', 1, WAIT_MS);

  Assert.AreEqual(1, Length(LSecond), 'a NAK must redeliver without waiting out AckWait');
  Assert.AreEqual(UInt64(1), LSecond[0].Metadata.StreamSeq, 'the same message');
  Assert.AreEqual(UInt64(2), LSecond[0].Metadata.NumDelivered, 'and it is the second delivery');
  Assert.IsTrue(LSecond[0].Metadata.IsRedelivery);
end;

procedure TJetStreamLiveTests.InProgress_LeavesTheMessageUnacknowledged;
var
  LConfig: TJetStreamConsumerConfig;
  LFirst, LHeld, LRedelivered: TArray<IJetStreamMsg>;
  LIndex: Integer;
begin
  Connect;
  CreateTestStream;

  { A SHORT AckWait, because the whole test is about outlasting it }
  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := 'workers';
  LConfig.AckPolicy := TJetStreamAckPolicy.Explicit;
  LConfig.AckWait := TJetStreamDuration.FromMillis(WPI_ACK_WAIT);
  FJs.AddConsumer(FStream, LConfig);

  FJs.Publish(FStream + '.one', 'first');

  LFirst := FJs.Fetch(FStream, 'workers', 1, WAIT_MS);
  Assert.AreEqual(1, Length(LFirst));
  Assert.AreEqual(UInt64(1), LFirst[0].Metadata.NumDelivered);

  { The sleeps here are the SUBJECT of the test, not a race being papered over:
    what +WPI does is push AckWait back, and the only way to observe that is to
    hold the message for longer than AckWait and see it not come back.

    Checking NumAckPending instead would prove nothing. nats-server ignores an
    ack payload it does not recognise, so a misspelled +WPI leaves the message
    pending too - the counter reads the same either way, which is exactly how
    an earlier version of this test passed against '+WIP'. }
  for LIndex := 1 to WPI_SENDS do
  begin
    Sleep(WPI_INTERVAL);
    LFirst[0].InProgress;
  end;

  LHeld := FJs.Fetch(FStream, 'workers', 1, 500);
  Assert.AreEqual(0, Length(LHeld),
    Format('+WPI must keep pushing AckWait back - the message was redelivered ' +
      'after %d ms of progress reports', [WPI_SENDS * WPI_INTERVAL]));

  { and once the reports stop, AckWait finally elapses and it does come back -
    without this half, a fetch that was simply broken would pass the assertion
    above }
  LRedelivered := FJs.Fetch(FStream, 'workers', 1, WPI_ACK_WAIT * 3);

  Assert.AreEqual(1, Length(LRedelivered),
    'once the progress reports stop, AckWait must expire and redeliver');
  Assert.AreEqual(UInt64(2), LRedelivered[0].Metadata.NumDelivered);
  Assert.IsFalse(LFirst[0].Acknowledged, '+WPI settles nothing');
end;

procedure TJetStreamLiveTests.SubscribePush_ReceivesWhatWasPublished;
var
  LConsumerCfg: TJetStreamConsumerConfig;
  LSeen: TNatsTestLog;
  LDeliver: string;
begin
  Connect;
  CreateTestStream;

  { A PUSH consumer: setting DeliverSubject is the whole difference }
  LDeliver := 'deliver.' + TNUID.NextNuid;
  LConsumerCfg := Default(TJetStreamConsumerConfig);
  LConsumerCfg.DurableName := 'pushers';
  LConsumerCfg.AckPolicy := TJetStreamAckPolicy.Explicit;
  LConsumerCfg.DeliverSubject := LDeliver;
  FJs.AddConsumer(FStream, LConsumerCfg);

  LSeen := TNatsTestLog.Create;
  try
    FJs.SubscribePush(FStream, 'pushers',
      procedure (const AMsg: IJetStreamMsg)
      begin
        { Runs on the consumer thread, so only thread-safe state here - and
          plain Ack only writes, so it is allowed. AckSync would deadlock }
        LSeen.AddFmt('%s|%d', [AMsg.Payload, AMsg.Metadata.StreamSeq]);
        AMsg.Ack;
      end);

    FJs.Publish(FStream + '.one', 'pushed');

    Assert.IsTrue(WaitForCondition(
      function: Boolean
      begin
        Result := LSeen.Count > 0;
      end,
      WAIT_MS),
      'the push consumer never delivered anything');

    Assert.AreEqual('pushed|1', LSeen.Item(0));
  finally
    LSeen.Free;
  end;
end;

{ TJetStreamKVLiveTests }

/// <summary>
///   A memory-backed bucket config, so a crashed run leaves no files behind.
///   AHistory is how many revisions of each key the bucket keeps
/// </summary>
function KVConfig(const ABucket: string; AHistory: Integer): TJetStreamKVConfig;
begin
  Result := Default(TJetStreamKVConfig);
  Result.Bucket := ABucket;
  Result.History := AHistory;
  Result.Storage := TJetStreamStorage.Memory;
end;

procedure TJetStreamKVLiveTests.Setup;
begin
  UseIndySocket;

  { A bucket name may only hold letters, digits, underscore and hyphen, and
    NUID is base62 - so this can never produce an invalid one }
  FBucket := 'delphi_kv_' + TNUID.NextNuid;

  FConn := TNatsConnection.Create;
  FConn.Name := 'KvLiveConn';
  FConn.SetChannel(LIVE_HOST, NatsConstants.DEFAULT_PORT, LIVE_TIMEOUT);

  FJs := TJetStreamContext.Create(FConn);
  FKV := nil;
end;

procedure TJetStreamKVLiveTests.TearDown;
begin
  if Assigned(FConn) and FConn.Connected then
    try
      TJetStreamKV.DeleteBucket(FJs, FBucket);
    except
      on E: EJetStreamApiError do ;   // never created, or already gone
      on E: EJetStreamTimeout do ;
    end;

  FKV.Free;
  FJs.Free;
  FConn.Free;
end;

procedure TJetStreamKVLiveTests.Connect;
begin
  FConn.Open(nil);
  Assert.IsTrue(FConn.WaitForReady(WAIT_MS),
    Format('handshake with nats-server at %s:%d did not complete: %s',
      [LIVE_HOST, NatsConstants.DEFAULT_PORT, FConn.LastError]));
end;

procedure TJetStreamKVLiveTests.Bucket_CreateStatusAndDelete;
var
  LConfig: TJetStreamKVConfig;
  LStatus: TJetStreamKVStatus;
begin
  Connect;

  LConfig := Default(TJetStreamKVConfig);
  LConfig.Bucket := FBucket;
  LConfig.History := 5;
  LConfig.Storage := TJetStreamStorage.Memory;
  FKV := TJetStreamKV.CreateBucket(FJs, LConfig);

  LStatus := FKV.Status;

  { Every one of these is a plain stream setting read back - which is the whole
    claim this layer makes, that a bucket IS a stream }
  Assert.AreEqual(FBucket, LStatus.Bucket);
  Assert.AreEqual('KV_' + FBucket, LStatus.StreamName);
  Assert.AreEqual(Int64(5), LStatus.History, 'history is max_msgs_per_subject');
  Assert.AreEqual(UInt64(0), LStatus.Values, 'a new bucket holds nothing');

  TJetStreamKV.DeleteBucket(FJs, FBucket);

  Assert.WillRaise(
    procedure
    begin
      FKV.Status;
    end,
    EJetStreamApiError, 'the bucket''s stream must really be gone');
end;

procedure TJetStreamKVLiveTests.ListBuckets_FindsTheNewBucket;
var
  LBuckets: TArray<string>;
  LName: string;
  LFound: Boolean;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 1));

  LBuckets := TJetStreamKV.ListBuckets(FJs);

  LFound := False;
  for LName in LBuckets do
  begin
    if LName = FBucket then
      LFound := True;

    { and every name that came back must be a BUCKET name, never the KV_ stream
      name it was derived from }
    Assert.IsFalse(LName.StartsWith('KV_'),
      'ListBuckets must strip the stream prefix, got: ' + LName);
  end;

  Assert.IsTrue(LFound, 'the new bucket must appear in the list');
end;

procedure TJetStreamKVLiveTests.PutAndGet_RoundTrip;
var
  LRevision: UInt64;
  LEntry: TKVEntry;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 1));

  LRevision := FKV.Put('name', 'delphi');
  Assert.AreEqual(UInt64(1), LRevision, 'the first value in a bucket is revision 1');

  Assert.IsTrue(FKV.Get('name', LEntry));
  Assert.AreEqual('delphi', LEntry.ValueString);
  Assert.AreEqual('name', LEntry.Key);
  Assert.AreEqual(FBucket, LEntry.Bucket);
  Assert.AreEqual(LRevision, LEntry.Revision);
  Assert.IsTrue(LEntry.Operation = TKVOperation.Put);
  Assert.IsFalse(LEntry.IsDelete);

  Assert.AreEqual('delphi', FKV.Get('name'), 'and the convenience form agrees');
end;

procedure TJetStreamKVLiveTests.Put_Again_OverwritesAndBumpsTheRevision;
var
  LFirst, LSecond: UInt64;
  LEntry: TKVEntry;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 1));

  LFirst := FKV.Put('name', 'first');
  LSecond := FKV.Put('name', 'second');

  Assert.IsTrue(LSecond > LFirst, 'a revision is a stream sequence, so it only goes up');

  Assert.IsTrue(FKV.Get('name', LEntry));
  Assert.AreEqual('second', LEntry.ValueString, 'the current value is the LAST message');
  Assert.AreEqual(LSecond, LEntry.Revision);
end;

procedure TJetStreamKVLiveTests.Get_MissingKey_ReportsNotFound;
var
  LEntry: TKVEntry;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 1));

  { The most ordinary question a key/value store is asked, so it must not raise }
  Assert.IsFalse(FKV.Get('never_set', LEntry));
  Assert.AreEqual('fallback', FKV.Get('never_set', 'fallback'));
end;

procedure TJetStreamKVLiveTests.Get_MissingBucket_Raises;
var
  LEntry: TKVEntry;
begin
  Connect;
  { Bind WITHOUT creating the bucket - the constructor makes no round trip, so
    this really is "the bucket does not exist", not a handle on one }
  FKV := TJetStreamKV.Create(FJs, FBucket);

  { The mock pins the discrimination on err_code 10037 vs 10059; this proves a
    real server answers STREAM.MSG.GET on a missing stream with 10059 - and
    that Get therefore raises instead of reading it as an unset key }
  Assert.WillRaise(
    procedure
    begin
      FKV.Get('name', LEntry);
    end,
    EJetStreamApiError);
end;

procedure TJetStreamKVLiveTests.BinaryValue_SurvivesTheBase64RoundTrip;
var
  LValue: TBytes;
  LEntry: TKVEntry;
  LIndex: Integer;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 1));

  { Not valid UTF-8. A stored message comes back base64'd inside JSON, and this
    is what proves that path does not go through a string anywhere }
  LValue := [$00, $FF, $FE, $01, $80, $7F];
  FKV.Put('blob', LValue);

  Assert.IsTrue(FKV.Get('blob', LEntry));
  Assert.AreEqual(Length(LValue), Length(LEntry.Value), 'byte count');

  for LIndex := 0 to High(LValue) do
    Assert.AreEqual(LValue[LIndex], LEntry.Value[LIndex],
      Format('byte %d survived unchanged', [LIndex]));
end;

procedure TJetStreamKVLiveTests.MultiTokenKey_RoundTrips;
var
  LEntry: TKVEntry;
  LKeys: TArray<string>;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 1));

  { A key with dots becomes several subject tokens, which is legitimate - and
    reading the key back out of the subject has to put them together again
    rather than stopping at the first dot }
  FKV.Put('app.db.host', 'localhost');

  Assert.IsTrue(FKV.Get('app.db.host', LEntry));
  Assert.AreEqual('localhost', LEntry.ValueString);
  Assert.AreEqual('app.db.host', LEntry.Key, 'the whole key, dots included');

  LKeys := FKV.Keys;
  Assert.AreEqual(1, Length(LKeys));
  Assert.AreEqual('app.db.host', LKeys[0]);
end;

procedure TJetStreamKVLiveTests.PutIfAbsent_SecondTime_Raises;
var
  LEntry: TKVEntry;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 1));

  FKV.PutIfAbsent('name', 'first');

  { An expectation of ZERO - "nothing has ever been written to this subject" -
    is what makes this a create rather than a put. A client that dropped a zero
    expectation as "unset" would let this second call succeed }
  Assert.WillRaise(
    procedure
    begin
      FKV.PutIfAbsent('name', 'second');
    end,
    EJetStreamKVError);

  Assert.IsTrue(FKV.Get('name', LEntry));
  Assert.AreEqual('first', LEntry.ValueString, 'the rejected write must not have landed');
end;

procedure TJetStreamKVLiveTests.Update_WithAStaleRevision_Raises;
var
  LRevision: UInt64;
  LEntry: TKVEntry;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 1));

  LRevision := FKV.Put('name', 'first');
  FKV.Update('name', 'second', LRevision);   // still current: succeeds

  { ...and now LRevision is stale, which is exactly the race a compare-and-set
    exists to lose safely }
  Assert.WillRaise(
    procedure
    begin
      FKV.Update('name', 'third', LRevision);
    end,
    EJetStreamKVError);

  Assert.IsTrue(FKV.Get('name', LEntry));
  Assert.AreEqual('second', LEntry.ValueString);
end;

procedure TJetStreamKVLiveTests.Delete_HidesTheKeyButKeepsItsHistory;
var
  LEntry: TKVEntry;
  LHistory: TArray<TKVEntry>;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 10));

  FKV.Put('name', 'first');
  FKV.Delete('name');

  Assert.IsFalse(FKV.Get('name', LEntry), 'a deleted key is not set');

  { The stream denies deletes, so the tombstone is an EXTRA message rather than
    the removal of one - and the old value is still there to be seen }
  LHistory := FKV.History('name');
  Assert.AreEqual(2, Length(LHistory), 'the value and the tombstone');
  Assert.AreEqual('first', LHistory[0].ValueString);
  Assert.IsTrue(LHistory[1].Operation = TKVOperation.Delete);
  Assert.AreEqual('', LHistory[1].ValueString, 'a tombstone carries no value');
end;

procedure TJetStreamKVLiveTests.Purge_ErasesTheHistoryToo;
var
  LHistory: TArray<TKVEntry>;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 10));

  FKV.Put('name', 'first');
  FKV.Put('name', 'second');
  Assert.AreEqual(2, Length(FKV.History('name')), 'both revisions are held');

  FKV.Purge('name');

  { Nats-Rollup: sub is what does this - the purge message REPLACES every
    earlier one on the subject. Misspell the header and the server ignores it,
    leaving all three messages behind, which is what this pins }
  LHistory := FKV.History('name');
  Assert.AreEqual(1, Length(LHistory), 'a purge rolls the subject up to itself');
  Assert.IsTrue(LHistory[0].Operation = TKVOperation.Purge);
end;

procedure TJetStreamKVLiveTests.Keys_ListsLiveKeysOnly;
var
  LKeys: TArray<string>;
  LKey: string;
  LFoundA, LFoundC, LFoundDeleted: Boolean;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 1));

  FKV.Put('alpha', '1');
  FKV.Put('beta', '2');
  FKV.Put('gamma', '3');
  FKV.Delete('beta');

  LKeys := FKV.Keys;

  LFoundA := False;
  LFoundC := False;
  LFoundDeleted := False;
  for LKey in LKeys do
  begin
    if LKey = 'alpha' then LFoundA := True;
    if LKey = 'gamma' then LFoundC := True;
    if LKey = 'beta' then LFoundDeleted := True;
  end;

  Assert.IsTrue(LFoundA, 'alpha');
  Assert.IsTrue(LFoundC, 'gamma');

  { A deleted key still HAS a subject and a last message - the tombstone - so
    only reading the operation tells a live key from a dead one. A Keys that
    just listed subjects would report beta }
  Assert.IsFalse(LFoundDeleted, 'a deleted key is not a key');
  Assert.AreEqual(2, Length(LKeys));
end;

procedure TJetStreamKVLiveTests.Keys_DoesNotWaitOutAPullTimeout;
var
  LClock: TStopwatch;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 1));
  FKV.Put('one', '1');

  LClock := TStopwatch.StartNew;
  FKV.Keys;
  LClock.Stop;

  { A scan drains what is ALREADY in the stream, so it must pull with no_wait.
    An ordinary Fetch sends an expiry the server honours by HOLDING the final,
    empty request open for its whole duration - and every scan ends with one of
    those, so Keys, History, List and every object read paid a full timeout.
    Nothing else notices: the results are identical, only slower }
  Assert.IsTrue(LClock.ElapsedMilliseconds < SCAN_BUDGET_MS,
    Format('a scan of one key took %d ms - it is waiting out a pull timeout ' +
      'rather than pulling with no_wait', [LClock.ElapsedMilliseconds]));
end;

procedure TJetStreamKVLiveTests.History_ShowsEveryRevisionOldestFirst;
var
  LHistory: TArray<TKVEntry>;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 10));

  FKV.Put('name', 'one');
  FKV.Put('name', 'two');
  FKV.Put('name', 'three');

  LHistory := FKV.History('name');

  Assert.AreEqual(3, Length(LHistory));
  Assert.AreEqual('one', LHistory[0].ValueString, 'oldest first');
  Assert.AreEqual('two', LHistory[1].ValueString);
  Assert.AreEqual('three', LHistory[2].ValueString);

  Assert.IsTrue(LHistory[0].Revision < LHistory[2].Revision, 'revisions ascend');
end;

procedure TJetStreamKVLiveTests.History_IsBoundedByTheBucketsHistorySetting;
var
  LHistory: TArray<TKVEntry>;
begin
  Connect;

  { History 2 means max_msgs_per_subject 2, and the server enforces it by
    dropping the oldest - there is no separate history mechanism to get wrong }
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 2));

  FKV.Put('name', 'one');
  FKV.Put('name', 'two');
  FKV.Put('name', 'three');

  LHistory := FKV.History('name');

  Assert.AreEqual(2, Length(LHistory), 'only the configured number of revisions is kept');
  Assert.AreEqual('two', LHistory[0].ValueString, 'the oldest was dropped');
  Assert.AreEqual('three', LHistory[1].ValueString);
end;

procedure TJetStreamKVLiveTests.Status_Values_CountsKeysNotMessages;
var
  LStatus: TJetStreamKVStatus;
begin
  Connect;
  FKV := TJetStreamKV.CreateBucket(FJs, KVConfig(FBucket, 3));

  { Two revisions of a, one each of b and c, then a tombstone for b }
  FKV.Put('a', 'one');
  FKV.Put('a', 'two');
  FKV.Put('b', 'one');
  FKV.Put('c', 'one');
  FKV.Delete('b');

  LStatus := FKV.Status;

  { Five messages live in the stream, but only three SUBJECTS hold them: a
    (two revisions), b (its tombstone), c. Values must be the key count with
    the tombstoned key still counted - the old code returned the message
    count, which was right only for a History=1 bucket with no deletes. This
    also proves nats-server's num_subjects is what the KV layer expects }
  Assert.AreEqual(UInt64(3), LStatus.Values,
    'Values counts keys with a message, tombstones included');
  Assert.IsTrue(LStatus.Bytes > 0, 'the stored bytes are reported');
  Assert.AreEqual(2, Length(FKV.Keys), 'two live keys besides the tombstone');
end;

{ TJetStreamObjectStoreLiveTests }

procedure TJetStreamObjectStoreLiveTests.Setup;
begin
  UseIndySocket;

  FBucket := 'delphi_obj_' + TNUID.NextNuid;

  FConn := TNatsConnection.Create;
  FConn.Name := 'ObjLiveConn';
  FConn.SetChannel(LIVE_HOST, NatsConstants.DEFAULT_PORT, LIVE_TIMEOUT);

  FJs := TJetStreamContext.Create(FConn);
  FOs := nil;
end;

procedure TJetStreamObjectStoreLiveTests.TearDown;
begin
  if Assigned(FConn) and FConn.Connected then
    try
      TJetStreamObjectStore.DeleteBucket(FJs, FBucket);
    except
      on E: EJetStreamApiError do ;
      on E: EJetStreamTimeout do ;
    end;

  FOs.Free;
  FJs.Free;
  FConn.Free;
end;

procedure TJetStreamObjectStoreLiveTests.Connect;
begin
  FConn.Open(nil);
  Assert.IsTrue(FConn.WaitForReady(WAIT_MS),
    Format('handshake with nats-server at %s:%d did not complete: %s',
      [LIVE_HOST, NatsConstants.DEFAULT_PORT, FConn.LastError]));
end;

procedure TJetStreamObjectStoreLiveTests.CreateBucket(AChunkSize: Integer);
var
  LConfig: TJetStreamObjectStoreConfig;
begin
  LConfig := Default(TJetStreamObjectStoreConfig);
  LConfig.Bucket := FBucket;
  LConfig.Storage := TJetStreamStorage.Memory;   // no files left behind

  { Deliberately tiny, so a few kilobytes is genuinely many chunks. The default
    is 128 KB, which no test would want to exceed just to see it split }
  LConfig.ChunkSize := AChunkSize;

  FOs := TJetStreamObjectStore.CreateBucket(FJs, LConfig);
end;

function TJetStreamObjectStoreLiveTests.Pattern(ACount: Integer): TBytes;
var
  LIndex: Integer;
begin
  SetLength(Result, ACount);

  { Not uniform: a chunk delivered twice, or two delivered out of order, would
    be invisible in a buffer of identical bytes }
  for LIndex := 0 to ACount - 1 do
    Result[LIndex] := Byte((LIndex * 7 + (LIndex div 251)) and $FF);
end;

procedure TJetStreamObjectStoreLiveTests.Bucket_CreateStatusAndDelete;
var
  LStatus: TJetStreamObjectStoreStatus;
begin
  Connect;
  CreateBucket(1024);

  LStatus := FOs.Status;
  Assert.AreEqual(FBucket, LStatus.Bucket);
  Assert.AreEqual('OBJ_' + FBucket, LStatus.StreamName);
  Assert.AreEqual(UInt64(0), LStatus.Messages, 'a new bucket holds nothing');

  TJetStreamObjectStore.DeleteBucket(FJs, FBucket);

  Assert.WillRaise(
    procedure
    begin
      FOs.Status;
    end,
    EJetStreamApiError, 'the bucket''s stream must really be gone');
end;

procedure TJetStreamObjectStoreLiveTests.SmallObject_RoundTrips;
var
  LInfo: TJetStreamObjectInfo;
begin
  Connect;
  CreateBucket(1024);

  LInfo := FOs.PutString('greeting.txt', 'hello from delphi');

  Assert.AreEqual('greeting.txt', LInfo.Name);
  Assert.AreEqual(FBucket, LInfo.Bucket);
  Assert.AreEqual(UInt64(17), LInfo.Size);
  Assert.AreEqual(1, LInfo.Chunks, 'well under one chunk');
  Assert.IsTrue(LInfo.Digest.StartsWith('SHA-256='), 'the digest names its algorithm');

  Assert.AreEqual('hello from delphi', FOs.GetString('greeting.txt'));
end;

procedure TJetStreamObjectStoreLiveTests.LargeObject_IsSplitAndPutBackTogether;
var
  LData, LBack: TBytes;
  LInfo: TJetStreamObjectInfo;
  LIndex: Integer;
begin
  Connect;
  CreateBucket(1024);

  { 10 whole chunks plus a partial one, so the loop's end condition is exercised
    rather than landing exactly on a boundary }
  LData := Pattern(10 * 1024 + 377);

  LInfo := FOs.Put('big.bin', LData);
  Assert.AreEqual(UInt64(Length(LData)), LInfo.Size);
  Assert.AreEqual(11, LInfo.Chunks, 'ten full chunks and a remainder');

  Assert.IsTrue(FOs.Get('big.bin', LBack));

  { Byte for byte. Every chunk of an object is on ONE subject, so stream order
    IS chunk order - but nothing about that is obvious, and a reassembly that
    dropped or repeated one would produce a plausible file of the wrong length }
  Assert.AreEqual(Length(LData), Length(LBack), 'the whole object came back');
  for LIndex := 0 to High(LData) do
    if LData[LIndex] <> LBack[LIndex] then
      Assert.Fail(Format('byte %d differs: stored %d, got %d',
        [LIndex, LData[LIndex], LBack[LIndex]]));
end;

procedure TJetStreamObjectStoreLiveTests.BinaryObject_SurvivesIntact;
var
  LData, LBack: TBytes;
begin
  Connect;
  CreateBucket(1024);

  { Not valid UTF-8 anywhere, and containing the bytes a text path would ruin }
  LData := [$00, $FF, $FE, $0D, $0A, $80, $7F, $00];

  FOs.Put('blob.bin', LData);
  Assert.IsTrue(FOs.Get('blob.bin', LBack));

  Assert.AreEqual(Length(LData), Length(LBack));
  Assert.AreEqual(LData[0], LBack[0], 'a leading zero byte');
  Assert.AreEqual(LData[3], LBack[3], 'an embedded CR');
  Assert.AreEqual(LData[4], LBack[4], 'an embedded LF');
end;

procedure TJetStreamObjectStoreLiveTests.ObjectNameWithSpacesAndSlashes_RoundTrips;
const
  NAME = 'reports/2024 Q1 (final).pdf';
var
  LList: TArray<TJetStreamObjectInfo>;
begin
  Connect;
  CreateBucket(1024);

  { The name is base64url-encoded into the subject. Used raw it would be
    several tokens with spaces and brackets in them - which is not a legal
    subject at all, so this is what proves the encoding is really applied }
  FOs.PutString(NAME, 'quarterly');

  Assert.AreEqual('quarterly', FOs.GetString(NAME));

  { and it must come back DECODED, not as the base64 the subject carries }
  LList := FOs.List;
  Assert.AreEqual(1, Length(LList));
  Assert.AreEqual(NAME, LList[0].Name);
end;

procedure TJetStreamObjectStoreLiveTests.Get_MissingObject_ReportsNotFound;
var
  LData: TBytes;
begin
  Connect;
  CreateBucket(1024);

  Assert.IsFalse(FOs.Get('never_stored', LData));
  Assert.AreEqual('fallback', FOs.GetString('never_stored', 'fallback'));
end;

procedure TJetStreamObjectStoreLiveTests.Delete_RemovesTheObjectAndItsBytes;
var
  LInfo: TJetStreamObjectInfo;
  LBefore, LAfter: UInt64;
begin
  Connect;
  CreateBucket(1024);

  FOs.Put('big.bin', Pattern(5 * 1024));
  LBefore := FOs.Status.Messages;
  Assert.IsTrue(LBefore >= 5, 'the chunks and the metadata record are all there');

  FOs.Delete('big.bin');

  Assert.IsFalse(FOs.Info('big.bin', LInfo), 'a deleted object is not an object');

  { The chunks are purged, so the bucket shrinks. The metadata tombstone stays,
    which is why this is "fewer", not "none" }
  LAfter := FOs.Status.Messages;
  Assert.IsTrue(LAfter < LBefore,
    Format('the bytes must go: %d messages before, %d after', [LBefore, LAfter]));
end;

procedure TJetStreamObjectStoreLiveTests.Put_Again_ReplacesTheObjectAndDropsTheOldChunks;
var
  LFirst, LSecond: TJetStreamObjectInfo;
  LAfterFirst, LAfterSecond: UInt64;
  LBack: TBytes;
begin
  Connect;
  CreateBucket(1024);

  LFirst := FOs.Put('doc.bin', Pattern(5 * 1024));
  LAfterFirst := FOs.Status.Messages;

  LSecond := FOs.Put('doc.bin', Pattern(5 * 1024));
  LAfterSecond := FOs.Status.Messages;

  { A new write gets a NEW nuid, so its chunks land on a subject of their own -
    which is what lets a reader finish streaming the old version }
  Assert.AreNotEqual(LFirst.Nuid, LSecond.Nuid, 'each write gets its own chunk subject');

  { ...and then the old subject is purged. Without that the bucket would grow
    without bound on every overwrite, holding every version forever }
  Assert.IsTrue(LAfterSecond <= LAfterFirst + 1,
    Format('the old chunks must be purged: %d messages after one write, %d after two',
      [LAfterFirst, LAfterSecond]));

  Assert.IsTrue(FOs.Get('doc.bin', LBack));
  Assert.AreEqual(5 * 1024, Length(LBack), 'and the current version still reads');
end;

procedure TJetStreamObjectStoreLiveTests.List_OmitsDeletedObjects;
var
  LList: TArray<TJetStreamObjectInfo>;
  LInfo: TJetStreamObjectInfo;
  LFoundDeleted: Boolean;
begin
  Connect;
  CreateBucket(1024);

  FOs.PutString('alpha.txt', '1');
  FOs.PutString('beta.txt', '2');
  FOs.PutString('gamma.txt', '3');
  FOs.Delete('beta.txt');

  LList := FOs.List;

  LFoundDeleted := False;
  for LInfo in LList do
    if LInfo.Name = 'beta.txt' then
      LFoundDeleted := True;

  { A deleted object keeps its metadata record, so a List that just read the
    metadata space without checking the tombstone would report beta }
  Assert.IsFalse(LFoundDeleted, 'a deleted object must not be listed');
  Assert.AreEqual(2, Length(LList));
end;

procedure TJetStreamObjectStoreLiveTests.Get_WithItsChunksPurged_Raises;
var
  LInfo: TJetStreamObjectInfo;
  LPurge: TJetStreamPurgeRequest;
  LData: TBytes;
begin
  Connect;
  CreateBucket(1024);

  LInfo := FOs.Put('doomed.bin', Pattern(5 * 1024));

  { Take the bytes out from under the metadata, which is exactly what a MaxAge
    or a MaxBytes would eventually do to an old object }
  LPurge := Default(TJetStreamPurgeRequest);
  LPurge.Filter := Format('$O.%s.C.%s', [FBucket, LInfo.Nuid]);
  FJs.PurgeStream('OBJ_' + FBucket, LPurge);

  { The metadata still says five chunks, and five chunks is what the caller is
    entitled to. Returning nothing and reporting success would hand back an
    empty file that looks exactly like a successfully retrieved empty object }
  Assert.WillRaise(
    procedure
    begin
      FOs.Get('doomed.bin', LData);
    end,
    EJetStreamObjectError);
end;

procedure TJetStreamObjectStoreLiveTests.Get_FailedRead_LeavesTheDestinationRolledBack;
var
  LInfo: TJetStreamObjectInfo;
  LDest: TBytesStream;
begin
  Connect;
  CreateBucket(1024);

  LInfo := FOs.Put('doomed.bin', Pattern(5 * 1024));

  { Rewrite the metadata with a WRONG digest - the chunks are all still there,
    so the read writes every byte to the destination and only then fails the
    digest check. That is the case the rollback exists for }
  LInfo.Digest := 'SHA-256=Zm9v';   // deliberately wrong
  LInfo.Mtime := '2026-08-13T00:00:00Z';
  FJs.Publish(
    Format('$O.%s.M.%s', [FBucket, TObjectStoreEncoding.Encode('doomed.bin')]),
    TJetStreamJSON.ToJSON<TJetStreamObjectInfo>(LInfo),
    TJetStreamPubOptions.New.WithHeader(
      JetStreamConstants.Header.ROLLUP, JetStreamConstants.Header.ROLLUP_SUBJECT));

  LDest := TBytesStream.Create;
  try
    Assert.WillRaise(
      procedure
      begin
        FOs.Get('doomed.bin', LDest);
      end,
      EJetStreamObjectError);

    { All 5 KB reached the destination before the digest check failed - without
      the rollback a caller catching the exception would be left with a stream
      that looks exactly like a successful partial download }
    Assert.AreEqual(Int64(0), LDest.Size, 'the partial bytes must be rolled back');
    Assert.AreEqual(Int64(0), LDest.Position, 'and the position restored');
  finally
    LDest.Free;
  end;
end;

procedure TJetStreamObjectStoreLiveTests.PutFileAndGetFile_RoundTrip;
var
  LSource, LTarget: string;
  LData, LBack: TBytes;
begin
  Connect;
  CreateBucket(1024);

  LSource := TPath.Combine(TPath.GetTempPath, 'nats_obj_src_' + TNUID.NextNuid + '.bin');
  LTarget := TPath.Combine(TPath.GetTempPath, 'nats_obj_dst_' + TNUID.NextNuid + '.bin');
  try
    LData := Pattern(3 * 1024 + 11);
    TFile.WriteAllBytes(LSource, LData);

    FOs.PutFile('from_disk.bin', LSource);
    Assert.IsTrue(FOs.GetFile('from_disk.bin', LTarget));

    LBack := TFile.ReadAllBytes(LTarget);
    Assert.AreEqual(Length(LData), Length(LBack), 'the file came back whole');
    Assert.AreEqual(LData[High(LData)], LBack[High(LBack)], 'including its last byte');
  finally
    if TFile.Exists(LSource) then
      TFile.Delete(LSource);
    if TFile.Exists(LTarget) then
      TFile.Delete(LTarget);
  end;
end;

procedure TJetStreamObjectStoreLiveTests.Put_PartialReads_StillStoresTheWholeObject;
var
  LData, LBack: TBytes;
  LStream: TShortReadStream;
  LInfo: TJetStreamObjectInfo;
  LIndex: Integer;
begin
  Connect;
  CreateBucket(1024);

  LData := Pattern(5 * 1024 + 123);
  LStream := TShortReadStream.Create(LData, 300);
  try
    LInfo := FOs.Put('short-reads.bin', LStream);
  finally
    LStream.Free;
  end;

  { The old loop stopped on the FIRST short read, so only 300 of these bytes
    would have been stored - and the digest was computed over those 300, so a
    Get would come back short with nothing having raised. The whole object has
    to land }
  Assert.AreEqual(UInt64(Length(LData)), LInfo.Size,
    'a stream that reads in dribs and drabs must still be stored whole');
  Assert.IsTrue(LInfo.Chunks > 1, 'the short reads must produce several chunks');

  Assert.IsTrue(FOs.Get('short-reads.bin', LBack));
  Assert.AreEqual(Length(LData), Length(LBack), 'the whole object came back');
  for LIndex := 0 to High(LData) do
    if LData[LIndex] <> LBack[LIndex] then
      Assert.Fail(Format('byte %d differs: stored %d, got %d',
        [LIndex, LData[LIndex], LBack[LIndex]]));
end;

procedure TJetStreamObjectStoreLiveTests.Put_StreamEndingEarly_Raises;
var
  LStream: TShortStream;
begin
  Connect;
  CreateBucket(1024);

  LStream := TShortStream.Create(Pattern(1024));
  try
    { A stream that claims a size it will never deliver: storing the bytes it
      did hand out would produce an object whose digest matches a truncated
      upload, exactly the silent corruption the completeness check exists to
      stop }
    Assert.WillRaise(
      procedure
      begin
        FOs.Put('short.bin', LStream);
      end,
      EJetStreamObjectError);
  finally
    LStream.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TNatsLiveServerTests);
  TDUnitX.RegisterTestFixture(TJetStreamLiveTests);
  TDUnitX.RegisterTestFixture(TJetStreamKVLiveTests);
  TDUnitX.RegisterTestFixture(TJetStreamObjectStoreLiveTests);

end.
