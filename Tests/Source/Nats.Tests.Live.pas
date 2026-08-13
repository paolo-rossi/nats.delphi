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
  System.SysUtils, System.Classes, System.Diagnostics,

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
  Nats.JetStream.Entities,
  Nats.JetStream.Message,

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

var
  GLiveSwitchCount: Integer = 0;

procedure UseIndySocket;
begin
  // see UseMockSocket: the registry default is process-wide
  Inc(GLiveSwitchCount);
  TNatsSocketRegistry.Register<TNatsSocketIndy>(Format('IndyLive#%d', [GLiveSwitchCount]), True);
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

initialization
  TDUnitX.RegisterTestFixture(TNatsLiveServerTests);
  TDUnitX.RegisterTestFixture(TJetStreamLiveTests);

end.
