{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Tests.Adapters;

{******************************************************************************}
{                                                                              }
{  Socket adapter tests and end-to-end protocol tests: a real TNatsConnection   }
{  driven against TNatsMockSocket, asserting the exact bytes on the wire.       }
{                                                                              }
{  Tests marked [KNOWN BUG §n] assert correct NATS behaviour and currently      }
{  FAIL. "n" refers to the section in Docs\Core-Protocol-Review.md.            }
{                                                                              }
{******************************************************************************}

interface

uses
  System.SysUtils, System.Classes,

  DUnitX.TestFramework,

  Nats.Consts,
  Nats.Classes,
  Nats.Entities,
  Nats.Socket,
  Nats.Connection,
  Nats.Exceptions,

  Nats.Tests.Mocks;

type
  [TestFixture]
  TNatsSocketRegistryTests = class
  public
    [Test]
    procedure Get_WithEmptyName_ReturnsTheDefaultClass;
    [Test]
    procedure Get_ByRegisteredName_ReturnsInstance;
    [Test]
    procedure Get_UnknownName_Raises;
  end;

  [TestFixture]
  TNatsMockSocketTests = class
  private
    FSocket: INatsSocket;
    function Mock: TNatsMockSocket;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure SendString_AppendsCrLf;
    [Test]
    procedure SendBytes_AppendsCrLf;
    [Test]
    procedure ReceiveString_StripsCrLf;
    [Test]
    procedure ReceiveString_ReturnsOneLineAtATime;
    [Test]
    procedure ReceiveExactBytes_ReturnsExactlyTheRequestedCount;
    [Test]
    procedure ReceiveString_WithoutData_TimesOut;
  end;

  // §13: a connect timeout and a read timeout are different things
  [TestFixture]
  TNatsChannelTimeoutTests = class
  private
    FConn: TNatsConnection;
    FSocket: TNatsMockSocket;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure SetChannel_SetsTheConnectTimeout;
    [Test]
    procedure SetChannel_DoesNotTouchTheReadTimeout;
    [Test]
    procedure SetChannel_CanSetTheReadTimeoutExplicitly;
    [Test]
    procedure DefaultReadTimeout_OutlastsTheServerPingInterval;
  end;

  [TestFixture]
  TNatsConnectionProtocolTests = class
  private
    FConn: TNatsConnection;
    FSocket: TNatsMockSocket;
    FLog: TNatsTestLog;      // connection lifecycle events
    FMsgLog: TNatsTestLog;   // delivered messages, kept separate so that
                             // "a message arrived" is never satisfied by the
                             // handshake
    FConnectUser: string;
    /// <summary>
    ///   Drives the "server" side of a RequestSync, which blocks the test
    ///   thread. Joined in TearDown so it can never outlive the mock socket
    /// </summary>
    FServerThread: TThread;
    procedure OpenAndHandshake; overload;
    procedure OpenAndHandshake(const AInfoJson: string); overload;
    function LogHandler: TNatsMsgHandler;
    procedure ReplyWhenSubscribed(const APayload: string);
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    { handshake }

    [Test]
    procedure Info_TriggersConnect;
    [Test]
    procedure Info_InvokesConnectHandlerWithServerInfo;
    [Test]
    procedure Info_ConnectHandlerCanAmendConnectOptions;
    [Test]
    procedure Connect_DeclaresHeaderSupport;
    [Test]
    procedure ServerPing_IsAnsweredWithPong;

    { publish }

    [Test]
    procedure Publish_WritesPubFrame;
    [Test]
    procedure Publish_WithReplyTo_WritesReplySubject;
    [Test]
    procedure Publish_EmptyPayload_WritesZeroLength;
    [Test]
    procedure Publish_MultiBytePayload_DeclaresByteLengthNotCharLength;
    [Test]
    procedure Publish_EmptySubject_Raises;
    [Test]
    procedure Publish_SubjectWithWhitespace_Raises;
    [Test]
    procedure Subscribe_EmptySubject_Raises;

    { subscribe }

    [Test]
    procedure Subscribe_WritesSubFrame;
    [Test]
    procedure Subscribe_WithQueueGroup_WritesQueueName;
    [Test]
    procedure Subscribe_AssignsIncrementingSids;
    [Test]
    procedure Unsubscribe_WritesUnsubFrame;
    [Test]
    procedure Unsubscribe_WithMaxMessages_WritesMaxCount;
    // §5: <max_msgs> is the total delivered on the sid, not "this many more"
    [Test]
    procedure Unsubscribe_WithMax_CountsMessagesAlreadyReceived;
    [Test]
    procedure Unsubscribe_WithMaxAlreadyReached_DropsTheSubscriptionAtOnce;
    [Test]
    procedure Unsubscribe_WithMax_DropsTheSubscriptionAfterTheLastMessage;
    [Test]
    procedure Unsubscribe_BySubject_WritesUnsubFrame;
    [Test]
    procedure Unsubscribe_UnknownSubject_Raises;

    { deliver }

    [Test]
    procedure Msg_IsDispatchedToTheSubscriptionHandler;
    [Test]
    procedure Msg_EmptyPayload_IsDispatched;
    [Test]
    procedure Msg_TwoFramesInOneRead_AreBothDispatched;
    [Test]
    procedure Msg_ForUnknownSid_IsIgnored;
    [Test]
    procedure Msg_CarriesReplyToSubject;
    [Test]
    procedure Msg_BinaryPayload_IsDeliveredIntact;

    { headers }

    // [KNOWN BUG §2 + §3]
    [Test]
    procedure Hpub_WritesSpecCompliantFrame;
    // [KNOWN BUG §2] isolated from the separator bug
    [Test]
    procedure Hpub_DeclaredCountsMatchTheBytesWritten;
    [Test]
    procedure Hpub_WithoutHeaders_FallsBackToPub;
    // [KNOWN BUG §1 + §4]
    [Test]
    procedure Hmsg_PayloadIsDeliveredIntact;
    [Test]
    procedure Hmsg_HeadersAreDeliveredToTheHandler;

    { status line - §2 of Docs\JetStream-Plan.md }

    // the whole point: a "404 No Messages" and a legitimate empty message are
    // the same bytes apart from the status line
    [Test]
    procedure Hmsg_StatusMessage_CarriesTheStatusCode;
    [Test]
    procedure Hmsg_EmptyMessage_HasNoStatus;
    [Test]
    procedure Msg_WithoutHeaderBlock_HasNoStatus;

    { request / inbox }

    [Test]
    procedure Request_SubscribesToAnInbox;
    [Test]
    procedure Request_PublishesWithTheInboxAsReplyTo;
    // §6: a request expects exactly one reply and must not leak its inbox
    [Test]
    procedure Request_AutoUnsubscribesAfterOneReply;
    [Test]
    procedure Request_SubscriptionIsDroppedAfterTheReply;
    [Test]
    procedure NewInbox_IsUniqueAcrossConnections;

    { synchronous request - §1 of Docs\JetStream-Plan.md }

    [Test]
    procedure RequestSync_WritesSubArmUnsubThenPublish;
    [Test]
    procedure RequestSync_ReturnsTheReply;
    [Test]
    procedure RequestSync_Reply_LeavesNoSubscription;
    [Test]
    procedure RequestSync_Timeout_ReturnsFalse;
    // the leak §1 exists to close: a reply that never comes used to leave the
    // inbox subscribed on both sides, one entry per call
    [Test]
    procedure RequestSync_Timeout_RemovesTheSubscription;
    [Test]
    procedure RequestSync_Timeout_UnsubscribesOnTheWire;
    // the form JetStream publish needs: binary payload plus headers
    [Test]
    procedure RequestSync_WithHeaders_WritesHpub;
    [Test]
    procedure RequestSync_ConnectionClosedWhileWaiting_Raises;
    [Test]
    procedure RequestSync_NotConnected_Raises;
    [Test]
    procedure RequestSync_ZeroTimeout_Raises;

    { max_payload - §5 of Docs\JetStream-Plan.md. Oversized publishes get the
      connection closed by the server, so they must be refused at the call site }

    [Test]
    procedure MaxPayload_IsTakenFromInfo;
    // F10: max_payload * 2 overflows Integer above ~1 GiB - the line cap must
    // be a fixed ceiling, not something derived from max_payload
    [Test]
    procedure MaxLineLength_IsNotScaledOffMaxPayload;
    [Test]
    procedure Publish_OversizedPayload_Raises;
    [Test]
    procedure Publish_OversizedPayload_WritesNothing;
    [Test]
    procedure Publish_ExactlyMaxPayload_IsAllowed;
    [Test]
    procedure PublishBytes_OversizedPayload_Raises;
    // the server counts HPUB's <#total bytes>, so headers count too
    [Test]
    procedure Hpub_OversizedTotal_Raises;
    [Test]
    procedure Publish_BeforeInfo_IsNotSizeChecked;
    [Test]
    procedure MaxPayload_LaterInfo_UpdatesTheLimit;
  end;

implementation

const
  INFO_JSON =
    '{"server_id":"NDHJZQZ4YQXQ","server_name":"nats-1","version":"2.10.11",' +
    '"proto":1,"host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,' +
    '"jetstream":true,"client_id":7,"client_ip":"127.0.0.1"}';

  MOCK_TIMEOUT = 200;

  /// <summary>
  ///   Request timeout for the tests that mean to hit it. Short on purpose, and
  ///   unrelated to the socket read timeout - the mock's is 30 s, so nothing
  ///   here trips a keep-alive PING and pollutes the captured wire bytes
  /// </summary>
  REQUEST_TIMEOUT = 150;

  /// <summary>
  ///   The same server, declaring a tiny max_payload. Lets the size checks be
  ///   exercised exactly at the boundary without allocating megabytes
  /// </summary>
  SMALL_MAX_PAYLOAD = 32;
  INFO_JSON_SMALL =
    '{"server_id":"NDHJZQZ4YQXQ","server_name":"nats-1","version":"2.10.11",' +
    '"proto":1,"host":"0.0.0.0","port":4222,"headers":true,"max_payload":32,' +
    '"jetstream":true,"client_id":7,"client_ip":"127.0.0.1"}';

  /// <summary>
  ///   The same server declaring a max_payload above ~1 GiB, where the old
  ///   "max_payload * 2" line-cap calculation overflowed Integer
  /// </summary>
  INFO_JSON_HUGE_MAX_PAYLOAD =
    '{"server_id":"NDHJZQZ4YQXQ","server_name":"nats-1","version":"2.10.11",' +
    '"proto":1,"host":"0.0.0.0","port":4222,"headers":true,"max_payload":2147483647,' +
    '"jetstream":true,"client_id":7,"client_ip":"127.0.0.1"}';

{ TNatsSocketRegistryTests }

procedure TNatsSocketRegistryTests.Get_WithEmptyName_ReturnsTheDefaultClass;
var
  LSocket: INatsSocket;
begin
  UseMockSocket;   // the live fixture flips the process-wide default

  LSocket := TNatsSocketRegistry.Get(String.Empty);

  Assert.IsNotNull(LSocket);
  Assert.IsTrue((LSocket as TObject) is TNatsMockSocket,
    'the mock must be the default socket class in the test project');
end;

procedure TNatsSocketRegistryTests.Get_ByRegisteredName_ReturnsInstance;
begin
  Assert.IsNotNull(TNatsSocketRegistry.Get('Mock'));
end;

procedure TNatsSocketRegistryTests.Get_UnknownName_Raises;
begin
  Assert.WillRaise(
    procedure
    begin
      TNatsSocketRegistry.Get('NoSuchSocketClass');
    end,
    ENatsException);
end;

{ TNatsMockSocketTests }

procedure TNatsMockSocketTests.Setup;
begin
  FSocket := TNatsSocketRegistry.Get('Mock');
  FSocket.ReadTimeout := MOCK_TIMEOUT;
  FSocket.Open;
end;

procedure TNatsMockSocketTests.TearDown;
begin
  FSocket := nil;
end;

function TNatsMockSocketTests.Mock: TNatsMockSocket;
begin
  Result := FSocket as TNatsMockSocket;
end;

procedure TNatsMockSocketTests.SendString_AppendsCrLf;
begin
  FSocket.SendString('PING');

  Assert.AreEqual('PING'#13#10, Mock.ClientText);
end;

procedure TNatsMockSocketTests.SendBytes_AppendsCrLf;
begin
  FSocket.SendBytes(TEncoding.UTF8.GetBytes('hello'));

  Assert.AreEqual('hello'#13#10, Mock.ClientText);
end;

procedure TNatsMockSocketTests.ReceiveString_StripsCrLf;
begin
  Mock.ServerSendLine('PONG');

  Assert.AreEqual('PONG', FSocket.ReceiveString);
end;

procedure TNatsMockSocketTests.ReceiveString_ReturnsOneLineAtATime;
begin
  Mock.ServerSend('+OK'#13#10'PING'#13#10);

  Assert.AreEqual('+OK', FSocket.ReceiveString);
  Assert.AreEqual('PING', FSocket.ReceiveString);
end;

procedure TNatsMockSocketTests.ReceiveExactBytes_ReturnsExactlyTheRequestedCount;
var
  LData: TBytes;
begin
  Mock.ServerSend('hello world'#13#10);

  LData := FSocket.ReceiveExactBytes(5);

  Assert.AreEqual(5, Length(LData));
  Assert.AreEqual('hello', TEncoding.UTF8.GetString(LData));
  Assert.AreEqual(' world', FSocket.ReceiveString);
end;

procedure TNatsMockSocketTests.ReceiveString_WithoutData_TimesOut;
begin
  Assert.WillRaise(
    procedure
    begin
      FSocket.ReceiveString;
    end,
    ENatsReadTimeout, 'the mock must time out like Indy does, never block forever');
end;

{ TNatsChannelTimeoutTests }

procedure TNatsChannelTimeoutTests.Setup;
begin
  UseMockSocket;
  FConn := TNatsConnection.Create;
  FSocket := TNatsMockSocket.LastInstance;
end;

procedure TNatsChannelTimeoutTests.TearDown;
begin
  FConn.Free;
end;

procedure TNatsChannelTimeoutTests.SetChannel_SetsTheConnectTimeout;
begin
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, 1234);

  Assert.AreEqual(Cardinal(1234), FSocket.ConnectTimeout,
    'the third argument bounds establishing the connection');
end;

procedure TNatsChannelTimeoutTests.SetChannel_DoesNotTouchTheReadTimeout;
var
  LBefore: Cardinal;
begin
  LBefore := FSocket.ReadTimeout;

  // the demo passes 1000 here; as a read timeout that made an idle but
  // perfectly healthy connection fail every single second
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, 1000);

  Assert.AreEqual(LBefore, FSocket.ReadTimeout,
    'a connect timeout must not become the read timeout');
end;

procedure TNatsChannelTimeoutTests.SetChannel_CanSetTheReadTimeoutExplicitly;
begin
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, 1000, 4321);

  Assert.AreEqual(Cardinal(1000), FSocket.ConnectTimeout);
  Assert.AreEqual(Cardinal(4321), FSocket.ReadTimeout);
end;

procedure TNatsChannelTimeoutTests.DefaultReadTimeout_OutlastsTheServerPingInterval;
var
  LRead, LPing, LConnect: Cardinal;
begin
  { through variables, so the compiler compares values instead of folding two
    constants and warning that the answer is known }
  LRead := NatsConstants.DEFAULT_READ_TIMEOUT;
  LPing := NatsConstants.DEFAULT_SERVER_PING_INTERVAL;
  LConnect := NatsConstants.DEFAULT_CONNECT_TIMEOUT;

  // nothing arrives on an idle connection until the server's next PING, so a
  // shorter read timeout would report a healthy connection as broken
  Assert.IsTrue(LRead > LPing,
    Format('read timeout %d must outlast the server ping interval %d', [LRead, LPing]));

  Assert.IsTrue(LConnect < LRead,
    'establishing a connection should be bounded far more tightly than a read');
end;

{ TNatsConnectionProtocolTests }

procedure TNatsConnectionProtocolTests.Setup;
begin
  UseMockSocket;   // the live fixture flips the process-wide default

  FLog := TNatsTestLog.Create;
  FMsgLog := TNatsTestLog.Create;
  FConnectUser := '';

  FConn := TNatsConnection.Create;
  // TNatsConnection builds its own socket through the registry
  FSocket := TNatsMockSocket.LastInstance;
  Assert.IsNotNull(FSocket, 'the connection did not create a mock socket');

  FConn.Name := 'TestConn';
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, MOCK_TIMEOUT);
end;

procedure TNatsConnectionProtocolTests.TearDown;
begin
  { Joined BEFORE the connection goes, because the mock socket is reference
    counted and dies with it - a server thread still running would be touching
    freed memory }
  if Assigned(FServerThread) then
  begin
    FServerThread.WaitFor;
    FreeAndNil(FServerThread);
  end;

  FConn.Free;   // may fire the disconnect handler, which writes to FLog
  FMsgLog.Free;
  FLog.Free;
end;

procedure TNatsConnectionProtocolTests.ReplyWhenSubscribed(const APayload: string);
var
  LSocket: TNatsMockSocket;
begin
  LSocket := FSocket;

  FServerThread := TThread.CreateAnonymousThread(
    procedure
    var
      LLines, LParts: TArray<string>;
    begin
      { Wait for the SUB to actually reach the wire. Replying earlier would
        deliver a message for a sid that does not exist yet, and the consumer
        would drop it }
      if not LSocket.WaitForClientText(NatsConstants.Protocol.SUB + ' ' +
           NatsConstants.INBOX_PREFIX) then
        Exit;

      // first line written by RequestSync is "SUB <inbox> <sid>"
      LLines := LSocket.ClientText.Split([NatsConstants.CR_LF]);
      LParts := LLines[0].Split([NatsConstants.SPC]);

      LSocket.ServerSend(Format('MSG %s %s %d'#13#10'%s'#13#10,
        [LParts[1], LParts[2], Length(TEncoding.UTF8.GetBytes(APayload)), APayload]));
    end);

  FServerThread.FreeOnTerminate := False;   // TearDown joins and frees it
  FServerThread.Start;
end;

function TNatsConnectionProtocolTests.LogHandler: TNatsMsgHandler;
begin
  Result :=
    procedure (const AMsg: TNatsArgsMSG)
    begin
      // runs on the consumer thread: only touch the thread-safe log
      FMsgLog.AddFmt('%s|%s|%s', [AMsg.Subject, AMsg.ReplyTo, AMsg.Payload]);
    end;
end;

procedure TNatsConnectionProtocolTests.OpenAndHandshake;
begin
  OpenAndHandshake(INFO_JSON);
end;

procedure TNatsConnectionProtocolTests.OpenAndHandshake(const AInfoJson: string);
begin
  FConn.Open(
    procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
    begin
      FLog.Add('CONNECT:' + AInfo.ServerName);
      if FConnectUser <> '' then
        AConnectOptions.User := FConnectUser;
    end,
    procedure
    begin
      FLog.Add('DISCONNECT');
    end);

  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + AInfoJson);

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.CONNECT),
    'the client never sent CONNECT in response to INFO');
  FSocket.ClearClientData;
end;

procedure TNatsConnectionProtocolTests.Info_TriggersConnect;
var
  LText: string;
begin
  FConn.Open(
    procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
    begin
    end);

  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + INFO_JSON);

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.CONNECT),
    'CONNECT must be sent after INFO');

  LText := FSocket.ClientText;
  Assert.IsTrue(LText.StartsWith('CONNECT {'), 'CONNECT must carry a JSON payload, got: ' + LText);
  Assert.IsTrue(LText.EndsWith(NatsConstants.CR_LF), 'CONNECT must be CRLF terminated');
  Assert.IsTrue(LText.Contains('"lang":"Delphi"'), 'CONNECT must identify the client language');
end;

procedure TNatsConnectionProtocolTests.Info_InvokesConnectHandlerWithServerInfo;
begin
  OpenAndHandshake;

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FLog.Contains('CONNECT:nats-1');
    end),
    'the connect handler must receive the deserialized INFO, log was: ' + FLog.Text);
end;

procedure TNatsConnectionProtocolTests.Info_ConnectHandlerCanAmendConnectOptions;
begin
  FConnectUser := 'joe';

  OpenAndHandshake;

  // ConnectOptions is passed to the handler as var, so what the handler sets
  // must end up in the CONNECT payload
  Assert.IsTrue(FConn.ConnectOptions.User = 'joe', 'the handler must be able to set ConnectOptions');
end;

procedure TNatsConnectionProtocolTests.Connect_DeclaresHeaderSupport;
var
  LText: string;
begin
  FConn.Open(
    procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
    begin
    end);

  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + INFO_JSON);
  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.CONNECT), 'no CONNECT was sent');

  // §19: a server refuses HPUB from a client that has not declared header
  // support, and strips headers from what it delivers
  LText := FSocket.ClientText;
  Assert.IsTrue(LText.Contains('"headers":true'),
    'CONNECT must declare header support by default, sent: ' + LText);
end;

procedure TNatsConnectionProtocolTests.ServerPing_IsAnsweredWithPong;
begin
  OpenAndHandshake;

  FSocket.ServerSendLine(NatsConstants.Protocol.PING);

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.PONG),
    'a server PING must be answered with PONG');
  Assert.AreEqual('PONG'#13#10, FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Publish_WritesPubFrame;
begin
  OpenAndHandshake;

  FConn.Publish('foo.bar', 'hello');

  Assert.AreEqual('PUB foo.bar 5'#13#10'hello'#13#10, FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Publish_WithReplyTo_WritesReplySubject;
begin
  OpenAndHandshake;

  FConn.Publish('foo.bar', 'hello', '_INBOX.7');

  Assert.AreEqual('PUB foo.bar _INBOX.7 5'#13#10'hello'#13#10, FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Publish_EmptyPayload_WritesZeroLength;
begin
  OpenAndHandshake;

  FConn.Publish('foo.bar', '');

  Assert.AreEqual('PUB foo.bar 0'#13#10#13#10, FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Publish_MultiBytePayload_DeclaresByteLengthNotCharLength;
const
  // escaped rather than literal: this file has no BOM, so a literal "àèì"
  // would be decoded differently depending on the compiler's source encoding
  ACCENTED = #$00E0#$00E8#$00EC;  // 3 characters, 6 UTF-8 bytes
begin
  OpenAndHandshake;

  // the protocol counts bytes, not characters
  FConn.Publish('foo', ACCENTED);

  Assert.AreEqual('PUB foo 6'#13#10 + ACCENTED + #13#10, FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Publish_EmptySubject_Raises;
begin
  OpenAndHandshake;

  // it used to Exit silently, which is easy to debug past for a long time
  Assert.WillRaise(
    procedure
    begin
      FConn.Publish('', 'hello');
    end,
    ENatsException, 'publishing without a subject must not be silently ignored');

  Assert.AreEqual('', FSocket.ClientText, 'and nothing must reach the socket');
end;

procedure TNatsConnectionProtocolTests.Publish_SubjectWithWhitespace_Raises;
begin
  OpenAndHandshake;

  { A space would end the subject token and shift every field after it, so the
    server would read a different message than the one intended - and a line
    break would desynchronize the stream outright }
  Assert.WillRaise(
    procedure
    begin
      FConn.Publish('foo bar', 'hello');
    end,
    ENatsException);

  Assert.WillRaise(
    procedure
    begin
      FConn.Publish('foo'#13#10'PUB evil 3', 'hello');
    end,
    ENatsException, 'a subject must not be able to inject a second command');

  Assert.AreEqual('', FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Subscribe_EmptySubject_Raises;
begin
  OpenAndHandshake;

  Assert.WillRaise(
    procedure
    begin
      FConn.Subscribe('', LogHandler());
    end,
    ENatsException);
end;

procedure TNatsConnectionProtocolTests.Msg_BinaryPayload_IsDeliveredIntact;
var
  LSid: Integer;
  LSeen: TNatsTestLog;
begin
  OpenAndHandshake;

  LSeen := FMsgLog;
  LSid := FConn.Subscribe('bin',
    procedure (const AMsg: TNatsArgsMSG)
    begin
      LSeen.AddFmt('%d:%d,%d',
        [Length(AMsg.PayloadData), AMsg.PayloadData[0], AMsg.PayloadData[1]]);
    end);

  // 0x00 and 0xFF are not valid UTF-8; the string form cannot represent them
  FSocket.ServerSend(Format('MSG bin %d 3'#13#10, [LSid]));
  FSocket.ServerSendBytes([0, 255, 65]);
  FSocket.ServerSend(#13#10);

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end),
    'the binary message was never dispatched');
  Assert.AreEqual('3:0,255', FMsgLog.Item(0), 'binary payloads must survive byte for byte');
end;

procedure TNatsConnectionProtocolTests.Subscribe_WritesSubFrame;
begin
  OpenAndHandshake;

  FConn.Subscribe('foo.bar', LogHandler());

  Assert.AreEqual('SUB foo.bar 1'#13#10, FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Subscribe_WithQueueGroup_WritesQueueName;
begin
  OpenAndHandshake;

  FConn.Subscribe('foo.bar', 'workers', LogHandler());

  Assert.AreEqual('SUB foo.bar workers 1'#13#10, FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Subscribe_AssignsIncrementingSids;
begin
  OpenAndHandshake;

  Assert.AreEqual(1, FConn.Subscribe('a', LogHandler()));
  Assert.AreEqual(2, FConn.Subscribe('b', LogHandler()));
  Assert.AreEqual(2, Length(FConn.GetSubscriptionList));
end;

procedure TNatsConnectionProtocolTests.Unsubscribe_WritesUnsubFrame;
var
  LSid: Integer;
begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('foo.bar', LogHandler());
  FSocket.ClearClientData;

  FConn.Unsubscribe(LSid);

  Assert.AreEqual('UNSUB 1'#13#10, FSocket.ClientText);
  Assert.AreEqual(0, Length(FConn.GetSubscriptionList), 'the subscription must be forgotten');
end;

procedure TNatsConnectionProtocolTests.Unsubscribe_WithMaxMessages_WritesMaxCount;
var
  LSid: Integer;
begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('foo.bar', LogHandler());
  FSocket.ClearClientData;

  FConn.Unsubscribe(LSid, 5);

  Assert.AreEqual('UNSUB 1 5'#13#10, FSocket.ClientText);
  Assert.AreEqual(1, Length(FConn.GetSubscriptionList),
    'the subscription stays until the server has delivered the remaining messages');
end;

procedure TNatsConnectionProtocolTests.Unsubscribe_WithMax_CountsMessagesAlreadyReceived;
var
  LSid: Integer;

  { ASCII payloads only: the frame declares Length(APayload) as a byte count }
  procedure DeliverAndWait(const APayload: string; AExpectedCount: Integer);
  begin
    FSocket.ServerSend(Format('MSG foo.bar %d %d'#13#10'%s'#13#10,
      [LSid, Length(APayload), APayload]));
    WaitForCondition(
      function: Boolean
      begin
        Result := FMsgLog.Count >= AExpectedCount;
      end);
  end;

begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('foo.bar', LogHandler());

  DeliverAndWait('one', 1);
  DeliverAndWait('two', 2);
  DeliverAndWait('333', 3);
  Assert.AreEqual(3, FMsgLog.Count, 'setup: three messages should have arrived');

  // the server has already delivered 3, so "stop after 5 in total" leaves 2
  FConn.Unsubscribe(LSid, 5);
  Assert.AreEqual(1, Length(FConn.GetSubscriptionList), 'two messages are still expected');

  DeliverAndWait('four', 4);
  Assert.AreEqual(1, Length(FConn.GetSubscriptionList), 'one message is still expected');

  DeliverAndWait('five', 5);
  Assert.AreEqual(0, Length(FConn.GetSubscriptionList),
    'the subscription must be dropped once max_msgs messages have arrived in total');
end;

procedure TNatsConnectionProtocolTests.Unsubscribe_WithMaxAlreadyReached_DropsTheSubscriptionAtOnce;
var
  LSid: Integer;
begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('foo.bar', LogHandler());

  FSocket.ServerSend(Format('MSG foo.bar %d 3'#13#10'one'#13#10, [LSid]));
  FSocket.ServerSend(Format('MSG foo.bar %d 3'#13#10'two'#13#10, [LSid]));
  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count >= 2;
    end),
    'setup: two messages should have arrived');
  FSocket.ClearClientData;

  // the count is already reached: the server drops the subscription as soon as
  // it reads this UNSUB, so the client must not keep it either
  FConn.Unsubscribe(LSid, 2);

  Assert.AreEqual('UNSUB 1 2'#13#10, FSocket.ClientText, 'the server must still be told');
  Assert.AreEqual(0, Length(FConn.GetSubscriptionList),
    'a subscription that has already reached max_msgs must not linger');
end;

procedure TNatsConnectionProtocolTests.Unsubscribe_WithMax_DropsTheSubscriptionAfterTheLastMessage;
var
  LSid: Integer;
begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('foo.bar', LogHandler());

  // nothing received yet, so max_msgs and "how many more" coincide here
  FConn.Unsubscribe(LSid, 2);
  Assert.AreEqual(1, Length(FConn.GetSubscriptionList));

  FSocket.ServerSend(Format('MSG foo.bar %d 3'#13#10'one'#13#10, [LSid]));
  FSocket.ServerSend(Format('MSG foo.bar %d 3'#13#10'two'#13#10, [LSid]));

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := Length(FConn.GetSubscriptionList) = 0;
    end),
    'the subscription must be dropped after the second message');
  Assert.AreEqual(2, FMsgLog.Count, 'both messages must still reach the handler');
end;

procedure TNatsConnectionProtocolTests.Unsubscribe_BySubject_WritesUnsubFrame;
begin
  OpenAndHandshake;
  FConn.Subscribe('foo.bar', LogHandler());
  FSocket.ClearClientData;

  FConn.Unsubscribe('foo.bar');

  Assert.AreEqual('UNSUB 1'#13#10, FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Unsubscribe_UnknownSubject_Raises;
begin
  OpenAndHandshake;

  Assert.WillRaise(
    procedure
    begin
      FConn.Unsubscribe('never.subscribed');
    end,
    ENatsException);
end;

procedure TNatsConnectionProtocolTests.Msg_IsDispatchedToTheSubscriptionHandler;
var
  LSid: Integer;
begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('foo.bar', LogHandler());

  FSocket.ServerSend(Format('MSG foo.bar %d 5'#13#10'hello'#13#10, [LSid]));

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end),
    'the message was never dispatched');
  Assert.AreEqual('foo.bar||hello', FMsgLog.Item(0));
end;

procedure TNatsConnectionProtocolTests.Msg_EmptyPayload_IsDispatched;
var
  LSid: Integer;
begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('foo.bar', LogHandler());

  FSocket.ServerSend(Format('MSG foo.bar %d 0'#13#10#13#10, [LSid]));

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end),
    'a zero length message must still be dispatched');
  Assert.AreEqual('foo.bar||', FMsgLog.Item(0));
end;

procedure TNatsConnectionProtocolTests.Msg_TwoFramesInOneRead_AreBothDispatched;
var
  LSid: Integer;
begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('foo.bar', LogHandler());

  FSocket.ServerSend(Format(
    'MSG foo.bar %0:d 3'#13#10'one'#13#10 +
    'MSG foo.bar %0:d 3'#13#10'two'#13#10, [LSid]));

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count >= 2;
    end),
    'both frames must be dispatched, log was: ' + FMsgLog.Text);
  Assert.AreEqual('foo.bar||one', FMsgLog.Item(0));
  Assert.AreEqual('foo.bar||two', FMsgLog.Item(1));
end;

procedure TNatsConnectionProtocolTests.Msg_ForUnknownSid_IsIgnored;
begin
  OpenAndHandshake;
  FConn.Subscribe('foo.bar', LogHandler());

  FSocket.ServerSend('MSG foo.bar 99 5'#13#10'hello'#13#10);
  FSocket.ServerSend('MSG foo.bar 1 5'#13#10'world'#13#10);

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end),
    'the known subscription must still receive its message');
  Assert.AreEqual(1, FMsgLog.Count, 'a message for an unknown sid must be dropped');
  Assert.AreEqual('foo.bar||world', FMsgLog.Item(0));
end;

procedure TNatsConnectionProtocolTests.Msg_CarriesReplyToSubject;
var
  LSid: Integer;
begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('svc.time', LogHandler());

  FSocket.ServerSend(Format('MSG svc.time %d _INBOX.42 4'#13#10'ping'#13#10, [LSid]));

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end));
  Assert.AreEqual('svc.time|_INBOX.42|ping', FMsgLog.Item(0));
end;

procedure TNatsConnectionProtocolTests.Hpub_WritesSpecCompliantFrame;
var
  LHeaders: TNatsHeaders;
begin
  OpenAndHandshake;

  LHeaders := nil;
  LHeaders.Add('K', 'V');
  FConn.Publish('foo', 'hello', '', LHeaders);

  // header block = 'NATS/1.0'#13#10 (10) + 'K: V'#13#10 (6) + #13#10 (2) = 18 bytes
  // total = 18 + 5 = 23
  // [KNOWN BUG §2 + §3]
  Assert.AreEqual(
    'HPUB foo 18 23'#13#10 +
    'NATS/1.0'#13#10'K: V'#13#10#13#10 +
    'hello'#13#10,
    FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Hpub_DeclaredCountsMatchTheBytesWritten;
var
  LHeaders: TNatsHeaders;
  LBytes: TBytes;
  LText, LLine, LHeaderBlock: string;
  LParts: TArray<string>;
  LCrLfPos, LControlLen, LDeclaredHeader, LDeclaredTotal, LBodyLen: Integer;
begin
  OpenAndHandshake;

  LHeaders := nil;
  LHeaders.Add('K', 'V');
  FConn.Publish('foo', 'hello', '', LHeaders);

  LBytes := FSocket.ClientBytes;
  LText := TEncoding.UTF8.GetString(LBytes);

  LCrLfPos := Pos(NatsConstants.CR_LF, LText);
  Assert.IsTrue(LCrLfPos > 0, 'no control line was written');

  LLine := Copy(LText, 1, LCrLfPos - 1);
  LParts := LLine.Split([NatsConstants.SPC]);
  Assert.AreEqual(NatsConstants.Protocol.HPUB, LParts[0], 'publishing with headers must use HPUB');

  LDeclaredHeader := StrToInt(LParts[Length(LParts) - 2]);
  LDeclaredTotal := StrToInt(LParts[Length(LParts) - 1]);

  LControlLen := Length(TEncoding.UTF8.GetBytes(LLine)) + NatsConstants.CR_LF_LEN;
  LBodyLen := Length(LBytes) - LControlLen;

  // [KNOWN BUG §2] the declared total must cover every byte between the control
  // line and the message's trailing CRLF
  Assert.AreEqual(LDeclaredTotal + NatsConstants.CR_LF_LEN, LBodyLen,
    '<#total bytes> must equal the header block plus the payload');

  LHeaderBlock := TEncoding.UTF8.GetString(Copy(LBytes, LControlLen, LDeclaredHeader));
  Assert.IsTrue(LHeaderBlock.StartsWith(NatsConstants.CLIENT_HEADER_VERSION),
    'the header block must start with NATS/1.0, got: ' + LHeaderBlock);
  Assert.IsTrue(LHeaderBlock.EndsWith(NatsConstants.CR_LF + NatsConstants.CR_LF),
    '<#header bytes> must include the blank line that terminates the header block');
end;

procedure TNatsConnectionProtocolTests.Hpub_WithoutHeaders_FallsBackToPub;
var
  LHeaders: TNatsHeaders;
begin
  OpenAndHandshake;

  LHeaders := nil;
  FConn.Publish('foo', 'hello', '', LHeaders);

  Assert.AreEqual('PUB foo 5'#13#10'hello'#13#10, FSocket.ClientText,
    'an empty header set must not produce an HPUB');
end;

procedure TNatsConnectionProtocolTests.Hmsg_PayloadIsDeliveredIntact;
var
  LSid: Integer;
begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('foo', LogHandler());

  // header block is 18 bytes, payload 5, total 23
  FSocket.ServerSend(Format('HMSG foo %d 18 23'#13#10, [LSid]) +
    'NATS/1.0'#13#10'K: V'#13#10#13#10 + 'hello'#13#10);

  // [KNOWN BUG §1] the reader consumes an extra line after the header block
  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end),
    'the HMSG was never dispatched');
  Assert.AreEqual('foo||hello', FMsgLog.Item(0));
end;

procedure TNatsConnectionProtocolTests.Hmsg_HeadersAreDeliveredToTheHandler;
var
  LSid: Integer;
  LSeen: TNatsTestLog;
begin
  OpenAndHandshake;

  LSeen := FMsgLog;
  LSid := FConn.Subscribe('foo',
    procedure (const AMsg: TNatsArgsMSG)
    var
      LHeaders: TNatsHeaders;
    begin
      LHeaders := AMsg.Headers;
      LSeen.AddFmt('count=%d;K=%s', [LHeaders.Count, LHeaders.GetHeader('K')]);
    end);

  FSocket.ServerSend(Format('HMSG foo %d 18 23'#13#10, [LSid]) +
    'NATS/1.0'#13#10'K: V'#13#10#13#10 + 'hello'#13#10);

  // [KNOWN BUG §1 + §4]
  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end),
    'the HMSG was never dispatched');
  Assert.AreEqual('count=1;K=V', FMsgLog.Item(0));
end;

procedure TNatsConnectionProtocolTests.Request_SubscribesToAnInbox;
begin
  OpenAndHandshake;

  FConn.Request('svc.time', 'ping', LogHandler());

  Assert.IsTrue(FSocket.ClientText.Contains(NatsConstants.Protocol.SUB + ' ' +
    NatsConstants.INBOX_PREFIX), 'Request must subscribe to a fresh inbox, wrote: ' + FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.Request_PublishesWithTheInboxAsReplyTo;
var
  LText: string;
begin
  OpenAndHandshake;

  FConn.Request('svc.time', 'ping', LogHandler());

  LText := FSocket.ClientText;
  Assert.IsTrue(LText.Contains('PUB svc.time ' + NatsConstants.INBOX_PREFIX),
    'Request must publish with the inbox as reply-to, wrote: ' + LText);
  Assert.IsTrue(LText.EndsWith('ping'#13#10), 'the request payload must be sent, wrote: ' + LText);
end;

procedure TNatsConnectionProtocolTests.Request_AutoUnsubscribesAfterOneReply;
var
  LSid: Integer;
  LText: string;
  LUnsubPos, LPubPos: Integer;
begin
  OpenAndHandshake;

  LSid := FConn.Request('svc.time', 'ping', LogHandler());

  // §6: without UNSUB <sid> 1 the inbox subscription leaks on both sides
  LText := FSocket.ClientText;
  LUnsubPos := Pos(Format('%s %d 1%s', [NatsConstants.Protocol.UNSUB, LSid, NatsConstants.CR_LF]), LText);
  LPubPos := Pos(NatsConstants.Protocol.PUB + ' svc.time ', LText);

  Assert.IsTrue(LUnsubPos > 0,
    'Request must auto-unsubscribe after one reply, wrote: ' + LText);
  Assert.IsTrue(LPubPos > LUnsubPos,
    'the auto-unsubscribe must be armed before the request is published, wrote: ' + LText);
end;

procedure TNatsConnectionProtocolTests.Request_SubscriptionIsDroppedAfterTheReply;
var
  LSid: Integer;
  LInbox: string;
  LLines: TArray<string>;
begin
  OpenAndHandshake;

  LSid := FConn.Request('svc.time', 'ping', LogHandler());
  Assert.AreEqual(1, Length(FConn.GetSubscriptionList), 'the inbox subscription must be live');

  // first line written is "SUB <inbox> <sid>"
  LLines := FSocket.ClientText.Split([NatsConstants.CR_LF]);
  LInbox := LLines[0].Split([NatsConstants.SPC])[1];
  Assert.IsTrue(LInbox.StartsWith(NatsConstants.INBOX_PREFIX), 'unexpected SUB line: ' + LLines[0]);

  FSocket.ServerSend(Format('MSG %s %d 4'#13#10'pong'#13#10, [LInbox, LSid]));

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end),
    'the reply never reached the handler');
  Assert.AreEqual(LInbox + '||pong', FMsgLog.Item(0));
  Assert.AreEqual(0, Length(FConn.GetSubscriptionList),
    'the inbox subscription must be gone once the single reply has arrived');
end;

procedure TNatsConnectionProtocolTests.NewInbox_IsUniqueAcrossConnections;
var
  LOther: TNatsConnection;
begin
  LOther := TNatsConnection.Create;
  try
    // §6: a per-connection counter would have every client in the network
    // starting at _INBOX.1, so replies could reach the wrong one
    Assert.AreNotEqual(FConn.GetNewInbox, LOther.GetNewInbox,
      'inbox subjects must be unique across clients, not just within one connection');
    Assert.AreNotEqual(FConn.GetNewInbox, FConn.GetNewInbox,
      'and unique per call within one connection');
  finally
    LOther.Free;
  end;
end;

procedure TNatsConnectionProtocolTests.Hmsg_StatusMessage_CarriesTheStatusCode;
var
  LSid: Integer;
  LSeen: TNatsTestLog;
begin
  OpenAndHandshake;

  LSeen := FMsgLog;
  LSid := FConn.Subscribe('foo',
    procedure (const AMsg: TNatsArgsMSG)
    begin
      LSeen.AddFmt('%d|%s|%s|%d', [AMsg.Status, AMsg.Description, AMsg.Payload,
        Ord(AMsg.HasStatus)]);
    end);

  { 'NATS/1.0 404 No Messages'#13#10 is 26 bytes, + the blank line = 28, and a
    status message has no payload at all, so total = 28 }
  FSocket.ServerSend(Format('HMSG foo %d 28 28'#13#10, [LSid]) +
    'NATS/1.0 404 No Messages'#13#10#13#10 + #13#10);

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end),
    'the status message was never dispatched');
  Assert.AreEqual('404|No Messages||1', FMsgLog.Item(0),
    'a pull consumer cannot work until 404 reaches it as a status, not as an empty message');
end;

procedure TNatsConnectionProtocolTests.Hmsg_EmptyMessage_HasNoStatus;
var
  LSid: Integer;
  LSeen: TNatsTestLog;
begin
  OpenAndHandshake;

  LSeen := FMsgLog;
  LSid := FConn.Subscribe('foo',
    procedure (const AMsg: TNatsArgsMSG)
    begin
      LSeen.AddFmt('%d|%s|%s|%d', [AMsg.Status, AMsg.Description, AMsg.Payload,
        Ord(AMsg.HasStatus)]);
    end);

  // a real message that happens to have headers and an empty body: 18-byte
  // header block, no payload
  FSocket.ServerSend(Format('HMSG foo %d 18 18'#13#10, [LSid]) +
    'NATS/1.0'#13#10'K: V'#13#10#13#10 + #13#10);

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end),
    'the empty message was never dispatched');
  Assert.AreEqual('0|||0', FMsgLog.Item(0),
    'an empty message must NOT look like a status - that is the distinction §2 exists for');
end;

procedure TNatsConnectionProtocolTests.Msg_WithoutHeaderBlock_HasNoStatus;
var
  LSid: Integer;
  LSeen: TNatsTestLog;
begin
  OpenAndHandshake;

  LSeen := FMsgLog;
  LSid := FConn.Subscribe('foo',
    procedure (const AMsg: TNatsArgsMSG)
    begin
      LSeen.AddFmt('%d|%d', [AMsg.Status, Ord(AMsg.HasStatus)]);
    end);

  FSocket.ServerSend(Format('MSG foo %d 5'#13#10'hello'#13#10, [LSid]));

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FMsgLog.Count > 0;
    end),
    'the message was never dispatched');
  Assert.AreEqual('0|0', FMsgLog.Item(0),
    'a plain MSG has no header block, so it can never carry a status');
end;

procedure TNatsConnectionProtocolTests.RequestSync_WritesSubArmUnsubThenPublish;
var
  LReply: TNatsArgsMSG;
  LInbox, LSid: string;
  LLines, LParts: TArray<string>;
begin
  OpenAndHandshake;

  FConn.RequestSync('svc.time', 'ping', LReply, REQUEST_TIMEOUT);

  LLines := FSocket.ClientText.Split([NatsConstants.CR_LF]);
  LParts := LLines[0].Split([NatsConstants.SPC]);   // SUB <inbox> <sid>

  Assert.AreEqual(NatsConstants.Protocol.SUB, LParts[0],
    'the inbox must be subscribed first: a fast responder can reply before Publish returns');
  LInbox := LParts[1];
  LSid := LParts[2];
  Assert.IsTrue(LInbox.StartsWith(NatsConstants.INBOX_PREFIX), 'unexpected SUB line: ' + LLines[0]);

  Assert.AreEqual(Format('%s %s 1', [NatsConstants.Protocol.UNSUB, LSid]), LLines[1],
    'the one-reply auto-unsubscribe must be armed before the request goes out');
  Assert.AreEqual(Format('%s svc.time %s 4', [NatsConstants.Protocol.PUB, LInbox]), LLines[2],
    'the request must be published with the inbox as reply-to');
  Assert.AreEqual('ping', LLines[3]);
end;

procedure TNatsConnectionProtocolTests.RequestSync_ReturnsTheReply;
var
  LReply: TNatsArgsMSG;
begin
  OpenAndHandshake;
  ReplyWhenSubscribed('pong');

  Assert.IsTrue(FConn.RequestSync('svc.time', 'ping', LReply),
    'RequestSync must report that a reply arrived');
  Assert.AreEqual('pong', LReply.Payload,
    'the reply must be copied out of the consumer thread intact');
end;

procedure TNatsConnectionProtocolTests.RequestSync_Reply_LeavesNoSubscription;
var
  LReply: TNatsArgsMSG;
begin
  OpenAndHandshake;
  ReplyWhenSubscribed('pong');

  Assert.IsTrue(FConn.RequestSync('svc.time', 'ping', LReply));
  Assert.AreEqual(0, Length(FConn.GetSubscriptionList),
    'the inbox subscription must be gone once the reply has arrived');
end;

procedure TNatsConnectionProtocolTests.RequestSync_Timeout_ReturnsFalse;
var
  LReply: TNatsArgsMSG;
begin
  OpenAndHandshake;

  Assert.IsFalse(FConn.RequestSync('svc.time', 'ping', LReply, REQUEST_TIMEOUT),
    'a request nobody answers must time out, not succeed');
end;

procedure TNatsConnectionProtocolTests.RequestSync_Timeout_RemovesTheSubscription;
var
  LReply: TNatsArgsMSG;
begin
  OpenAndHandshake;

  FConn.RequestSync('svc.time', 'ping', LReply, REQUEST_TIMEOUT);

  Assert.AreEqual(0, Length(FConn.GetSubscriptionList),
    'a timed-out request must not leave its inbox subscribed - that leak is one ' +
    'entry per call, and every JetStream operation is a request');
end;

procedure TNatsConnectionProtocolTests.RequestSync_Timeout_UnsubscribesOnTheWire;
var
  LReply: TNatsArgsMSG;
  LText: string;
  LLines, LParts: TArray<string>;
begin
  OpenAndHandshake;

  FConn.RequestSync('svc.time', 'ping', LReply, REQUEST_TIMEOUT);

  LText := FSocket.ClientText;
  LLines := LText.Split([NatsConstants.CR_LF]);
  LParts := LLines[0].Split([NatsConstants.SPC]);   // SUB <inbox> <sid>

  { The server delivered none of the one message the auto-unsubscribe allowed,
    so it still holds the subscription and has to be told explicitly. Note this
    is the bare "UNSUB <sid>", which the armed "UNSUB <sid> 1" does not match }
  Assert.IsTrue(LText.Contains(Format('%s %s%s',
    [NatsConstants.Protocol.UNSUB, LParts[2], NatsConstants.CR_LF])),
    'a timed-out request must unsubscribe its inbox on the server too, wrote: ' + LText);
end;

procedure TNatsConnectionProtocolTests.RequestSync_WithHeaders_WritesHpub;
var
  LReply: TNatsArgsMSG;
  LHeaders: TNatsHeaders;
  LText: string;
begin
  OpenAndHandshake;

  LHeaders := nil;
  LHeaders.Add('Nats-Msg-Id', 'abc');

  FConn.RequestSync('foo', TEncoding.UTF8.GetBytes('hello'), LHeaders, LReply, REQUEST_TIMEOUT);

  LText := FSocket.ClientText;
  Assert.IsTrue(LText.Contains(NatsConstants.Protocol.HPUB + ' foo ' + NatsConstants.INBOX_PREFIX),
    'a request carrying headers must go out as HPUB with the inbox as reply-to, wrote: ' + LText);
  Assert.IsTrue(LText.Contains('Nats-Msg-Id: abc'),
    'the header must reach the wire, wrote: ' + LText);
end;

procedure TNatsConnectionProtocolTests.RequestSync_ConnectionClosedWhileWaiting_Raises;
var
  LReply: TNatsArgsMSG;
  LSocket: TNatsMockSocket;
begin
  OpenAndHandshake;

  LSocket := FSocket;
  FServerThread := TThread.CreateAnonymousThread(
    procedure
    begin
      // drop the connection instead of answering
      if LSocket.WaitForClientText(NatsConstants.Protocol.SUB + ' ' +
           NatsConstants.INBOX_PREFIX) then
        LSocket.Close;
    end);
  FServerThread.FreeOnTerminate := False;
  FServerThread.Start;

  { A connection that goes away is not the same outcome as a responder that is
    slow, and the caller must not have to sit out the whole timeout to find out }
  Assert.WillRaise(
    procedure
    begin
      FConn.RequestSync('svc.time', 'ping', LReply, 3000);
    end,
    ENatsException,
    'a connection lost mid-request must raise, not look like a timeout');
end;

procedure TNatsConnectionProtocolTests.RequestSync_NotConnected_Raises;
var
  LReply: TNatsArgsMSG;
begin
  // no handshake at all: the socket was never opened
  Assert.WillRaise(
    procedure
    begin
      FConn.RequestSync('svc.time', 'ping', LReply, REQUEST_TIMEOUT);
    end,
    ENatsException);
end;

procedure TNatsConnectionProtocolTests.RequestSync_ZeroTimeout_Raises;
var
  LReply: TNatsArgsMSG;
begin
  OpenAndHandshake;

  Assert.WillRaise(
    procedure
    begin
      FConn.RequestSync('svc.time', 'ping', LReply, 0);
    end,
    ENatsException,
    'a zero timeout would block forever, which is never what the caller meant');
end;

procedure TNatsConnectionProtocolTests.MaxPayload_IsTakenFromInfo;
begin
  Assert.AreEqual(0, FConn.MaxPayload, 'nothing is known before the handshake');

  OpenAndHandshake(INFO_JSON_SMALL);

  Assert.AreEqual(SMALL_MAX_PAYLOAD, FConn.MaxPayload,
    'the limit must be taken from the server''s INFO');
end;

procedure TNatsConnectionProtocolTests.MaxLineLength_IsNotScaledOffMaxPayload;
var
  LSocket: INatsSocket;
begin
  { max_payload * 2 overflows Integer above ~1 GiB: the old code wrapped to a
    negative value and assigned a nonsense Cardinal cap (here: 4294967294).
    Control lines never carry payloads, so the cap must be a fixed, generous
    ceiling whatever the server declares }
  LSocket := FSocket;

  OpenAndHandshake(INFO_JSON_HUGE_MAX_PAYLOAD);

  Assert.AreEqual(Cardinal(1024 * 1024), LSocket.MaxLineLength,
    'the line cap must be the fixed constant, not something derived from max_payload');
end;

procedure TNatsConnectionProtocolTests.Publish_OversizedPayload_Raises;
begin
  OpenAndHandshake(INFO_JSON_SMALL);

  Assert.WillRaise(
    procedure
    begin
      FConn.Publish('foo', StringOfChar('x', SMALL_MAX_PAYLOAD + 1));
    end,
    ENatsMaxPayloadError,
    'the server would answer this with -ERR and close the connection');
end;

procedure TNatsConnectionProtocolTests.Publish_OversizedPayload_WritesNothing;
begin
  OpenAndHandshake(INFO_JSON_SMALL);

  try
    FConn.Publish('foo', StringOfChar('x', SMALL_MAX_PAYLOAD + 1));
  except
    on E: ENatsMaxPayloadError do ;   // expected, asserted by the test above
  end;

  { Nothing may reach the wire: a half-written PUB would desynchronise the
    stream, which is worse than the violation it is avoiding }
  Assert.AreEqual('', FSocket.ClientText,
    'a refused publish must not put a single byte on the wire');
end;

procedure TNatsConnectionProtocolTests.Publish_ExactlyMaxPayload_IsAllowed;
begin
  OpenAndHandshake(INFO_JSON_SMALL);

  // the limit is inclusive: max_payload bytes is legal, one more is not
  FConn.Publish('foo', StringOfChar('x', SMALL_MAX_PAYLOAD));

  Assert.AreEqual(Format('PUB foo %d'#13#10'%s'#13#10,
    [SMALL_MAX_PAYLOAD, StringOfChar('x', SMALL_MAX_PAYLOAD)]), FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.PublishBytes_OversizedPayload_Raises;
var
  LData: TBytes;
begin
  OpenAndHandshake(INFO_JSON_SMALL);

  SetLength(LData, SMALL_MAX_PAYLOAD + 1);

  Assert.WillRaise(
    procedure
    begin
      FConn.PublishBytes('foo', LData);
    end,
    ENatsMaxPayloadError,
    'the binary path must be checked too, not just the string one');
end;

procedure TNatsConnectionProtocolTests.Hpub_OversizedTotal_Raises;
var
  LHeaders: TNatsHeaders;
begin
  OpenAndHandshake(INFO_JSON_SMALL);

  LHeaders := nil;
  LHeaders.Add('K', 'V');

  { The payload alone is 20 bytes, under the limit of 32. The header block adds
    18, so <#total bytes> is 38 - and that total is what the server measures.
    Checking only the payload here would let this through and lose the
    connection anyway }
  Assert.WillRaise(
    procedure
    begin
      FConn.Publish('foo', StringOfChar('x', 20), '', LHeaders);
    end,
    ENatsMaxPayloadError,
    'headers count towards max_payload, because HPUB declares a total');
end;

procedure TNatsConnectionProtocolTests.Publish_BeforeInfo_IsNotSizeChecked;
begin
  { No handshake, so no limit is known. Refusing here would be guessing, and
    the publish fails on the socket anyway }
  Assert.AreEqual(0, FConn.MaxPayload);
  Assert.WillNotRaise(
    procedure
    begin
      try
        FConn.Publish('foo', StringOfChar('x', 10000));
      except
        on E: ENatsMaxPayloadError do
          raise;              // the only failure this test cares about
        on E: Exception do ;  // the socket is not open - not our concern here
      end;
    end,
    ENatsMaxPayloadError);
end;

procedure TNatsConnectionProtocolTests.MaxPayload_LaterInfo_UpdatesTheLimit;
begin
  OpenAndHandshake(INFO_JSON);
  Assert.AreEqual(1048576, FConn.MaxPayload, 'guard: the first INFO set the limit');

  { A second INFO - a cluster reconfiguration - must not drive a second CONNECT,
    but it must still update the limit }
  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + INFO_JSON_SMALL);

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FConn.MaxPayload = SMALL_MAX_PAYLOAD;
    end),
    'a later INFO must update max_payload, not be ignored with the handshake');
end;

initialization
  TDUnitX.RegisterTestFixture(TNatsSocketRegistryTests);
  TDUnitX.RegisterTestFixture(TNatsMockSocketTests);
  TDUnitX.RegisterTestFixture(TNatsChannelTimeoutTests);
  TDUnitX.RegisterTestFixture(TNatsConnectionProtocolTests);

end.
