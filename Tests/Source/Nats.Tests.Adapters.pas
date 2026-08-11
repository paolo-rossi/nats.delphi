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
    procedure OpenAndHandshake;
    function LogHandler: TNatsMsgHandler;
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
    procedure Publish_EmptySubject_WritesNothing;

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

    { request / inbox }

    [Test]
    procedure Request_SubscribesToAnInbox;
    [Test]
    procedure Request_PublishesWithTheInboxAsReplyTo;
    // [KNOWN BUG §6]
    [Test]
    procedure Request_AutoUnsubscribesAfterOneReply;
    // [KNOWN BUG §6]
    [Test]
    procedure NewInbox_IsUniqueAcrossConnections;
  end;

implementation

const
  INFO_JSON =
    '{"server_id":"NDHJZQZ4YQXQ","server_name":"nats-1","version":"2.10.11",' +
    '"proto":1,"host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,' +
    '"jetstream":true,"client_id":7,"client_ip":"127.0.0.1"}';

  MOCK_TIMEOUT = 200;

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
  FSocket.Timeout := MOCK_TIMEOUT;
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
    ENatsMock, 'the mock must time out like Indy does, never block forever');
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
  FConn.Free;   // may fire the disconnect handler, which writes to FLog
  FMsgLog.Free;
  FLog.Free;
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
  FConn.Open(
    procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
    begin
      FLog.Add('CONNECT:' + AInfo.server_name);
      if FConnectUser <> '' then
        AConnectOptions.user := FConnectUser;
    end,
    procedure
    begin
      FLog.Add('DISCONNECT');
    end);

  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + INFO_JSON);

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
  Assert.IsTrue(FConn.ConnectOptions.user = 'joe', 'the handler must be able to set ConnectOptions');
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

procedure TNatsConnectionProtocolTests.Publish_EmptySubject_WritesNothing;
begin
  OpenAndHandshake;

  FConn.Publish('', 'hello');

  Assert.AreEqual('', FSocket.ClientText, 'publishing without a subject must not write to the socket');
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

  FConn.Unsubscribe(Cardinal(LSid));

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

  FConn.Unsubscribe(Cardinal(LSid), 5);

  Assert.AreEqual('UNSUB 1 5'#13#10, FSocket.ClientText);
  Assert.AreEqual(1, Length(FConn.GetSubscriptionList),
    'the subscription stays until the server has delivered the remaining messages');
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
begin
  OpenAndHandshake;

  FConn.Request('svc.time', 'ping', LogHandler());

  // [KNOWN BUG §6] without UNSUB <sid> 1 the inbox subscription leaks on both
  // the client and the server
  Assert.IsTrue(FSocket.ClientText.Contains(NatsConstants.Protocol.UNSUB),
    'Request must auto-unsubscribe after one reply, wrote: ' + FSocket.ClientText);
end;

procedure TNatsConnectionProtocolTests.NewInbox_IsUniqueAcrossConnections;
var
  LOther: TNatsConnection;
begin
  LOther := TNatsConnection.Create;
  try
    // [KNOWN BUG §6] inboxes come from a per-connection counter, so every
    // client in the network starts at _INBOX.1
    Assert.AreNotEqual(FConn.GetNewInbox, LOther.GetNewInbox,
      'inbox subjects must be unique across clients, not just within one connection');
  finally
    LOther.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TNatsSocketRegistryTests);
  TDUnitX.RegisterTestFixture(TNatsMockSocketTests);
  TDUnitX.RegisterTestFixture(TNatsConnectionProtocolTests);

end.
