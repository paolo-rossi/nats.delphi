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
  System.SysUtils, System.Classes,

  DUnitX.TestFramework,

  Nats.Consts,
  Nats.Classes,
  Nats.Entities,
  Nats.Socket,
  Nats.Socket.Indy,
  Nats.Connection,
  Nats.Nuid,
  Nats.Exceptions,

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

    // Covers §1, §2, §3, §4 and §19 together: publishing headers and getting
    // them back is only possible if all five are right
    [Test]
    procedure Headers_RoundTripThroughTheServer;
  end;

implementation

const
  LIVE_HOST = '127.0.0.1';
  LIVE_TIMEOUT = 5000;
  WAIT_MS = 5000;

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
      FServerName := AInfo.server_name;
      FServerVersion := AInfo.version;
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

  FConn.Unsubscribe(Cardinal(LSid));
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

initialization
  TDUnitX.RegisterTestFixture(TNatsLiveServerTests);

end.
