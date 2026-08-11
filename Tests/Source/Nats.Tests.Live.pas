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
  SETTLE_MS = 250;   // see the comment in Connect

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

  // Open() returns as soon as the TCP socket is up; CONNECT is only sent once
  // the consumer thread has seen INFO (see §16), so publishing before this
  // point can race the handshake
  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FHandshakeDone;
    end,
    WAIT_MS),
    Format('no INFO from nats-server at %s:%d - is one running?',
      [LIVE_HOST, NatsConstants.DEFAULT_PORT]));

  // ------------------------------------------------------------------
  // Deliberate workaround, not padding. The connect handler runs BEFORE
  // SendConnect (Nats.Connection.pas:715-722), so FHandshakeDone is set while
  // CONNECT is still unwritten, and there is no callback for "CONNECT is on
  // the wire" (§16). Worse, SendConnect writes from the consumer thread
  // without taking FLock while SendSubscribe writes from this thread, also
  // without FLock (§10) - so a SUB issued right now can be spliced into the
  // middle of the CONNECT line. Against a real server that costs you the
  // subscription, and the resulting -ERR then trips the §9 self-join deadlock
  // and hangs the whole run.
  //
  // Measured without this sleep: 3 consecutive runs gave 9 passed / 2 failed /
  // hung. That flakiness IS the bug; it is reproduced deterministically and
  // safely by ConcurrentWrites_AreNotInterleavedOnTheSocket in the mock suite.
  // Here we sleep so that the round-trip tests below measure what they claim
  // to measure. Delete this once §10 and §16 are fixed.
  // ------------------------------------------------------------------
  TThread.Sleep(SETTLE_MS);
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
