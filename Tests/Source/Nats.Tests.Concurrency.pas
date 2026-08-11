{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Tests.Concurrency;

{******************************************************************************}
{                                                                              }
{  Thread-safety and connection lifecycle tests.                               }
{                                                                              }
{  Tests marked [KNOWN BUG §n] assert correct behaviour and currently FAIL.     }
{  "n" refers to the section in Docs\Core-Protocol-Review.md.                  }
{                                                                              }
{  Two tests are [Ignore]d on purpose: they reproduce a use-after-free and a    }
{  self-join deadlock, which would take the whole test process down rather      }
{  than report a failure. Enable them once §8 and §9 are fixed.                }
{                                                                              }
{******************************************************************************}

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections,

  DUnitX.TestFramework,

  Nats.Consts,
  Nats.Classes,
  Nats.Entities,
  Nats.Connection,
  Nats.Nuid,
  Nats.Exceptions,

  Nats.Tests.Mocks;

type
  [TestFixture]
  TNatsGeneratorTests = class
  public
    [Test]
    procedure SubscriptionIds_AreUniqueUnderConcurrentLoad;
    [Test]
    procedure Inboxes_AreUniqueUnderConcurrentLoad;
    [Test]
    procedure Nuid_IsUniqueUnderConcurrentLoad;
  end;

  [TestFixture]
  TNatsConnectionConcurrencyTests = class
  private
    FConn: TNatsConnection;
    FSocket: TNatsMockSocket;
    FLog: TNatsTestLog;
    procedure OpenConnection;
    procedure OpenAndHandshake;
    function WaitForLog(const AText: string): Boolean;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    // [KNOWN BUG §10] SendSubscribe writes outside FLock, so a SUB can be
    // spliced between a PUB control line and its payload
    [Test]
    procedure ConcurrentWrites_AreNotInterleavedOnTheSocket;

    // §14: a failure must never be silent
    [Test]
    procedure Reader_UnparsableCommand_TearsDownAndReportsIt;
    [Test]
    procedure Reader_ServerError_ReportsTheServersReason;

    // §15: a server-side disconnect must reach the application
    [Test]
    procedure ServerDisconnect_IsReportedToTheApplication;
    [Test]
    procedure IdleConnection_IsProbedWithAKeepAlivePing;
    [Test]
    procedure MissedPong_TearsDownAndReportsIt;

    // §16: "connected" must mean the handshake finished
    [Test]
    procedure Connected_IsFalseUntilTheHandshakeCompletes;
    [Test]
    procedure WaitForReady_ReturnsFalseWhenNoInfoArrives;
    [Test]
    procedure SecondInfo_DoesNotResendConnect;

    // §17: a session that died on its own must not leak into the next one
    [Test]
    procedure Open_AfterAFailure_StartsACleanSession;

    // §18
    [Test]
    procedure Info_RequiringTls_TearsDownWithAClearError;

    [Test]
    procedure Close_ThenOpen_RestartsTheConnection;
    [Test]
    procedure Close_InvokesTheDisconnectHandler;
    [Test]
    procedure Close_WhenNeverOpened_DoesNotRaise;
    [Test]
    procedure Close_LeavesNoError;

    // §8: the dictionary owns its values, so an Unsubscribe must not free a
    // subscription out from under a dispatch in flight
    [Test]
    procedure Dispatch_WhileUnsubscribing_DoesNotUseFreedMemory;

    // §9: a fatal -ERR must tear the connection down without the consumer
    // thread joining itself
    [Test]
    procedure ServerError_DoesNotDeadlockTheConsumer;
  end;

implementation

const
  INFO_JSON =
    '{"server_id":"NDHJZQZ4YQXQ","server_name":"nats-1","version":"2.10.11",' +
    '"proto":1,"host":"0.0.0.0","port":4222,"headers":true,"max_payload":1048576,' +
    '"jetstream":true,"client_id":7,"client_ip":"127.0.0.1"}';

  MOCK_TIMEOUT = 200;

procedure AssertAllUnique(const AValues: TArray<string>; const AWhat: string);
var
  LSeen: TDictionary<string, Boolean>;
  LValue: string;
begin
  LSeen := TDictionary<string, Boolean>.Create;
  try
    for LValue in AValues do
    begin
      if LSeen.ContainsKey(LValue) then
        Assert.Fail(Format('duplicate %s produced under concurrent load: "%s"', [AWhat, LValue]));
      LSeen.Add(LValue, True);
    end;
    Assert.AreEqual(Length(AValues), LSeen.Count, 'every ' + AWhat + ' must be unique');
  finally
    LSeen.Free;
  end;
end;

{ TNatsGeneratorTests }

procedure TNatsGeneratorTests.SubscriptionIds_AreUniqueUnderConcurrentLoad;
const
  THREADS = 8;
  PER_THREAD = 500;
var
  LGenerator: TNatsGenerator;
  LResults: TNatsTestLog;
begin
  LGenerator := TNatsGenerator.Create;
  LResults := TNatsTestLog.Create;
  try
    RunParallel(THREADS,
      procedure (AIndex: Integer)
      var
        LLoop: Integer;
      begin
        for LLoop := 1 to PER_THREAD do
          LResults.Add(UIntToStr(LGenerator.GetSubNextId));
      end);

    Assert.AreEqual(THREADS * PER_THREAD, LResults.Count);
    AssertAllUnique(LResults.ToArray, 'subscription id');
  finally
    LResults.Free;
    LGenerator.Free;
  end;
end;

procedure TNatsGeneratorTests.Inboxes_AreUniqueUnderConcurrentLoad;
const
  THREADS = 8;
  PER_THREAD = 250;
var
  LGenerator: TNatsGenerator;
  LResults: TNatsTestLog;
begin
  LGenerator := TNatsGenerator.Create;
  LResults := TNatsTestLog.Create;
  try
    RunParallel(THREADS,
      procedure (AIndex: Integer)
      var
        LLoop: Integer;
      begin
        for LLoop := 1 to PER_THREAD do
          LResults.Add(LGenerator.GetNewInbox);
      end);

    AssertAllUnique(LResults.ToArray, 'inbox');
  finally
    LResults.Free;
    LGenerator.Free;
  end;
end;

procedure TNatsGeneratorTests.Nuid_IsUniqueUnderConcurrentLoad;
const
  THREADS = 8;
  PER_THREAD = 500;
var
  LResults: TNatsTestLog;
begin
  LResults := TNatsTestLog.Create;
  try
    RunParallel(THREADS,
      procedure (AIndex: Integer)
      var
        LLoop: Integer;
      begin
        for LLoop := 1 to PER_THREAD do
          LResults.Add(TNUID.NextNuid);
      end);

    Assert.AreEqual(THREADS * PER_THREAD, LResults.Count);
    AssertAllUnique(LResults.ToArray, 'NUID');
  finally
    LResults.Free;
  end;
end;

{ TNatsConnectionConcurrencyTests }

procedure TNatsConnectionConcurrencyTests.Setup;
begin
  UseMockSocket;   // the live fixture flips the process-wide default

  FLog := TNatsTestLog.Create;
  FConn := TNatsConnection.Create;
  FSocket := TNatsMockSocket.LastInstance;
  FConn.Name := 'ConcurrencyConn';
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, MOCK_TIMEOUT);
end;

procedure TNatsConnectionConcurrencyTests.TearDown;
begin
  FConn.Free;
  FLog.Free;
end;

procedure TNatsConnectionConcurrencyTests.OpenConnection;
begin
  FConn.OnError :=
    procedure (const AError: string)
    begin
      FLog.Add('ERROR:' + AError);
    end;

  FConn.Open(
    procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
    begin
      FLog.Add('CONNECT');
    end,
    procedure
    begin
      FLog.Add('DISCONNECT');
    end);
end;

procedure TNatsConnectionConcurrencyTests.OpenAndHandshake;
begin
  OpenConnection;

  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + INFO_JSON);
  Assert.IsTrue(FConn.WaitForReady(3000), 'handshake did not complete');
  FSocket.ClearClientData;
end;

function TNatsConnectionConcurrencyTests.WaitForLog(const AText: string): Boolean;
begin
  Result := WaitForCondition(
    function: Boolean
    var
      LEntry: string;
    begin
      Result := False;
      for LEntry in FLog.ToArray do
        if LEntry.StartsWith(AText) then
          Exit(True);
    end);
end;

procedure TNatsConnectionConcurrencyTests.Reader_UnparsableCommand_TearsDownAndReportsIt;
begin
  OpenAndHandshake;

  { The old version of this test asserted the reader carried on delivering
    afterwards. That was the wrong contract: an unknown protocol operation
    means the stream can no longer be trusted to be aligned, and every other
    NATS client treats it as fatal. What §14 is really about is that the
    failure used to be SILENT - the reader thread died and nobody was told. }
  FSocket.ServerSendLine('NONSENSE not a nats command');

  Assert.IsTrue(WaitForLog('ERROR:Protocol error'),
    'a protocol error must reach the error handler, log was: ' + FLog.Text);
  Assert.IsTrue(FLog.Contains('DISCONNECT'), 'and the connection must be reported as gone');
  Assert.IsFalse(FConn.Connected, 'and it must not still claim to be connected');
  Assert.IsTrue(FConn.LastError.Contains('Protocol error'), 'LastError: ' + FConn.LastError);
end;

procedure TNatsConnectionConcurrencyTests.Reader_ServerError_ReportsTheServersReason;
begin
  OpenAndHandshake;

  FSocket.ServerSendLine(NatsConstants.Protocol.ERR + ' ''Authorization Violation''');

  Assert.IsTrue(WaitForLog('ERROR:'), 'the -ERR must reach the error handler, log was: ' + FLog.Text);
  // the reason used to be discarded by the parser entirely
  Assert.IsTrue(FConn.LastError.Contains('Authorization Violation'),
    'the server''s reason must be preserved, got: ' + FConn.LastError);
end;

procedure TNatsConnectionConcurrencyTests.ServerDisconnect_IsReportedToTheApplication;
begin
  OpenAndHandshake;

  // the peer goes away without a word
  FSocket.Close;

  Assert.IsTrue(WaitForLog('ERROR:'),
    'losing the connection must be reported, not silently ignored; log was: ' + FLog.Text);
  Assert.IsTrue(FLog.Contains('DISCONNECT'), 'the disconnect handler must run');
  Assert.IsFalse(FConn.Connected);
end;

procedure TNatsConnectionConcurrencyTests.IdleConnection_IsProbedWithAKeepAlivePing;
begin
  // short read timeout so "idle" happens within the test's lifetime
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, MOCK_TIMEOUT, 200);
  OpenAndHandshake;

  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.PING),
    'an idle connection must be probed rather than assumed dead or assumed alive');

  // answering the probe keeps it alive
  FSocket.ServerSendLine(NatsConstants.Protocol.PONG);
  TThread.Sleep(300);

  Assert.IsTrue(FConn.Connected, 'a PONG must keep the connection alive');
  Assert.IsFalse(FLog.Contains('DISCONNECT'), 'log was: ' + FLog.Text);
end;

procedure TNatsConnectionConcurrencyTests.MissedPong_TearsDownAndReportsIt;
begin
  FConn.SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, MOCK_TIMEOUT, 200);
  OpenAndHandshake;

  // never answer the keep-alive: the peer is a black hole
  Assert.IsTrue(WaitForLog('ERROR:'),
    'an unanswered keep-alive must tear the connection down; log was: ' + FLog.Text);
  Assert.IsTrue(FConn.LastError.Contains('PONG'), 'LastError: ' + FConn.LastError);
  Assert.IsFalse(FConn.Connected);
end;

procedure TNatsConnectionConcurrencyTests.Connected_IsFalseUntilTheHandshakeCompletes;
begin
  OpenConnection;

  { The socket is up, but the server has not been told who we are, so nothing
    may be published yet - Connected must not claim otherwise }
  Assert.IsFalse(FConn.Connected, 'the socket being open is not the same as being connected');

  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + INFO_JSON);

  Assert.IsTrue(FConn.WaitForReady(3000), 'WaitForReady must return once CONNECT has been written');
  Assert.IsTrue(FConn.Connected);
  Assert.IsTrue(FSocket.ClientText.Contains(NatsConstants.Protocol.CONNECT),
    'and CONNECT really is on the wire by then');
end;

procedure TNatsConnectionConcurrencyTests.WaitForReady_ReturnsFalseWhenNoInfoArrives;
begin
  OpenConnection;

  // no INFO is ever sent
  Assert.IsFalse(FConn.WaitForReady(300), 'a handshake that never happens must not report ready');
end;

procedure TNatsConnectionConcurrencyTests.SecondInfo_DoesNotResendConnect;
begin
  OpenAndHandshake; // clears the captured wire data

  // servers send INFO again on cluster changes and lame duck mode
  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + INFO_JSON);
  TThread.Sleep(300);

  Assert.IsFalse(FSocket.ClientText.Contains(NatsConstants.Protocol.CONNECT),
    'a second CONNECT is a protocol violation, wrote: ' + FSocket.ClientText);
  Assert.IsTrue(FConn.Connected, 'and the connection must survive it');
end;

procedure TNatsConnectionConcurrencyTests.Open_AfterAFailure_StartsACleanSession;
begin
  OpenAndHandshake;

  FSocket.ServerSendLine('NONSENSE not a nats command');
  Assert.IsTrue(WaitForLog('ERROR:'), 'setup: the session should have failed');

  { Re-opening has to reap the threads of the dead session rather than
    overwrite them, and must not inherit its error }
  FSocket.ClearClientData;
  OpenAndHandshake;

  Assert.IsTrue(FConn.Connected, 'the connection must be usable again');
  Assert.AreEqual('', FConn.LastError, 'a new session must not inherit the old error');
  Assert.AreEqual(2, FSocket.OpenCount, 'the channel must have been reopened');
end;

procedure TNatsConnectionConcurrencyTests.Info_RequiringTls_TearsDownWithAClearError;
begin
  OpenConnection;

  FSocket.ServerSendLine(NatsConstants.Protocol.INFO +
    ' {"server_name":"nats-1","proto":1,"tls_required":true,"max_payload":1048576}');

  { Carrying on in plaintext just gets the connection dropped by the server
    with no explanation }
  Assert.IsTrue(WaitForLog('ERROR:'),
    'a TLS-only server must produce a clear error; log was: ' + FLog.Text);
  Assert.IsTrue(FConn.LastError.Contains('TLS'), 'LastError: ' + FConn.LastError);
  Assert.IsFalse(FSocket.ClientText.Contains(NatsConstants.Protocol.CONNECT),
    'and CONNECT must not be sent in the clear');
end;

procedure TNatsConnectionConcurrencyTests.Close_LeavesNoError;
begin
  OpenAndHandshake;

  FConn.Close;

  Assert.AreEqual('', FConn.LastError, 'a deliberate Close is not a failure');
end;

procedure TNatsConnectionConcurrencyTests.ConcurrentWrites_AreNotInterleavedOnTheSocket;
const
  PAYLOAD = 'payload';
  WRITERS = 4;
  ITERATIONS = 500;
var
  LConn: TNatsConnection;
begin
  OpenAndHandshake;
  LConn := FConn;

  // Half the workers publish (two socket writes under FLock), half subscribe
  // (one socket write with no lock at all). This is probabilistic: it needs a
  // SUB to land between a PUB control line and its payload.
  RunParallel(WRITERS,
    procedure (AIndex: Integer)
    var
      LLoop: Integer;
    begin
      for LLoop := 1 to ITERATIONS do
        if AIndex mod 2 = 0 then
          LConn.Publish('stress', PAYLOAD)
        else
          LConn.Subscribe('stress',
            procedure (const AMsg: TNatsArgsMSG)
            begin
            end);
    end);

  // [KNOWN BUG §10] raises when two writes have interleaved
  try
    ValidateClientStream(FSocket.ClientBytes, PAYLOAD);
  except
    on E: ENatsMock do
      Assert.Fail(E.Message);
  end;
end;

procedure TNatsConnectionConcurrencyTests.Close_ThenOpen_RestartsTheConnection;
begin
  OpenAndHandshake;
  Assert.IsTrue(FConn.Connected, 'the connection should be open');

  FConn.Close;
  Assert.IsFalse(FConn.Connected, 'the connection should be closed');

  FSocket.ClearClientData;
  OpenAndHandshake;

  Assert.IsTrue(FConn.Connected, 'the connection should be open again');
  Assert.AreEqual(2, FSocket.OpenCount, 'the channel must be reopened');
end;

procedure TNatsConnectionConcurrencyTests.Close_InvokesTheDisconnectHandler;
begin
  OpenAndHandshake;

  FConn.Close;

  Assert.IsTrue(FLog.Contains('DISCONNECT'), 'the disconnect handler must run, log was: ' + FLog.Text);
end;

procedure TNatsConnectionConcurrencyTests.Close_WhenNeverOpened_DoesNotRaise;
begin
  Assert.WillNotRaise(
    procedure
    begin
      FConn.Close;
    end);
end;

procedure TNatsConnectionConcurrencyTests.Dispatch_WhileUnsubscribing_DoesNotUseFreedMemory;
const
  ITERATIONS = 200;
var
  LConn: TNatsConnection;
  LSocket: TNatsMockSocket;
  LLoop: Integer;
begin
  OpenAndHandshake;
  LConn := FConn;
  LSocket := FSocket;

  for LLoop := 1 to ITERATIONS do
  begin
    var LSid := LConn.Subscribe('churn',
      procedure (const AMsg: TNatsArgsMSG)
      begin
        FLog.Add(AMsg.Payload);
      end);

    LSocket.ServerSend(Format('MSG churn %d 5'#13#10'hello'#13#10, [LSid]));
    // races the consumer thread, which may be inside LSub.Handler right now
    LConn.Unsubscribe(Cardinal(LSid));
  end;

  Assert.AreEqual(0, Length(LConn.GetSubscriptionList),
    'every subscription must have been removed after the churn');
end;

procedure TNatsConnectionConcurrencyTests.ServerError_DoesNotDeadlockTheConsumer;
begin
  OpenAndHandshake;

  FSocket.ServerSendLine(NatsConstants.Protocol.ERR + ' ''Authorization Violation''');

  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := not FConn.Connected;
    end,
    5000),
    'a fatal -ERR must close the connection');
  Assert.IsTrue(FLog.Contains('DISCONNECT'), 'the application must be told the connection died');
end;

initialization
  TDUnitX.RegisterTestFixture(TNatsGeneratorTests);
  TDUnitX.RegisterTestFixture(TNatsConnectionConcurrencyTests);

end.
