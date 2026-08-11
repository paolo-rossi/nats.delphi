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
    procedure OpenAndHandshake;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    // [KNOWN BUG §10] SendSubscribe writes outside FLock, so a SUB can be
    // spliced between a PUB control line and its payload
    [Test]
    procedure ConcurrentWrites_AreNotInterleavedOnTheSocket;

    // [KNOWN BUG §14] one unparsable line kills the reader thread silently
    [Test]
    procedure Reader_SurvivesAnUnparsableCommand;

    [Test]
    procedure Close_ThenOpen_RestartsTheConnection;
    [Test]
    procedure Close_InvokesTheDisconnectHandler;
    [Test]
    procedure Close_WhenNeverOpened_DoesNotRaise;

    // [KNOWN BUG §8] TObjectDictionary owns the values, so Unsubscribe frees a
    // TNatsSubscription the consumer thread may be dispatching through
    [Test]
    [Ignore('Reproduces the §8 use-after-free: crashes the process instead of failing. Enable once subscription access is serialized.')]
    procedure Dispatch_WhileUnsubscribing_DoesNotUseFreedMemory;

    // [KNOWN BUG §9] the consumer calls Close on itself, which WaitFor's itself
    [Test]
    [Ignore('Reproduces the §9 self-join deadlock: hangs the runner instead of failing. Enable once the -ERR path stops calling Close on the consumer thread.')]
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

procedure TNatsConnectionConcurrencyTests.OpenAndHandshake;
begin
  FConn.Open(
    procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
    begin
      FLog.Add('CONNECT');
    end,
    procedure
    begin
      FLog.Add('DISCONNECT');
    end);

  FSocket.ServerSendLine(NatsConstants.Protocol.INFO + ' ' + INFO_JSON);
  Assert.IsTrue(FSocket.WaitForClientText(NatsConstants.Protocol.CONNECT), 'handshake did not complete');
  FSocket.ClearClientData;
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

procedure TNatsConnectionConcurrencyTests.Reader_SurvivesAnUnparsableCommand;
var
  LSid: Integer;
begin
  OpenAndHandshake;
  LSid := FConn.Subscribe('foo',
    procedure (const AMsg: TNatsArgsMSG)
    begin
      FLog.Add('MSG:' + AMsg.Payload);
    end);

  // an unknown control line must not be fatal
  FSocket.ServerSendLine('NONSENSE not a nats command');
  FSocket.ServerSend(Format('MSG foo %d 5'#13#10'hello'#13#10, [LSid]));

  // [KNOWN BUG §14] FParser.Parse runs outside the reader's try/except, so the
  // reader thread dies and nothing is ever delivered again
  Assert.IsTrue(WaitForCondition(
    function: Boolean
    begin
      Result := FLog.Contains('MSG:hello');
    end),
    'the reader must keep working after an unparsable command, log was: ' + FLog.Text);
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
