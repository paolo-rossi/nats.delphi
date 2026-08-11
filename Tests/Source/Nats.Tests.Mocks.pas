{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Tests.Mocks;

{******************************************************************************}
{                                                                              }
{  Test doubles and helpers shared by the test fixtures.                       }
{                                                                              }
{  TNatsMockSocket is an in-memory INatsSocket. It registers itself as the      }
{  default socket class, so a TNatsConnection created inside this test project  }
{  talks to the mock instead of a real server. Tests drive the "server side"    }
{  with ServerSend* and inspect what the client wrote with ClientText/Bytes.    }
{                                                                              }
{  Do NOT add Nats.Socket.Indy to this project: the mock must stay the only     }
{  registered socket class.                                                    }
{                                                                              }
{******************************************************************************}

interface

uses
  System.SysUtils, System.Classes, System.SyncObjs, System.Generics.Collections,

  Nats.Consts,
  Nats.Socket,
  Nats.Exceptions;

type
  ENatsMock = class(ENatsException);

  /// <summary>
  ///   In-memory INatsSocket implementation. All state is guarded by FLock
  ///   because the connection's reader/consumer threads touch it concurrently
  ///   with the test thread.
  /// </summary>
  TNatsMockSocket = class(TNatsSocket)
  public const
    /// Mirrors Indy's ReadTimeout. Kept short so tests fail fast instead of hanging.
    DEFAULT_READ_TIMEOUT = 200;
    POLL_INTERVAL = 5;
  public
    /// <summary>
    ///   The most recently constructed mock. TNatsConnection builds its own
    ///   socket through TNatsSocketRegistry, so this is how a test gets hold
    ///   of it. Valid only while the owning connection is alive (the socket is
    ///   reference counted and dies with the connection).
    /// </summary>
    class var LastInstance: TNatsMockSocket;
  private
    FLock: TCriticalSection;
    FInbound: TBytes;
    FInboundPos: Integer;
    FOutbound: TBytes;
    FConnected: Boolean;
    FHost: string;
    FPort: Integer;
    FTimeout: Cardinal;
    FMaxLineLength: Cardinal;
    FOpenCount: Integer;
    /// Both assume FLock is already held
    function TryTakeLine(out ALine: string): Boolean;
    function TryTakeBytes(ACount: Integer; out AData: TBytes): Boolean;
  protected
    function GetConnected: Boolean; override;
    function GetHost: string; override;
    function GetPort: Integer; override;
    function GetTimeout: Cardinal; override;
    function GetMaxLineLength: Cardinal; override;
    procedure SetHost(const Value: string); override;
    procedure SetPort(const Value: Integer); override;
    procedure SetTimeout(const Value: Cardinal); override;
    procedure SetMaxLineLength(const Value: Cardinal); override;
  public
    constructor Create; override;
    destructor Destroy; override;

    procedure Open(); override;
    procedure Close(); override;
    procedure SendBytes(const AValue: TBytes); override;
    procedure SendString(const AValue: string); override;
    function ReceiveString: string; override;
    function ReceiveBytes: TBytes; override;
    function ReceiveExactBytes(ACount: Integer): TBytes; override;
  public
    { "Server side": queue bytes for the client to read }

    /// Appends AText verbatim (no CRLF added)
    procedure ServerSend(const AText: string);
    /// Appends AText + CRLF
    procedure ServerSendLine(const AText: string);
    procedure ServerSendBytes(const AData: TBytes);

    { "Wire capture": everything the client has written }

    function ClientBytes: TBytes;
    function ClientText: string;
    procedure ClearClientData;
    function WaitForClientText(const ASubText: string; ATimeoutMs: Cardinal = 3000): Boolean;

    property OpenCount: Integer read FOpenCount;
  end;

  /// <summary>
  ///   Thread-safe string collector. Message handlers run on the connection's
  ///   consumer thread, so tests must not touch plain fields from them.
  /// </summary>
  TNatsTestLog = class
  private
    FLock: TCriticalSection;
    FItems: TStringList;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Add(const AText: string);
    procedure AddFmt(const AText: string; const AArgs: array of const);
    function Count: Integer;
    function Item(AIndex: Integer): string;
    function Text: string;
    function Contains(const AText: string): Boolean;
    function ToArray: TArray<string>;
  end;

/// <summary>
///   Polls ACondition until it returns True or ATimeoutMs elapses. Needed
///   because the consumer thread dispatches asynchronously (and currently
///   polls its queue every 100 ms).
/// </summary>
function WaitForCondition(const ACondition: TFunc<Boolean>; ATimeoutMs: Cardinal = 3000): Boolean;

/// <summary>
///   Runs AProc on AThreadCount threads (AProc receives the worker index) and
///   blocks until all have finished. Any exception escaping a worker is
///   re-raised on the calling thread.
/// </summary>
procedure RunParallel(AThreadCount: Integer; const AProc: TProc<Integer>);

/// <summary>
///   Makes TNatsMockSocket the class TNatsConnection will instantiate.
///
///   TNatsConnection always builds TNatsSocketRegistry.Get('') — there is no
///   way to pick an adapter per connection — so a suite that mixes mock and
///   live sockets has to flip the registry's default between fixtures. The
///   registry only exposes Register<T>(name, default) and raises on a
///   duplicate name, hence the throwaway unique name.
/// </summary>
procedure UseMockSocket;

/// <summary>
///   Walks a captured client stream and checks it is a well-formed sequence of
///   client commands, verifying that every PUB payload equals AExpectedPayload.
///   Detects two writes being interleaved on the socket without needing to
///   corrupt memory to prove it.
/// </summary>
procedure ValidateClientStream(const AData: TBytes; const AExpectedPayload: string);

implementation

const
  CR = 13;
  LF = 10;

var
  GDefaultSwitchCount: Integer = 0;

{ helpers }

procedure UseMockSocket;
begin
  Inc(GDefaultSwitchCount);
  TNatsSocketRegistry.Register<TNatsMockSocket>(
    Format('Mock#%d', [GDefaultSwitchCount]), True);
end;

function WaitForCondition(const ACondition: TFunc<Boolean>; ATimeoutMs: Cardinal): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := TThread.GetTickCount64 + ATimeoutMs;
  repeat
    if ACondition() then
      Exit(True);
    TThread.Sleep(10);
  until TThread.GetTickCount64 > LDeadline;

  Result := ACondition();
end;

function CreateWorker(const AProc: TProc<Integer>; AIndex: Integer; AErrors: TNatsTestLog): TProc;
begin
  // AIndex is a value parameter, so each call captures its own copy
  Result :=
    procedure
    begin
      try
        AProc(AIndex);
      except
        on E: Exception do
          AErrors.AddFmt('worker %d: %s: %s', [AIndex, E.ClassName, E.Message]);
      end;
    end;
end;

procedure RunParallel(AThreadCount: Integer; const AProc: TProc<Integer>);
var
  LThreads: TArray<TThread>;
  LErrors: TNatsTestLog;
  LIndex: Integer;
begin
  LErrors := TNatsTestLog.Create;
  try
    SetLength(LThreads, AThreadCount);
    for LIndex := 0 to AThreadCount - 1 do
    begin
      LThreads[LIndex] := TThread.CreateAnonymousThread(CreateWorker(AProc, LIndex, LErrors));
      LThreads[LIndex].FreeOnTerminate := False;
    end;

    try
      for LIndex := 0 to AThreadCount - 1 do
        LThreads[LIndex].Start;

      for LIndex := 0 to AThreadCount - 1 do
        LThreads[LIndex].WaitFor;
    finally
      for LIndex := 0 to AThreadCount - 1 do
        LThreads[LIndex].Free;
    end;

    if LErrors.Count > 0 then
      raise ENatsMock.Create('Parallel workers failed:'#13#10 + LErrors.Text);
  finally
    LErrors.Free;
  end;
end;

procedure ValidateClientStream(const AData: TBytes; const AExpectedPayload: string);
var
  LPos: Integer;
  LLine: string;
  LParts: TArray<string>;
  LDeclared: Integer;
  LPayload: string;

  function ReadLine(out ALine: string): Boolean;
  var
    LIdx: Integer;
  begin
    Result := False;
    ALine := '';
    LIdx := LPos;
    while LIdx < Length(AData) - 1 do
    begin
      if (AData[LIdx] = CR) and (AData[LIdx + 1] = LF) then
      begin
        ALine := TEncoding.UTF8.GetString(Copy(AData, LPos, LIdx - LPos));
        LPos := LIdx + 2;
        Exit(True);
      end;
      Inc(LIdx);
    end;
  end;

begin
  LPos := 0;
  while LPos < Length(AData) do
  begin
    if not ReadLine(LLine) then
      raise ENatsMock.CreateFmt(
        'Truncated command line at offset %d (stream is not a valid command sequence)', [LPos]);

    LParts := LLine.Split([NatsConstants.SPC]);
    if Length(LParts) = 0 then
      raise ENatsMock.Create('Empty command line in captured stream');

    if LParts[0] = NatsConstants.Protocol.PUB then
    begin
      LDeclared := StrToIntDef(LParts[Length(LParts) - 1], -1);
      if LDeclared < 0 then
        raise ENatsMock.Create('Malformed PUB control line: ' + LLine);
      if LPos + LDeclared + NatsConstants.CR_LF_LEN > Length(AData) then
        raise ENatsMock.Create('Truncated PUB payload after: ' + LLine);

      LPayload := TEncoding.UTF8.GetString(Copy(AData, LPos, LDeclared));
      Inc(LPos, LDeclared + NatsConstants.CR_LF_LEN);

      if LPayload <> AExpectedPayload then
        raise ENatsMock.CreateFmt(
          'Interleaved write detected: after "%s" the payload should be "%s" but was "%s"',
          [LLine, AExpectedPayload, LPayload]);
    end
    else if (LParts[0] = NatsConstants.Protocol.SUB) or
            (LParts[0] = NatsConstants.Protocol.UNSUB) or
            (LParts[0] = NatsConstants.Protocol.PING) or
            (LParts[0] = NatsConstants.Protocol.PONG) or
            (LParts[0] = NatsConstants.Protocol.CONNECT) then
    begin
      // control line only, nothing else to consume
    end
    else
      raise ENatsMock.Create('Unexpected command in captured stream: ' + LLine);
  end;
end;

{ TNatsMockSocket }

constructor TNatsMockSocket.Create;
begin
  // no "inherited Create": TNatsSocket.Create is abstract, calling it raises
  // an abstract error (TNatsSocketIndy does the same)
  FLock := TCriticalSection.Create;
  FHost := '127.0.0.1';
  FPort := NatsConstants.DEFAULT_PORT;
  FTimeout := DEFAULT_READ_TIMEOUT;
  FMaxLineLength := 16 * 1024;
  LastInstance := Self;
end;

destructor TNatsMockSocket.Destroy;
begin
  if LastInstance = Self then
    LastInstance := nil;
  FLock.Free;
  inherited;
end;

procedure TNatsMockSocket.Open;
begin
  FLock.Enter;
  try
    FConnected := True;
    Inc(FOpenCount);
  finally
    FLock.Leave;
  end;
end;

procedure TNatsMockSocket.Close;
begin
  FLock.Enter;
  try
    FConnected := False;
  finally
    FLock.Leave;
  end;
end;

function TNatsMockSocket.TryTakeLine(out ALine: string): Boolean;
var
  LIdx: Integer;
begin
  Result := False;
  ALine := '';
  LIdx := FInboundPos;
  while LIdx < Length(FInbound) - 1 do
  begin
    if (FInbound[LIdx] = CR) and (FInbound[LIdx + 1] = LF) then
    begin
      ALine := TEncoding.UTF8.GetString(Copy(FInbound, FInboundPos, LIdx - FInboundPos));
      FInboundPos := LIdx + 2;
      if FInboundPos >= Length(FInbound) then
      begin
        FInbound := nil;
        FInboundPos := 0;
      end;
      Exit(True);
    end;
    Inc(LIdx);
  end;
end;

function TNatsMockSocket.TryTakeBytes(ACount: Integer; out AData: TBytes): Boolean;
begin
  Result := Length(FInbound) - FInboundPos >= ACount;
  if not Result then
    Exit;

  AData := Copy(FInbound, FInboundPos, ACount);
  Inc(FInboundPos, ACount);
  if FInboundPos >= Length(FInbound) then
  begin
    FInbound := nil;
    FInboundPos := 0;
  end;
end;

function TNatsMockSocket.ReceiveString: string;
var
  LDeadline: UInt64;
begin
  LDeadline := TThread.GetTickCount64 + FTimeout;
  repeat
    FLock.Enter;
    try
      if not FConnected then
        raise ENatsMock.Create('Mock socket: read on a closed connection');
      if TryTakeLine(Result) then
        Exit;
    finally
      FLock.Leave;
    end;
    TThread.Sleep(POLL_INTERVAL);
  until TThread.GetTickCount64 > LDeadline;

  raise ENatsMock.Create('Mock socket: read timeout waiting for a line');
end;

function TNatsMockSocket.ReceiveBytes: TBytes;
begin
  Result := TEncoding.UTF8.GetBytes(ReceiveString);
end;

function TNatsMockSocket.ReceiveExactBytes(ACount: Integer): TBytes;
var
  LDeadline: UInt64;
begin
  if ACount <= 0 then
    Exit(nil);

  LDeadline := TThread.GetTickCount64 + FTimeout;
  repeat
    FLock.Enter;
    try
      if not FConnected then
        raise ENatsMock.Create('Mock socket: read on a closed connection');
      if TryTakeBytes(ACount, Result) then
        Exit;
    finally
      FLock.Leave;
    end;
    TThread.Sleep(POLL_INTERVAL);
  until TThread.GetTickCount64 > LDeadline;

  raise ENatsMock.CreateFmt('Mock socket: read timeout waiting for %d bytes', [ACount]);
end;

procedure TNatsMockSocket.SendBytes(const AValue: TBytes);
begin
  FLock.Enter;
  try
    FOutbound := FOutbound + AValue + TEncoding.UTF8.GetBytes(NatsConstants.CR_LF);
  finally
    FLock.Leave;
  end;
end;

procedure TNatsMockSocket.SendString(const AValue: string);
begin
  SendBytes(TEncoding.UTF8.GetBytes(AValue));
end;

procedure TNatsMockSocket.ServerSend(const AText: string);
begin
  ServerSendBytes(TEncoding.UTF8.GetBytes(AText));
end;

procedure TNatsMockSocket.ServerSendLine(const AText: string);
begin
  ServerSend(AText + NatsConstants.CR_LF);
end;

procedure TNatsMockSocket.ServerSendBytes(const AData: TBytes);
begin
  FLock.Enter;
  try
    FInbound := FInbound + AData;
  finally
    FLock.Leave;
  end;
end;

function TNatsMockSocket.ClientBytes: TBytes;
begin
  FLock.Enter;
  try
    Result := Copy(FOutbound, 0, Length(FOutbound));
  finally
    FLock.Leave;
  end;
end;

function TNatsMockSocket.ClientText: string;
begin
  Result := TEncoding.UTF8.GetString(ClientBytes);
end;

procedure TNatsMockSocket.ClearClientData;
begin
  FLock.Enter;
  try
    FOutbound := nil;
  finally
    FLock.Leave;
  end;
end;

function TNatsMockSocket.WaitForClientText(const ASubText: string; ATimeoutMs: Cardinal): Boolean;
begin
  Result := WaitForCondition(
    function: Boolean
    begin
      Result := ClientText.Contains(ASubText);
    end,
    ATimeoutMs);
end;

function TNatsMockSocket.GetConnected: Boolean;
begin
  FLock.Enter;
  try
    Result := FConnected;
  finally
    FLock.Leave;
  end;
end;

function TNatsMockSocket.GetHost: string;
begin
  Result := FHost;
end;

function TNatsMockSocket.GetPort: Integer;
begin
  Result := FPort;
end;

function TNatsMockSocket.GetTimeout: Cardinal;
begin
  Result := FTimeout;
end;

function TNatsMockSocket.GetMaxLineLength: Cardinal;
begin
  Result := FMaxLineLength;
end;

procedure TNatsMockSocket.SetHost(const Value: string);
begin
  FHost := Value;
end;

procedure TNatsMockSocket.SetPort(const Value: Integer);
begin
  FPort := Value;
end;

procedure TNatsMockSocket.SetTimeout(const Value: Cardinal);
begin
  FTimeout := Value;
end;

procedure TNatsMockSocket.SetMaxLineLength(const Value: Cardinal);
begin
  FMaxLineLength := Value;
end;

{ TNatsTestLog }

constructor TNatsTestLog.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FItems := TStringList.Create;
end;

destructor TNatsTestLog.Destroy;
begin
  FItems.Free;
  FLock.Free;
  inherited;
end;

procedure TNatsTestLog.Add(const AText: string);
begin
  FLock.Enter;
  try
    FItems.Add(AText);
  finally
    FLock.Leave;
  end;
end;

procedure TNatsTestLog.AddFmt(const AText: string; const AArgs: array of const);
begin
  Add(Format(AText, AArgs));
end;

function TNatsTestLog.Count: Integer;
begin
  FLock.Enter;
  try
    Result := FItems.Count;
  finally
    FLock.Leave;
  end;
end;

function TNatsTestLog.Item(AIndex: Integer): string;
begin
  FLock.Enter;
  try
    if (AIndex < 0) or (AIndex >= FItems.Count) then
      Exit('');
    Result := FItems[AIndex];
  finally
    FLock.Leave;
  end;
end;

function TNatsTestLog.Text: string;
begin
  FLock.Enter;
  try
    Result := FItems.Text;
  finally
    FLock.Leave;
  end;
end;

function TNatsTestLog.Contains(const AText: string): Boolean;
begin
  FLock.Enter;
  try
    Result := FItems.IndexOf(AText) >= 0;
  finally
    FLock.Leave;
  end;
end;

function TNatsTestLog.ToArray: TArray<string>;
begin
  FLock.Enter;
  try
    Result := FItems.ToStringArray;
  finally
    FLock.Leave;
  end;
end;

initialization
  TNatsSocketRegistry.Register<TNatsMockSocket>('Mock', True);

end.
