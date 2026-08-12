{******************************************************************************}
{                                                                              }
{  NATS.Delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{******************************************************************************}
{                                                                              }
{  Licensed under the Apache License, Version 2.0 (the "License");             }
{  you may not use this file except in compliance with the License.            }
{  You may obtain a copy of the License at                                     }
{                                                                              }
{      http://www.apache.org/licenses/LICENSE-2.0                              }
{                                                                              }
{  Unless required by applicable law or agreed to in writing, software         }
{  distributed under the License is distributed on an "AS IS" BASIS,           }
{  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.    }
{  See the License for the specific language governing permissions and         }
{  limitations under the License.                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Connection;

interface

{$SCOPEDENUMS ON}

uses
  System.Classes, System.SysUtils, System.Rtti, System.SyncObjs,
  System.Generics.Defaults, System.Generics.Collections,

  Nats.Classes,
  Nats.Consts,
  Nats.Entities,
  Nats.Parser,
  Nats.Socket;

type
  (*
  INatsConnection = interface
  ['{8630DB26-6324-4E33-8342-85BF42A34FC2}']
    procedure Publish(const ASubject, AMessage: string);
    procedure Subscribe(const ASubject: string);
  end;
  *)

  TNatsConnection = class;

  /// <summary>
  ///   The handoff between the consumer thread, which receives a request's
  ///   reply, and the caller blocked inside RequestSync
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     Reference counted on purpose. The subscription handler closure holds
  ///     one reference and the waiting caller holds another, so a reply that
  ///     arrives after the caller has already given up still finds a live
  ///     object to write into. A plain object owned by the caller's stack frame
  ///     would be freed underneath the consumer thread on every single timeout.
  ///   </para>
  ///   <para>
  ///     The reply is COPIED into the waiter, never handed over by reference:
  ///     TNatsArgsMSG is a record and the consumer thread's copy goes out of
  ///     scope as soon as the handler returns.
  ///   </para>
  /// </remarks>
  INatsRequestWaiter = interface
  ['{4F0D1C7A-9E3B-4A16-8D2F-1B7C6E5A9042}']
    /// <summary>
    ///   Delivers the reply. Runs on the consumer thread; the first reply wins
    ///   and any later one is discarded
    /// </summary>
    procedure Signal(const AMsg: TNatsArgsMSG);
    /// <summary>
    ///   Releases the caller empty handed because the connection is going away.
    ///   A reply that already landed is kept, so a race with Signal cannot lose
    ///   a message that was genuinely received
    /// </summary>
    procedure Cancel;
    /// <summary>
    ///   Blocks for up to ATimeoutMs. False means the time ran out with no
    ///   reply; a connection torn down while waiting raises instead, because
    ///   that is not the same thing as a slow responder
    /// </summary>
    function WaitFor(ATimeoutMs: Cardinal; out AMsg: TNatsArgsMSG): Boolean;
  end;

  /// <summary>
  ///   Simple Id generator for subscription and inbox
  /// </summary>
  TNatsGenerator = class
  private
    FSubId: Integer;
  public
    constructor Create(); // Initialize counters
    { Integer throughout: a subscription id is a dictionary key and a field on
      TNatsSubscription, both of which are Integer }
    function GetSubNextId: Integer;
    function GetNewInbox: string;
  end;

  /// <summary>
  ///   Worker thread for reading incoming messages from the socket channel
  /// </summary>
  TNatsReader = class(TNatsThread)
  private
    FConnection: TNatsConnection;
    FChannel: INatsSocket;
    FParser: TNatsParser;
    FQueue: TNatsCommandQueue;
    FError: string;
    procedure DoExecute;
    procedure ReadMessageBody(var ACommand: TNatsCommand);
  protected
    procedure Execute; override;
  public
    constructor Create(AConnection: TNatsConnection);
    destructor Destroy; override;
    property Error: string read FError write FError;
  end;

  /// <summary>
  ///   Worker thread for processing incoming and outgoing messages
  /// </summary>
  TNatsConsumer = class(TNatsThread)
  private
    FConnection: TNatsConnection;
    FQueue: TNatsCommandQueue;
    FError: string;
    procedure DoExecute;
  protected
    procedure Execute; override;
  public
    constructor Create(const AConnection: TNatsConnection);

    property Error: string read FError write FError;
  end;

  /// <summary>
  ///   Structure holding subscription metadata.
  ///
  ///   Owned by TNatsConnection.FSubscriptions, which frees it on removal, so
  ///   an instance may only be reached while holding the connection's
  ///   subscription lock - never store a reference to one and never pass one
  ///   outside that lock. Callers get TNatsSubscriptionInfo snapshots instead.
  /// </summary>
  TNatsSubscription = class
    Id: Integer;
    Subject: string;
    Handler: TNatsMsgHandler;
    Queue: string;
    Received: Integer;
    Expected: Integer;
    Remaining: Integer;

    constructor Create(AId: Integer; const ASubject, AQueue: string; AHandler: TNatsMsgHandler); overload;
  end;

  /// <summary>
  ///   Detached copy of a subscription's state, safe to hold and read at any
  ///   time because it shares nothing with the live subscription
  /// </summary>
  TNatsSubscriptionInfo = record
    Id: Integer;
    Subject: string;
    Queue: string;
    Received: Integer;
    Expected: Integer;
    Remaining: Integer;
  end;

  TNatsSubscriptions = TObjectDictionary<Integer, TNatsSubscription>;

  /// <summary>
  ///   TNatsConnection represents a bidirectional channel to the NATS server.
  ///   Message handler may be attached to each operation which is invoked when
  ///   the operation is processed by the server.
  /// </summary>
  /// <remarks>
  ///   <para>Threading contract - two locks, with strict rules:</para>
  ///   <para>
  ///     FWriteLock serializes every write to the channel, and the channel's
  ///     own open/close. It is held for exactly one complete command, so a
  ///     multi-part command (control line + payload) can never be split by
  ///     another thread's write.
  ///   </para>
  ///   <para>
  ///     FSubsLock guards FSubscriptions and every TNatsSubscription it owns.
  ///   </para>
  ///   <para>
  ///     Never hold both at once; never hold either while invoking a user
  ///     handler; never hold either while joining a worker thread. Handlers are
  ///     user code and routinely call back into this class, so any of those
  ///     would deadlock.
  ///   </para>
  ///   <para>
  ///     EVERY handler - message, connect, disconnect, error - must be written
  ///     to be thread safe. None of them is guaranteed to run on the thread
  ///     that opened the connection: message and connect handlers always run on
  ///     the consumer thread, and the disconnect and error handlers run on
  ///     whichever thread discovered the failure, which is a worker thread
  ///     whenever the server or the socket caused it and the caller's thread
  ///     when it was a deliberate Close. Marshal to the UI yourself, as the
  ///     demo does with TThread.Queue.
  ///   </para>
  /// </remarks>
  TNatsConnection = class
  private const
    { The socket being up is not the same as the connection being usable: until
      CONNECT has been written the server knows nothing about this client }
    STATE_CLOSED = 0;
    STATE_CONNECTING = 1;   // socket open, waiting for the server's INFO
    STATE_READY = 2;        // CONNECT written, safe to publish and subscribe
  private
    FChannel: INatsSocket;
    FGenerator: TNatsGenerator;
    FSubscriptions: TNatsSubscriptions;
    FName: string;
    FReader: TNatsReader;
    FConsumer: TNatsConsumer;
    FReadQueue: TNatsCommandQueue;
    FConnectHandler: TNatsConnectHandler;
    FDisconnectHandler: TNatsDisconnectHandler;
    FErrorHandler: TNatsErrorHandler;
    FWriteLock: TCriticalSection;
    FSubsLock: TCriticalSection;
    { Guards FPendingRequests only, and is always a LEAF: no other lock may be
      taken while it is held, and it is never taken while holding another }
    FRequestsLock: TCriticalSection;
    FPendingRequests: TList<INatsRequestWaiter>;
    FState: Integer;
    FPingOutstanding: Integer;
    FLastError: string;

    { every write to the channel goes through one of these }
    procedure SendCommand(const ALine: string); overload;
    procedure SendCommand(const ALine: string; const APayload: TBytes); overload;
    procedure SendCommand(const ALine, AHeaderBlock: string; const APayload: TBytes); overload;

    procedure SendPing;
    procedure SendPong;
    procedure SendConnect;
    procedure SendSubscribe(AId: Integer; const ASubject, AQueue: string);

    /// <summary>
    ///   Raises if ASubject could not be sent as-is. Whitespace or a line break
    ///   would split the control line and desynchronize the whole stream, which
    ///   is far harder to diagnose than an exception at the call site
    /// </summary>
    procedure CheckSubject(const ASubject: string);
    function GetConnected: Boolean;
    function GetReady: Boolean;
    function GetLastError: string;
    function TakeMessageHandler(AId: Integer; out AHandler: TNatsMsgHandler): Boolean;
    procedure SignalStop;
    procedure JoinThreads;
    procedure CloseChannel;
    procedure ClearSubscriptions;
    procedure AddPendingRequest(const AWaiter: INatsRequestWaiter);
    procedure RemovePendingRequest(const AWaiter: INatsRequestWaiter);
    /// <summary>
    ///   Releases everyone blocked in RequestSync. Without it a connection that
    ///   drops mid-request leaves each caller waiting out its full timeout for
    ///   a reply that provably cannot arrive
    /// </summary>
    procedure CancelPendingRequests;
    procedure TearDown; overload;
    procedure TearDown(const AError: string); overload;
    procedure HandleInfo(const AInfo: TNatsServerInfo);
    /// <summary>
    ///   Called by the reader when a read times out: an idle connection is not
    ///   a dead one, so probe it rather than tearing it down
    /// </summary>
    procedure PingOnIdle;
  public
    constructor Create;
    destructor Destroy; override;
  public
    /// <summary>
    ///   AConnectTimeout bounds establishing the connection; AReadTimeout (0 =
    ///   keep the socket's default) bounds a single read. They are not the same
    ///   thing and must not be given the same value
    /// </summary>
    function SetChannel(const AHost: string; APort, AConnectTimeout: Integer;
      AReadTimeout: Integer = 0): TNatsConnection;
    procedure Open(AConnectHandler: TNatsConnectHandler; ADisconnectHandler: TNatsDisconnectHandler = nil); overload;
    procedure Close();

    procedure Ping();
    procedure Connect(AOptions: TNatsConnectOptions); overload;

    procedure Publish(const ASubject, AMessage: string; const AReplyTo: string = ''); overload;
    procedure Publish(const ASubject, AMessage: string; const AReplyTo: string; AHeaders: TNatsHeaders); overload;
    procedure PublishBytes(const ASubject: string; const AData: TBytes; const AReplyTo: string = ''); overload;
    procedure PublishBytes(const ASubject: string; const AData: TBytes; const AReplyTo: string; AHeaders: TNatsHeaders); overload;

    function Request(const ASubject: string; AHandler: TNatsMsgHandler): Integer; overload;
    function Request(const ASubject, AMessage: string; AHandler: TNatsMsgHandler): Integer; overload;

    /// <summary>
    ///   Publishes to ASubject and BLOCKS until one reply arrives or ATimeoutMs
    ///   elapses. False means it timed out; the connection being torn down while
    ///   waiting raises, because that is a different outcome from silence
    /// </summary>
    /// <remarks>
    ///   <para>
    ///     Unlike Request, this cleans up on EVERY exit path. The inbox
    ///     subscription is removed whether the reply arrived, the wait timed
    ///     out, or the publish itself raised - a timeout used to leave the inbox
    ///     subscribed on both sides and leak an entry from FSubscriptions.
    ///   </para>
    ///   <para>
    ///     NEVER call this from a message, connect or disconnect handler. Those
    ///     run on the consumer thread, which is the thread that has to deliver
    ///     the reply, so it would block until the timeout and deadlock itself.
    ///   </para>
    /// </remarks>
    function RequestSync(const ASubject, AMessage: string; out AReply: TNatsArgsMSG;
      ATimeoutMs: Cardinal = NatsConstants.DEFAULT_REQUEST_TIMEOUT): Boolean; overload;
    function RequestSync(const ASubject: string; const AData: TBytes; out AReply: TNatsArgsMSG;
      ATimeoutMs: Cardinal = NatsConstants.DEFAULT_REQUEST_TIMEOUT): Boolean; overload;
    /// <summary>
    ///   The form JetStream needs: a binary payload and headers in the same
    ///   call, because a JetStream publish carries Nats-Msg-Id / Nats-Expected-*
    ///   and expects a PubAck back
    /// </summary>
    function RequestSync(const ASubject: string; const AData: TBytes; AHeaders: TNatsHeaders;
      out AReply: TNatsArgsMSG;
      ATimeoutMs: Cardinal = NatsConstants.DEFAULT_REQUEST_TIMEOUT): Boolean; overload;

    function Subscribe(const ASubject: string; AHandler: TNatsMsgHandler): Integer; overload;
    function Subscribe(const ASubject, AQueue: string; AHandler: TNatsMsgHandler): Integer; overload;

    procedure Unsubscribe(AId: Integer; AMaxMsg: Integer = 0); overload;
    procedure Unsubscribe(const ASubject: string; AMaxMsg: Integer = 0); overload;

    function GetSubscriptionList: TArray<TNatsSubscriptionInfo>;

    function GetNewInbox():string;

    /// <summary>
    ///   Blocks until the handshake has completed, i.e. until CONNECT has
    ///   actually been written. Open only starts the handshake, so publishing
    ///   or subscribing before this returns True races it
    /// </summary>
    function WaitForReady(ATimeoutMs: Cardinal = 5000): Boolean;
  public
    ConnectOptions: TNatsConnectOptions;
    property Name: string read FName write FName;
    /// <summary>
    ///   True once the handshake has completed. A socket that is up but has not
    ///   exchanged INFO/CONNECT yet does not count: the server does not know
    ///   this client's options, and nothing may be published over it
    /// </summary>
    property Connected: Boolean read GetConnected;
    property Ready: Boolean read GetReady;
    /// <summary>
    ///   Why the connection last failed; empty after a clean Close
    /// </summary>
    property LastError: string read GetLastError;
    /// <summary>
    ///   Fires on a protocol error, a dead socket or an -ERR from the server -
    ///   the failures an application would otherwise never see. Runs on a
    ///   worker thread
    /// </summary>
    property OnError: TNatsErrorHandler read FErrorHandler write FErrorHandler;
    property Reader: TNatsReader read FReader;
    property Consumer: TNatsConsumer read FConsumer;
    //property ConnectOptions: TNatsConnectOptions read FConnectOptions write FConnectOptions;
  end;

  TNatsNetwork = class(TObjectDictionary<string, TNatsConnection>)
  public
    function NewConnection(const AName: string): TNatsConnection;
  end;

implementation

uses
  Nats.Nuid,
  Nats.Exceptions;

const
  /// How long the consumer waits on the queue before re-checking Terminated
  QUEUE_WAIT_MS = 250;

type
  /// <summary>
  ///   The only implementation of INatsRequestWaiter - private to this unit,
  ///   because nothing outside RequestSync has any business creating one
  /// </summary>
  TNatsRequestWaiter = class(TInterfacedObject, INatsRequestWaiter)
  private
    FEvent: TLightweightEvent;
    FLock: TCriticalSection;
    FMsg: TNatsArgsMSG;
    FHasMsg: Boolean;
    FCancelled: Boolean;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Signal(const AMsg: TNatsArgsMSG);
    procedure Cancel;
    function WaitFor(ATimeoutMs: Cardinal; out AMsg: TNatsArgsMSG): Boolean;
  end;

{ TNatsRequestWaiter }

constructor TNatsRequestWaiter.Create;
begin
  inherited Create;
  FEvent := TLightweightEvent.Create;
  FLock := TCriticalSection.Create;
end;

destructor TNatsRequestWaiter.Destroy;
begin
  FLock.Free;
  FEvent.Free;
  inherited;
end;

procedure TNatsRequestWaiter.Signal(const AMsg: TNatsArgsMSG);
begin
  FLock.Enter;
  try
    { First reply wins. A second one is possible in principle - the server was
      told UNSUB <sid> 1, but that only bounds what it delivers AFTER it reads
      the UNSUB - and overwriting a message the caller may already be reading
      would be a data race for no benefit }
    if FHasMsg or FCancelled then
      Exit;

    FMsg := AMsg;   // a record copy: the handler's own copy dies on return
    FHasMsg := True;
  finally
    FLock.Leave;
  end;

  FEvent.SetEvent;
end;

procedure TNatsRequestWaiter.Cancel;
begin
  FLock.Enter;
  try
    { A reply that already arrived is still a valid answer, even though the
      connection is now going away - do not turn it into a failure }
    if FHasMsg then
      Exit;

    FCancelled := True;
  finally
    FLock.Leave;
  end;

  FEvent.SetEvent;
end;

function TNatsRequestWaiter.WaitFor(ATimeoutMs: Cardinal; out AMsg: TNatsArgsMSG): Boolean;
begin
  { The result of the wait itself is deliberately ignored: a reply that lands in
    the same instant the timeout expires is still a reply, so what counts is
    what is in the waiter afterwards, not which of the two got there first }
  FEvent.WaitFor(ATimeoutMs);

  FLock.Enter;
  try
    if FCancelled then
      raise ENatsException.Create(
        'The connection was closed while waiting for a reply');

    Result := FHasMsg;
    if Result then
      AMsg := FMsg;
  finally
    FLock.Leave;
  end;
end;

{ TNatsConnection }

constructor TNatsConnection.Create;
begin
  inherited Create;

  FWriteLock := TCriticalSection.Create;
  FSubsLock := TCriticalSection.Create;
  FRequestsLock := TCriticalSection.Create;
  FPendingRequests := TList<INatsRequestWaiter>.Create;
  FState := STATE_CLOSED;
  FReadQueue := TNatsCommandQueue.Create;
  FGenerator := TNatsGenerator.Create;
  FSubscriptions := TNatsSubscriptions.Create([doOwnsValues]);

  ConnectOptions.Lang := 'Delphi';
  ConnectOptions.Version := NatsConstants.CLIENT_VERSION;
  ConnectOptions.Protocol := 1;
  ConnectOptions.Echo := True;
  ConnectOptions.Headers := True;

  { TODO -opaolo -c : Remove the default behavior 31/05/2022 18:17:27 }
  FChannel := TNatsSocketRegistry.Get(String.Empty);
end;

destructor TNatsConnection.Destroy;
begin
  Close();

  FSubscriptions.Free;
  FGenerator.Free;
  FReadQueue.Free;
  { Close has already cancelled and released every waiter }
  FPendingRequests.Free;
  FRequestsLock.Free;
  FSubsLock.Free;
  FWriteLock.Free;
  inherited;
end;

procedure TNatsConnection.Connect(AOptions: TNatsConnectOptions);
begin
  { TODO -opaolo -c : 07/06/2022 21:23:54 }
end;

procedure TNatsConnection.Open(AConnectHandler: TNatsConnectHandler;
  ADisconnectHandler: TNatsDisconnectHandler = nil);
begin
  if FState <> STATE_CLOSED then
    Exit; // Already open or opening

  { Reap whatever is left of a previous session before taking the lock: a
    reader that died on its own is still an unfreed TThread, and simply
    overwriting FReader/FConsumer below would leak it }
  SignalStop;
  JoinThreads;

  FWriteLock.Enter;
  try
    FConnectHandler := AConnectHandler;
    FDisconnectHandler := ADisconnectHandler;

    FLastError := '';
    FPingOutstanding := 0;
    FReadQueue.Clear;

    FChannel.Open;
    { CONNECTING, not READY: the socket is up but the server has not been told
      who we are yet, so nothing may be published over it }
    FState := STATE_CONNECTING;

    FReader := TNatsReader.Create(Self);
    FConsumer := TNatsConsumer.Create(Self);

    FReader.Start;
    FConsumer.Start;
  finally
    FWriteLock.Leave;
  end;
end;

procedure TNatsConnection.CheckSubject(const ASubject: string);
var
  LChar: Char;
begin
  if ASubject.IsEmpty then
    raise ENatsException.Create('The subject cannot be empty');

  for LChar in ASubject do
    if (LChar = ' ') or (LChar = #9) or (LChar = #13) or (LChar = #10) then
      raise ENatsException.CreateFmt(
        'The subject [%s] cannot contain whitespace or a line break', [ASubject]);
end;

function TNatsConnection.GetConnected: Boolean;
begin
  Result := (FState = STATE_READY) and Assigned(FChannel) and FChannel.Connected;
end;

function TNatsConnection.GetReady: Boolean;
begin
  Result := GetConnected;
end;

function TNatsConnection.GetLastError: string;
begin
  FWriteLock.Enter;
  try
    Result := FLastError;
  finally
    FWriteLock.Leave;
  end;
end;

function TNatsConnection.WaitForReady(ATimeoutMs: Cardinal): Boolean;
var
  LDeadline: UInt64;
begin
  LDeadline := TThread.GetTickCount64 + ATimeoutMs;
  repeat
    if Connected then
      Exit(True);
    if FState = STATE_CLOSED then
      Exit(False); // the handshake failed outright
    TThread.Sleep(5);
  until TThread.GetTickCount64 > LDeadline;

  Result := Connected;
end;

function TNatsConnection.GetNewInbox: string;
begin
 Result := FGenerator.GetNewInbox;
end;

function TNatsConnection.GetSubscriptionList: TArray<TNatsSubscriptionInfo>;
var
  LPair: TPair<Integer, TNatsSubscription>;
  LIndex: Integer;
begin
  { Snapshots, not the live objects: the dictionary owns them and frees them on
    removal, so a caller holding one could be left with a dangling pointer }
  FSubsLock.Enter;
  try
    SetLength(Result, FSubscriptions.Count);
    LIndex := 0;
    for LPair in FSubscriptions do
    begin
      Result[LIndex].Id := LPair.Value.Id;
      Result[LIndex].Subject := LPair.Value.Subject;
      Result[LIndex].Queue := LPair.Value.Queue;
      Result[LIndex].Received := LPair.Value.Received;
      Result[LIndex].Expected := LPair.Value.Expected;
      Result[LIndex].Remaining := LPair.Value.Remaining;
      Inc(LIndex);
    end;
  finally
    FSubsLock.Leave;
  end;
end;

procedure TNatsConnection.SignalStop;
begin
  { Only signals - safe from any thread, including the workers themselves }
  if Assigned(FReader) then
    FReader.Stop;
  if Assigned(FConsumer) then
    FConsumer.Stop;

  FReadQueue.Wake; // release the consumer if it is waiting on the queue
end;

procedure TNatsConnection.CloseChannel;
begin
  FWriteLock.Enter;
  try
    if Assigned(FChannel) and FChannel.Connected then
      FChannel.Close;
  finally
    FWriteLock.Leave;
  end;
end;

procedure TNatsConnection.ClearSubscriptions;
begin
  FSubsLock.Enter;
  try
    FSubscriptions.Clear;
  finally
    FSubsLock.Leave;
  end;
end;

procedure TNatsConnection.AddPendingRequest(const AWaiter: INatsRequestWaiter);
begin
  FRequestsLock.Enter;
  try
    FPendingRequests.Add(AWaiter);
  finally
    FRequestsLock.Leave;
  end;
end;

procedure TNatsConnection.RemovePendingRequest(const AWaiter: INatsRequestWaiter);
begin
  FRequestsLock.Enter;
  try
    { Remove of something already gone is a no-op, which is what makes this safe
      to call unconditionally after CancelPendingRequests has emptied the list }
    FPendingRequests.Remove(AWaiter);
  finally
    FRequestsLock.Leave;
  end;
end;

procedure TNatsConnection.CancelPendingRequests;
var
  LWaiters: TArray<INatsRequestWaiter>;
  LWaiter: INatsRequestWaiter;
begin
  { Snapshot under the lock, cancel outside it: Cancel signals an event, and no
    lock in this class is ever held across a wait or a signal to another thread.
    The array holds interface references, so every waiter stays alive for the
    duration even if its caller returns and releases its own reference. }
  FRequestsLock.Enter;
  try
    LWaiters := FPendingRequests.ToArray;
    FPendingRequests.Clear;
  finally
    FRequestsLock.Leave;
  end;

  for LWaiter in LWaiters do
    LWaiter.Cancel;
end;

procedure TNatsConnection.TearDown;
begin
  TearDown('');
end;

procedure TNatsConnection.TearDown(const AError: string);
var
  LWasOpen: Boolean;
begin
  { Safe to call from any thread INCLUDING the workers, because it never joins
    them - it only asks them to stop. The state flip is atomic, so the handlers
    run exactly once however many callers race here. }
  LWasOpen := TInterlocked.Exchange(FState, STATE_CLOSED) <> STATE_CLOSED;

  SignalStop;
  CloseChannel;
  ClearSubscriptions;
  { After ClearSubscriptions, because that is what makes a reply impossible:
    the inbox handlers are gone, so anyone still blocked in RequestSync would
    otherwise sit there until its timeout waiting for nothing }
  CancelPendingRequests;

  { Everything below happens once per connection. Tearing down an already
    closed connection is a no-op: in particular it must not overwrite the
    error that closed it, nor invent one when Close simply raced the reader. }
  if not LWasOpen then
    Exit;

  if AError <> '' then
  begin
    FWriteLock.Enter;
    try
      FLastError := AError;
    finally
      FWriteLock.Leave;
    end;

    { An application cannot see a protocol error or a dead socket any other
      way, so say why before saying that the connection is gone }
    if Assigned(FErrorHandler) then
      FErrorHandler(AError);
  end;

  if Assigned(FDisconnectHandler) then
    FDisconnectHandler();
end;

procedure TNatsConnection.HandleInfo(const AInfo: TNatsServerInfo);
begin
  if FChannel.MaxLineLength > 0 then
    if AInfo.MaxPayload > 0 then
      FChannel.MaxLineLength := AInfo.MaxPayload * 2;

  { A server sends INFO again during the session - a cluster topology change,
    or lame duck mode. Answering those with a second CONNECT is a protocol
    violation, so only the first one drives the handshake. }
  if FState <> STATE_CONNECTING then
    Exit;

  if AInfo.TlsRequired then
  begin
    { Carrying on in plaintext just gets the connection dropped by the server
      with no explanation }
    TearDown('The server requires TLS, which this client does not support yet ' +
      '(see §18 in Docs\Core-Protocol-Review.md)');
    Exit;
  end;

  if Assigned(FConnectHandler) then
    FConnectHandler(AInfo, ConnectOptions);

  SendConnect;

  { Only now is the connection usable: the server knows our options and any
    caller blocked in WaitForReady can proceed }
  TInterlocked.Exchange(FState, STATE_READY);
end;

procedure TNatsConnection.PingOnIdle;
begin
  { Nothing has arrived for a whole read timeout, which is longer than the
    server's ping interval - so either the peer is gone or it is very quiet.
    Probe it once: if the PONG for the previous probe never came, it is gone. }
  if TInterlocked.CompareExchange(FPingOutstanding, 1, 0) <> 0 then
  begin
    TearDown('The server stopped responding: no PONG within the read timeout');
    Exit;
  end;

  try
    SendPing;
  except
    on E: Exception do
      TearDown('Keep-alive PING failed: ' + E.Message);
  end;
end;

procedure TNatsConnection.JoinThreads;
begin
  { No lock may be held here: a handler running on the consumer thread may be
    inside Publish waiting for FWriteLock, and it has to finish before the
    thread can end.

    A thread is never joined from itself: the consumer tears the connection
    down on a fatal -ERR, and joining itself there would hang forever. Workers
    only ever signal; whoever owns the connection does the freeing. }
  if Assigned(FReader) and (TThread.CurrentThread.ThreadID <> FReader.ThreadID) then
  begin
    FReader.WaitFor;
    FreeAndNil(FReader);
  end;

  if Assigned(FConsumer) and (TThread.CurrentThread.ThreadID <> FConsumer.ThreadID) then
  begin
    FConsumer.WaitFor;
    FreeAndNil(FConsumer);
  end;
end;

procedure TNatsConnection.Close();
begin
  TearDown;
  JoinThreads;
end;

procedure TNatsConnection.Ping;
begin
  SendPing;
end;

procedure TNatsConnection.Publish(const ASubject, AMessage: string; const AReplyTo: string = '');
var
  LMessageBytes: TBytes;
  LPub: string;
begin
  CheckSubject(ASubject);

  LMessageBytes := TEncoding.UTF8.GetBytes(AMessage);
  if AReplyTo.IsEmpty then
    LPub := Format('%s %s %d', [NatsConstants.Protocol.PUB, ASubject, Length(LMessageBytes)])
  else
    LPub := Format('%s %s %s %d', [NatsConstants.Protocol.PUB, ASubject, AReplyTo, Length(LMessageBytes)]);

  SendCommand(LPub, LMessageBytes);
end;

procedure TNatsConnection.PublishBytes(const ASubject: string; const AData: TBytes; const AReplyTo: string = '');
var
  LPub: string;
begin
  CheckSubject(ASubject);

  if AReplyTo.IsEmpty then
    LPub := Format('%s %s %d', [NatsConstants.Protocol.PUB, ASubject, Length(AData)])
  else
    LPub := Format('%s %s %s %d', [NatsConstants.Protocol.PUB, ASubject, AReplyTo, Length(AData)]);

  SendCommand(LPub, AData);
end;

procedure TNatsConnection.Publish(const ASubject, AMessage: string; const AReplyTo: string; AHeaders: TNatsHeaders);
var
  LPayloadBytes: TBytes;
begin
  LPayloadBytes := TEncoding.UTF8.GetBytes(AMessage);
  PublishBytes(ASubject, LPayloadBytes, AReplyTo, AHeaders);
end;

procedure TNatsConnection.PublishBytes(const ASubject: string; const AData: TBytes; const AReplyTo: string; AHeaders: TNatsHeaders);
var
  LHeaderBlock: string;
  LHeaderBlockBytes: TBytes;
  LHeaderBytes, LTotalBytes: Integer;
  LPub: string;
begin
  CheckSubject(ASubject);

  if AHeaders.Count = 0 then
  begin
    PublishBytes(ASubject, AData, AReplyTo);
    Exit;
  end;

  LHeaderBlock := NatsConstants.CLIENT_HEADER_VERSION + NatsConstants.CR_LF + AHeaders.Text;

  if not LHeaderBlock.EndsWith(NatsConstants.CR_LF) then
    LHeaderBlock := LHeaderBlock + NatsConstants.CR_LF;

  LHeaderBlockBytes := TEncoding.UTF8.GetBytes(LHeaderBlock);

  { The header block must end with a blank line, and <#header bytes> must count
    it. SendString below appends the CRLF that forms that blank line, so the
    block on the wire is LHeaderBlockBytes + CRLF - which is what we declare.
    <#total bytes> is the header block plus the payload, excluding the CRLF
    that terminates the message itself. }
  LHeaderBytes := Length(LHeaderBlockBytes) + NatsConstants.CR_LF_LEN;
  LTotalBytes := LHeaderBytes + Length(AData);

  if AReplyTo.IsEmpty then
    LPub := Format('%s %s %d %d', [
      NatsConstants.Protocol.HPUB,
      ASubject,
      LHeaderBytes,
      LTotalBytes
    ])
  else
    LPub := Format('%s %s %s %d %d', [
      NatsConstants.Protocol.HPUB,
      ASubject,
      AReplyTo,
      LHeaderBytes,
      LTotalBytes
    ]);

  SendCommand(LPub, LHeaderBlock, AData);
end;

function TNatsConnection.Request(const ASubject: string; AHandler: TNatsMsgHandler): Integer;
begin
  Result := Request(ASubject, String.Empty, AHandler);
end;

function TNatsConnection.Request(const ASubject, AMessage: string; AHandler: TNatsMsgHandler): Integer;
var
  LInbox: string;
begin
  LInbox := FGenerator.GetNewInbox;
  Result := Subscribe(LInbox, AHandler);

  { A request expects exactly one reply, so arm the auto-unsubscribe before the
    request goes out: without it the inbox subscription is never removed, on
    this side or on the server's, and every request leaks one }
  { TODO -opaolo -c : no timeout yet - a reply that never arrives leaves the
    subscription in place until the connection closes }
  Unsubscribe(Result, 1);

  Publish(ASubject, AMessage, LInbox);
end;

function TNatsConnection.RequestSync(const ASubject, AMessage: string;
  out AReply: TNatsArgsMSG; ATimeoutMs: Cardinal): Boolean;
begin
  Result := RequestSync(ASubject, TEncoding.UTF8.GetBytes(AMessage), nil, AReply, ATimeoutMs);
end;

function TNatsConnection.RequestSync(const ASubject: string; const AData: TBytes;
  out AReply: TNatsArgsMSG; ATimeoutMs: Cardinal): Boolean;
begin
  Result := RequestSync(ASubject, AData, nil, AReply, ATimeoutMs);
end;

function TNatsConnection.RequestSync(const ASubject: string; const AData: TBytes;
  AHeaders: TNatsHeaders; out AReply: TNatsArgsMSG; ATimeoutMs: Cardinal): Boolean;
var
  LWaiter: INatsRequestWaiter;
  LInbox: string;
  LId: Integer;
begin
  { Before anything is allocated or sent: a bad subject should not leave a
    subscription behind, and a dead connection should say so rather than
    surface as whatever error the socket layer happens to raise }
  CheckSubject(ASubject);

  if not Connected then
    raise ENatsException.Create(
      'Cannot send a request: the connection is not ready. Open it and wait ' +
      'for WaitForReady before requesting');

  if ATimeoutMs = 0 then
    raise ENatsException.Create('A request timeout of 0 would wait forever');

  LWaiter := TNatsRequestWaiter.Create;
  LInbox := FGenerator.GetNewInbox;

  { Registered before the subscription exists, so a connection torn down at any
    point from here on releases this caller instead of stranding it }
  AddPendingRequest(LWaiter);
  try
    { Subscribe before publishing: a fast responder can have the reply on the
      wire before Publish has even returned }
    LId := Subscribe(LInbox,
      procedure (const AMsg: TNatsArgsMSG)
      begin
        { Runs on the consumer thread. LWaiter is an interface, so this closure
          holds a reference of its own and the waiter cannot be freed while
          this is executing - even if the caller below has long since timed
          out, returned, and released its reference }
        LWaiter.Signal(AMsg);
      end);
    try
      { Exactly one reply is expected, so let the server drop the subscription
        by itself the moment it delivers - the success path then needs no UNSUB
        at all, on either side }
      Unsubscribe(LId, 1);

      PublishBytes(ASubject, AData, LInbox, AHeaders);

      Result := LWaiter.WaitFor(ATimeoutMs, AReply);
    finally
      { EVERY exit path, including the timeout, a cancel, and an exception out
        of Publish. On success this finds nothing and sends nothing, because
        TakeMessageHandler already removed the subscription when Remaining hit
        zero. On a timeout it is the only thing that ever removes the inbox -
        without it each timed-out request leaks one entry from FSubscriptions
        and leaves the inbox subscribed on the server. }
      Unsubscribe(LId, 0);
    end;
  finally
    RemovePendingRequest(LWaiter);
  end;
end;

procedure TNatsConnection.Unsubscribe(AId: Integer; AMaxMsg: Integer = 0);
var
  LSub: TNatsSubscription;
  LRemaining: Integer;
  LFound: Boolean;
begin
  if AMaxMsg < 0 then
    raise ENatsException.Create('The maximum message count cannot be negative');

  { Bookkeeping under the subscription lock, the write under the write lock -
    never both at once }
  FSubsLock.Enter;
  try
    LFound := FSubscriptions.TryGetValue(AId, LSub);
    if LFound then
    begin
      if AMaxMsg = 0 then
        FSubscriptions.Remove(AId)
      else
      begin
        { <max_msgs> is the TOTAL the server will have delivered on this sid
          before it drops the subscription, not "this many more". So what is
          still to come is <max_msgs> minus what has already arrived, and if
          that count is already reached the server drops the subscription the
          moment it reads this UNSUB - so drop ours too, otherwise it would
          linger forever. }
        LSub.Expected := AMaxMsg;
        LRemaining := AMaxMsg - LSub.Received;

        if LRemaining <= 0 then
          FSubscriptions.Remove(AId)
        else
          LSub.Remaining := LRemaining;
      end;
    end;
  finally
    FSubsLock.Leave;
  end;

  if not LFound then
    Exit; // Nothing to do here!

  if AMaxMsg = 0 then
    SendCommand(Format('%s %d', [NatsConstants.Protocol.UNSUB, AId]))
  else
    SendCommand(Format('%s %d %d', [NatsConstants.Protocol.UNSUB, AId, AMaxMsg]));
end;

function TNatsConnection.TakeMessageHandler(AId: Integer; out AHandler: TNatsMsgHandler): Boolean;
var
  LSub: TNatsSubscription;
begin
  AHandler := nil;

  FSubsLock.Enter;
  try
    Result := FSubscriptions.TryGetValue(AId, LSub);
    if not Result then
      Exit;

    { All of the bookkeeping happens here, under the lock, and the handler is
      copied out. TNatsMsgHandler is a refcounted closure, so the copy keeps it
      alive even when the line below frees the subscription that owns it - which
      is what lets the caller invoke it with no lock held. LSub must not be
      touched after the Remove. }
    AHandler := LSub.Handler;
    LSub.Received := LSub.Received + 1;

    if LSub.Remaining > -1 then
    begin
      LSub.Remaining := LSub.Remaining - 1;
      if LSub.Remaining <= 0 then
        FSubscriptions.Remove(AId);
    end;
  finally
    FSubsLock.Leave;
  end;
end;

procedure TNatsConnection.SendCommand(const ALine: string);
begin
  FWriteLock.Enter;
  try
    FChannel.SendString(ALine);
  finally
    FWriteLock.Leave;
  end;
end;

procedure TNatsConnection.SendCommand(const ALine: string; const APayload: TBytes);
begin
  { One lock for the whole command: a control line and its payload must never
    be separated by another thread's write }
  FWriteLock.Enter;
  try
    FChannel.SendString(ALine);
    FChannel.SendBytes(APayload);
  finally
    FWriteLock.Leave;
  end;
end;

procedure TNatsConnection.SendCommand(const ALine, AHeaderBlock: string; const APayload: TBytes);
begin
  FWriteLock.Enter;
  try
    FChannel.SendString(ALine);
    FChannel.SendString(AHeaderBlock);
    FChannel.SendBytes(APayload);
  finally
    FWriteLock.Leave;
  end;
end;

procedure TNatsConnection.SendConnect;
begin
  SendCommand(Format('%s %s', [NatsConstants.Protocol.Connect, ConnectOptions.ToJSONString]));
end;

procedure TNatsConnection.SendPing;
begin
  SendCommand(NatsConstants.Protocol.Ping);
end;

procedure TNatsConnection.SendPong;
begin
  SendCommand(NatsConstants.Protocol.PONG);
end;

procedure TNatsConnection.SendSubscribe(AId: Integer; const ASubject, AQueue: string);
begin
  { Takes copies rather than the TNatsSubscription: by the time this runs the
    subscription lock has been released and the object may already be gone }
  if AQueue.IsEmpty then
    SendCommand(Format('%s %s %d', [NatsConstants.Protocol.SUB, ASubject, AId]))
  else
    SendCommand(Format('%s %s %s %d', [NatsConstants.Protocol.SUB, ASubject, AQueue, AId]));
end;

function TNatsConnection.SetChannel(const AHost: string; APort, AConnectTimeout: Integer;
  AReadTimeout: Integer = 0): TNatsConnection;
begin
  FChannel.Host := AHost;
  FChannel.Port := APort;

  { AConnectTimeout bounds how long establishing the connection may take. It is
    NOT how long a read may block: an idle connection is healthy, and the demo
    passes 1000 here, which as a read timeout meant the reader failed every
    single second. Leave AReadTimeout at 0 to keep the socket's default, which
    is sized to outlast the server's ping interval. }
  if AConnectTimeout > 0 then
    FChannel.ConnectTimeout := AConnectTimeout;

  if AReadTimeout > 0 then
    FChannel.ReadTimeout := AReadTimeout;

  Result := Self;
end;

function TNatsConnection.Subscribe(const ASubject: string; AHandler: TNatsMsgHandler): Integer;
begin
  Result := Subscribe(ASubject, '', AHandler);
end;

function TNatsConnection.Subscribe(const ASubject, AQueue: string; AHandler: TNatsMsgHandler): Integer;
var
  LSub: TNatsSubscription;
begin
  CheckSubject(ASubject);

  LSub := TNatsSubscription.Create(FGenerator.GetSubNextId, ASubject, AQueue, AHandler);

  { Register before sending, so a message that arrives immediately after the
    server processes the SUB always finds its subscription }
  FSubsLock.Enter;
  try
    FSubscriptions.Add(LSub.Id, LSub);
    Result := LSub.Id;
  finally
    FSubsLock.Leave;
  end;

  SendSubscribe(Result, ASubject, AQueue);
end;

procedure TNatsConnection.Unsubscribe(const ASubject: string; AMaxMsg: Integer);
var
  LPair: TPair<Integer, TNatsSubscription>;
  LId: Integer;
begin
  LId := -1;

  FSubsLock.Enter;
  try
    for LPair in FSubscriptions do
      if LPair.Value.Subject = ASubject then
      begin
        LId := LPair.Value.Id;
        Break;
      end;
  finally
    FSubsLock.Leave;
  end;

  if LId > -1 then
    Unsubscribe(LId, AMaxMsg)
  else
    raise ENatsException.CreateFmt('Subscription [%s] not found in the subscription list', [ASubject]);
end;

{ TNatsReader }

constructor TNatsReader.Create(AConnection: TNatsConnection);
begin
  inherited Create;
  FParser := TNatsParser.Create;
  FConnection := AConnection;
  FChannel := AConnection.FChannel;
  FQueue := AConnection.FReadQueue;
end;

destructor TNatsReader.Destroy;
begin
  FParser.Free;
  inherited;
end;

procedure TNatsReader.DoExecute;
var
  LRead: string;
  LCommand: TNatsCommand;
begin
  while not Terminated do
  begin
    if not FChannel.Connected then
    begin
      { The peer went away. Nothing else will ever arrive on this socket, so
        say so instead of spinning here in silence until someone calls Close }
      if not Terminated then
        FConnection.TearDown('The connection to the server was lost');

      Break;
    end;

    try
      LRead := FChannel.ReceiveString;
    except
      on E: ENatsReadTimeout do
      begin
        { Not a failure: an idle connection is a healthy one. Probe it. }
        LRead := '';
        FConnection.PingOnIdle;
      end;
      on E: Exception do
      begin
        { Any other read failure leaves the stream at an unknown offset, so
          everything after it would be parsed out of alignment. Stop. }
        FError := E.Message;
        FConnection.TearDown('Read failed: ' + E.Message);
        Break;
      end;
    end;

    if LRead.IsEmpty then
      Continue;

    try
      LCommand := FParser.Parse(LRead);
    except
      on E: Exception do
      begin
        { Previously this ran outside the try, so one unrecognized line took
          the reader thread down without a word to anybody }
        FError := E.Message;
        FConnection.TearDown('Protocol error: ' + E.Message);
        Break;
      end;
    end;

    try
      ReadMessageBody(LCommand);
    except
      on E: Exception do
      begin
        { A half-read body means the stream is no longer aligned with the
          protocol, so there is nothing sensible to resume from }
        FError := E.Message;
        FConnection.TearDown('Failed reading a message body: ' + E.Message);
        Break;
      end;
    end;

    FQueue.Enqueue(LCommand); // the queue does its own locking and signalling
  end;
end;

procedure TNatsReader.ReadMessageBody(var ACommand: TNatsCommand);
var
  LMsgArgs: TNatsArgsMSG;
  LHeaderBlockBytes: TBytes;
  LPayloadBlockBytes: TBytes;
begin
  if ACommand.CommandType = TNatsCommandServer.MSG then
  begin
    LMsgArgs := ACommand.GetArgAsMsg;
    if LMsgArgs.PayloadBytes > 0 then
      LPayloadBlockBytes := FChannel.ReceiveExactBytes(LMsgArgs.PayloadBytes)
    else
      SetLength(LPayloadBlockBytes, 0);

    FChannel.ReceiveString; // Consume the trailing CRLF after payload
    FParser.SetCommandPayload(ACommand, LPayloadBlockBytes);
  end
  else if ACommand.CommandType = TNatsCommandServer.HMSG then
  begin
    LMsgArgs := ACommand.GetArgAsMsg;
    { <#header bytes> already covers the CRLFCRLF that terminates the header
      block, so the next byte is the first payload byte: do NOT read a line here }
    if LMsgArgs.HeaderBytes > 0 then
      LHeaderBlockBytes := FChannel.ReceiveExactBytes(LMsgArgs.HeaderBytes)
    else
      SetLength(LHeaderBlockBytes, 0);

    { The status comes out of the same block as the headers - it is the tail of
      its NATS/1.0 line - and has to be carried on the message, because a status
      message has an empty body and would otherwise look like an empty message }
    FParser.ParseHeaders(TEncoding.UTF8.GetString(LHeaderBlockBytes), LMsgArgs.Headers,
      LMsgArgs.Status, LMsgArgs.Description);
    ACommand.Arguments := TValue.From<TNatsArgsMSG>(LMsgArgs);

    if LMsgArgs.PayloadBytes > 0 then
      LPayloadBlockBytes := FChannel.ReceiveExactBytes(LMsgArgs.PayloadBytes)
    else
      SetLength(LPayloadBlockBytes, 0);
    FChannel.ReceiveString; // Consume CRLF after payload block

    FParser.SetCommandPayload(ACommand, LPayloadBlockBytes);
  end;
end;

procedure TNatsReader.Execute;
begin
  NameThreadForDebugging(FConnection.Name + ' Reader');
  try
    DoExecute;
  except
    on E: Exception do
      Error := E.Message;
  end;
end;

constructor TNatsGenerator.Create;
begin
  FSubId := 0;
end;

{ TNatsGenerator }

function TNatsGenerator.GetNewInbox: string;
begin
  { A counter would only be unique within this connection: every client in the
    network would start at _INBOX.1 and replies could be delivered to the wrong
    one. TNUID is what the other NATS clients use for exactly this }
  Result := NatsConstants.INBOX_PREFIX + TNUID.NextNuid;
end;

function TNatsGenerator.GetSubNextId: Integer;
begin
  Result := TInterlocked.Increment(FSubId);
end;

{ TNatsNetwork }

function TNatsNetwork.NewConnection(const AName: string): TNatsConnection;
begin
  Result := TNatsConnection.Create;
  Result.Name := AName;
  Self.Add(AName, Result);
end;

{ TNatsConsumer }

constructor TNatsConsumer.Create(const AConnection: TNatsConnection);
begin
  inherited Create;
  FConnection := AConnection;
  FQueue := AConnection.FReadQueue;
end;

procedure TNatsConsumer.DoExecute;
var
  LCommand: TNatsCommand;
  LHandler: TNatsMsgHandler;
  LShouldDisconnect: Boolean;
begin
  LShouldDisconnect := False;
  while not Terminated do
  begin
    { Blocks until a command arrives, the timeout expires, or SignalStop wakes
      the queue - no polling, so a message is dispatched as soon as it is read }
    if not FQueue.Dequeue(LCommand, QUEUE_WAIT_MS) then
      Continue;

      case LCommand.CommandType of
        TNatsCommandServer.INFO:
        begin
          { The whole handshake - TLS check, connect handler, CONNECT, and the
            move to READY - lives in the connection, because only the first
            INFO drives it }
          FConnection.HandleInfo(LCommand.GetArgAsInfo.Info);
        end;

        TNatsCommandServer.Ping:
        begin
          FConnection.SendPong;
        end;

        TNatsCommandServer.PONG:
        begin
          { the answer to our keep-alive probe: the peer is alive }
          TInterlocked.Exchange(FConnection.FPingOutstanding, 0);
        end;

        TNatsCommandServer.MSG,
        TNatsCommandServer.HMSG:
        begin
          var LMsgArgs := LCommand.GetArgAsMsg;
          { The lookup and all the counting happen inside the connection, under
            its subscription lock; the handler comes back as a copy so it can be
            invoked here with no lock held }
          if FConnection.TakeMessageHandler(LMsgArgs.Id, LHandler) and Assigned(LHandler) then
            LHandler(LMsgArgs);
        end;

        TNatsCommandServer.OK:
        begin
          // Nothing to do here!
        end;

        TNatsCommandServer.ERR:
        begin
          FError := 'ERR from server: ' + LCommand.GetArgAsErr;
          LShouldDisconnect := True;
          Break;
        end;
      end;
  end; // while

  { TearDown, never Close: Close joins the worker threads and this IS one of
    them, so it would wait for itself forever }
  if LShouldDisconnect then
    FConnection.TearDown(FError);
end;

procedure TNatsConsumer.Execute;
begin
  NameThreadForDebugging(FConnection.Name + ' Consumer');
  try
    DoExecute;
  except
    on E: Exception do
      Error := E.Message;
  end;
end;

{ TSubscription }

constructor TNatsSubscription.Create(AId: Integer; const ASubject, AQueue: string; AHandler: TNatsMsgHandler);
begin
  Received := 0;
  Expected := -1;
  Remaining := -1;

  Id := AId;
  Subject := ASubject;
  Handler := AHandler;
  Queue := AQueue;
end;

end.
