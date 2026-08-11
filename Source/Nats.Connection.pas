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
  ///   Simple Id generator for subscription and inbox
  /// </summary>
  TNatsGenerator = class
  private
    FSubId: Cardinal;
  public
    constructor Create(); // Initialize counters
    function GetSubNextId: Cardinal;
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
  /// </remarks>
  TNatsConnection = class
  private const
    STATE_CLOSED = 0;
    STATE_OPEN = 1;
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
    FWriteLock: TCriticalSection;
    FSubsLock: TCriticalSection;
    FState: Integer;

    { every write to the channel goes through one of these }
    procedure SendCommand(const ALine: string); overload;
    procedure SendCommand(const ALine: string; const APayload: TBytes); overload;
    procedure SendCommand(const ALine, AHeaderBlock: string; const APayload: TBytes); overload;

    procedure SendPing;
    procedure SendPong;
    procedure SendConnect;
    procedure SendSubscribe(AId: Integer; const ASubject, AQueue: string);

    function GetConnected: Boolean;
    function TakeMessageHandler(AId: Integer; out AHandler: TNatsMsgHandler): Boolean;
    procedure SignalStop;
    procedure JoinThreads;
    procedure CloseChannel;
    procedure ClearSubscriptions;
    procedure TearDown;
  public
    constructor Create;
    destructor Destroy; override;
  public
    function SetChannel(const AHost: string; APort, ATimeout: Integer): TNatsConnection;
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

    function Subscribe(const ASubject: string; AHandler: TNatsMsgHandler): Integer; overload;
    function Subscribe(const ASubject, AQueue: string; AHandler: TNatsMsgHandler): Integer; overload;

    procedure Unsubscribe(AId: Cardinal; AMaxMsg: Cardinal = 0); overload;
    procedure Unsubscribe(const ASubject: string; AMaxMsg: Cardinal = 0); overload;

    function GetSubscriptionList: TArray<TNatsSubscriptionInfo>;

    function GetNewInbox():string;
  public
    ConnectOptions: TNatsConnectOptions;
    property Name: string read FName write FName;
    property Connected: Boolean read GetConnected;
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
  Nats.Consts,
  Nats.Nuid,
  Nats.Exceptions;

const
  /// How long the consumer waits on the queue before re-checking Terminated
  QUEUE_WAIT_MS = 250;

{ TNatsConnection }

constructor TNatsConnection.Create;
begin
  inherited Create;

  FWriteLock := TCriticalSection.Create;
  FSubsLock := TCriticalSection.Create;
  FState := STATE_CLOSED;
  FReadQueue := TNatsCommandQueue.Create;
  FGenerator := TNatsGenerator.Create;
  FSubscriptions := TNatsSubscriptions.Create([doOwnsValues]);

  ConnectOptions.lang := 'Delphi';
  ConnectOptions.version := NatsConstants.CLIENT_VERSION;
  ConnectOptions.protocol := 1;
  ConnectOptions.echo := True;
  ConnectOptions.headers := True;

  { TODO -opaolo -c : Remove the default behavior 31/05/2022 18:17:27 }
  FChannel := TNatsSocketRegistry.Get(String.Empty);
end;

destructor TNatsConnection.Destroy;
begin
  Close();

  FSubscriptions.Free;
  FGenerator.Free;
  FReadQueue.Free;
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
  FWriteLock.Enter;
  try
    if Connected then
      Exit; // Already open or opening

    FConnectHandler := AConnectHandler;
    FDisconnectHandler := ADisconnectHandler;
    FChannel.Open;
    FState := STATE_OPEN;

    FReader := TNatsReader.Create(Self);
    FConsumer := TNatsConsumer.Create(Self);

    FReader.Start;
    FConsumer.Start;
  finally
    FWriteLock.Leave;
  end;
end;

function TNatsConnection.GetConnected: Boolean;
begin
  Result := (FState = STATE_OPEN) and Assigned(FChannel) and FChannel.Connected;
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

procedure TNatsConnection.TearDown;
var
  LWasOpen: Boolean;
begin
  { Safe to call from any thread INCLUDING the workers, because it never joins
    them - it only asks them to stop. The state flip is atomic, so the
    disconnect handler runs exactly once however many callers race here. }
  LWasOpen := TInterlocked.Exchange(FState, STATE_CLOSED) = STATE_OPEN;

  SignalStop;
  CloseChannel;
  ClearSubscriptions;

  if LWasOpen and Assigned(FDisconnectHandler) then
    FDisconnectHandler();
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
  if ASubject.IsEmpty then
    Exit;

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
  if ASubject.IsEmpty then
    Exit;

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
  if ASubject.IsEmpty then
    Exit;

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
  Unsubscribe(Cardinal(Result), 1);

  Publish(ASubject, AMessage, LInbox);
end;

procedure TNatsConnection.Unsubscribe(AId: Cardinal; AMaxMsg: Cardinal = 0);
var
  LSub: TNatsSubscription;
  LRemaining: Integer;
  LFound: Boolean;
begin
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
        LRemaining := Integer(AMaxMsg) - LSub.Received;

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

function TNatsConnection.SetChannel(const AHost: string; APort, ATimeout: Integer): TNatsConnection;
begin
  FChannel.Host := AHost;
  FChannel.Port := APort;
  FChannel.Timeout := ATimeout;
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

procedure TNatsConnection.Unsubscribe(const ASubject: string; AMaxMsg: Cardinal);
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
    Unsubscribe(Cardinal(LId), AMaxMsg)
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
  //LStep: Integer;

  LMsgArgs: TNatsArgsMSG;
  LHeaderBlockBytes: TBytes;
  LPayloadBlockBytes: TBytes;
begin
  while not Terminated do
  begin
    if not FChannel.Connected then
    begin
      if FStopEvent.WaitFor(1000) = wrSignaled then
        Break;

      Continue;
    end;

    try
      LRead := FChannel.ReceiveString;
    except
      on E: Exception do
      begin
        LRead := '';
        FError := E.Message;
      end;
    end;

    if LRead.IsEmpty then
      Continue;

    LCommand := FParser.Parse(LRead);

    if LCommand.CommandType = TNatsCommandServer.MSG then
    begin
      LMsgArgs := LCommand.GetArgAsMsg;
      if LMsgArgs.PayloadBytes > 0 then
        LPayloadBlockBytes := FChannel.ReceiveExactBytes(LMsgArgs.PayloadBytes)
      else
        SetLength(LPayloadBlockBytes, 0);

      LRead := FChannel.ReceiveString; // Consume the trailing CRLF after payload
      LCommand := FParser.SetCommandPayload(LCommand, TEncoding.UTF8.GetString(LPayloadBlockBytes));
    end
    else if LCommand.CommandType = TNatsCommandServer.HMSG then
    begin
      LMsgArgs := LCommand.GetArgAsMsg;
      { <#header bytes> already covers the CRLFCRLF that terminates the header
        block, so the next byte is the first payload byte: do NOT read a line here }
      if LMsgArgs.HeaderBytes > 0 then
        LHeaderBlockBytes := FChannel.ReceiveExactBytes(LMsgArgs.HeaderBytes)
      else
        SetLength(LHeaderBlockBytes, 0);

      FParser.ParseHeaders(TEncoding.UTF8.GetString(LHeaderBlockBytes), LMsgArgs.Headers);
      LCommand.Arguments := TValue.From<TNatsArgsMSG>(LMsgArgs);

      if LMsgArgs.PayloadBytes > 0 then
        LPayloadBlockBytes := FChannel.ReceiveExactBytes(LMsgArgs.PayloadBytes)
      else
        SetLength(LPayloadBlockBytes, 0);
      LRead := FChannel.ReceiveString; // Consume CRLF after payload block

      LCommand := FParser.SetCommandPayload(LCommand, TEncoding.UTF8.GetString(LPayloadBlockBytes));
    end;

    FQueue.Enqueue(LCommand); // the queue does its own locking and signalling
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

function TNatsGenerator.GetSubNextId: Cardinal;
begin
  TMonitor.Enter(Self);
  try
    Inc(FSubId);
    Result := FSubId;
  finally
    TMonitor.Exit(Self);
  end;
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
          { TODO -opaolo -c : read the TLSs parameters and (if) upgrade the connection 23/06/2022 11:00:44 }
          if Assigned(FConnection.FConnectHandler) then
            FConnection.FConnectHandler(LCommand.GetArgAsInfo.INFO, FConnection.ConnectOptions);

          if (FConnection.FChannel.MaxLineLength > 0) and (LCommand.GetArgAsInfo.Info.max_payload > 0) then
            FConnection.FChannel.MaxLineLength := LCommand.GetArgAsInfo.INFO.max_payload * 2;

          { Send CONNECT message to NATS }
          FConnection.SendConnect;
        end;

        TNatsCommandServer.Ping:
        begin
          FConnection.SendPong;
        end;

        TNatsCommandServer.PONG:
        begin
          { TODO -opaolo -c : Manage an handler set on the Ping? 23/06/2022 11:03:35 }
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
          FError := 'ERR from server: ' + LCommand.Arguments.ToString; // Placeholder
          LShouldDisconnect := True;
          Break;
        end;
      end;
  end; // while

  { TearDown, never Close: Close joins the worker threads and this IS one of
    them, so it would wait for itself forever }
  if LShouldDisconnect then
    FConnection.TearDown;
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
