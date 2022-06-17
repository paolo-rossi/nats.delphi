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
unit NATS.Connection;

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

  TNatsGenerator = class
  private
    FSubId: Cardinal;
    FInboxId: Cardinal;
  public
    function GetSubNextId: Cardinal;
    function GetNewInbox: string;
  end;

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

  TNatsSubscription = class
    Id: Integer;
    Subject: string;
    Handler: TNatsMsgHandler;
    Queue: string;
    Received: Integer;
    Expected: Integer;

    constructor Create(AId: Integer; const ASubject: string; AHandler: TNatsMsgHandler); overload;
  end;
  TNatsSubscriptions = TObjectDictionary<Integer, TNatsSubscription>;

  /// <summary>
  ///   TNatsConnection represents a bidirectional channel to the NATS server.
  ///   Message handler may be attached to each operation which is invoked when
  ///   the operation is processed by the server
  /// </summary>
  TNatsConnection = class
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
    procedure SendPing;
    procedure SendPong;
    procedure SendCommand(const ACommand: string); overload;
    procedure SendCommand(const ACommand: TBytes; APriority: Boolean); overload;
  private
    procedure SendSubscribe(const ASubscription: TNatsSubscription);
  public
    constructor Create;
    destructor Destroy; override;
  public
    function SetChannel(const AHost: string; APort, ATimeout: Integer): TNatsConnection;
    procedure Open(AConnectHandler: TNatsConnectHandler; ADisconnectHandler: TNatsDisconnectHandler = nil); overload;
    procedure Close();

    procedure Ping();
    procedure Connect(AOptions: TNatsConnectOptions); overload;

    procedure Publish(const ASubject, AMessage: string; const AReplyTo: string = '');

    function Request(const ASubject: string; AHandler: TNatsMsgHandler): Integer; overload;
    function Request(const ASubject, AMessage: string; AHandler: TNatsMsgHandler): Integer; overload;

    function Subscribe(const ASubject: string; AHandler: TNatsMsgHandler): Integer; overload;
    function Subscribe(const ASubject, AQueue: string; AHandler: TNatsMsgHandler): Integer; overload;

    procedure Unsubscribe(AId: Cardinal; AMaxMsg: Cardinal = 0);
  public
    property Name: string read FName write FName;
    //property Subscriptions: TNatsSubscriptions read FSubscriptions write FSubscriptions;
  end;

  TNatsNetwork = class(TObjectDictionary<string, TNatsConnection>)
  public
    function NewConnection(const AName: string): TNatsConnection;
  end;

implementation

uses
  Nats.Consts,
  Nats.Exceptions;

procedure TNatsConnection.Connect(AOptions: TNatsConnectOptions);
begin
  { TODO -opaolo -c : 07/06/2022 21:23:54 }
end;

procedure TNatsConnection.Open(AConnectHandler: TNatsConnectHandler;
    ADisconnectHandler: TNatsDisconnectHandler = nil);
begin
  FConnectHandler := AConnectHandler;
  FDisconnectHandler := ADisconnectHandler;
  FChannel.Open;
  FReader.Start;
  FConsumer.Start;
end;

constructor TNatsConnection.Create;
begin
  FReadQueue := TNatsCommandQueue.Create;
  FGenerator := TNatsGenerator.Create;
  FSubscriptions := TNatsSubscriptions.Create([doOwnsValues]);

  { TODO -opaolo -c : Remove the default behavior 31/05/2022 18:17:27 }
  FChannel := TNatsSocketRegistry.Get(String.Empty);

  FReader := TNatsReader.Create(Self);
  FConsumer := TNatsConsumer.Create(Self);
end;

destructor TNatsConnection.Destroy;
begin
  FReader.Stop;
  FConsumer.Stop;

  FReader.WaitFor;
  FReader.Free;

  FConsumer.WaitFor;
  FConsumer.Free;

  //FSocket.Free;
  FSubscriptions.Free;
  FGenerator.Free;
  FReadQueue.Free;
  inherited;
end;

procedure TNatsConnection.Close();
begin
  FChannel.Close;
end;

procedure TNatsConnection.Ping;
begin
  SendPing;
end;

procedure TNatsConnection.Publish(const ASubject, AMessage: string; const AReplyTo: string = '');
var
  LMessageBytes: TBytes;
  LSub: string;
begin
  if ASubject.IsEmpty then
    Exit;

  LMessageBytes := TEncoding.UTF8.GetBytes(AMessage);
  if AReplyTo.IsEmpty then
    LSub := Format('pub %s %d', [ASubject, Length(LMessageBytes)])
  else
    LSub := Format('pub %s %s %d', [ASubject, AReplyTo, Length(LMessageBytes)]);

  FChannel.SendString(LSub);
  FChannel.SendBytes(LMessageBytes);
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
  Publish(ASubject, AMessage, LInbox);
end;

procedure TNatsConnection.Unsubscribe(AId: Cardinal; AMaxMsg: Cardinal = 0);
var
  LSub: TNatsSubscription;
begin
  if FSubscriptions.TryGetValue(AId, LSub) then
    FSubscriptions.Remove(AId);

  if AMaxMsg = 0 then
    FChannel.SendString(Format('%s %d', [NatsConstants.Protocol.UNSUB, AId]))
  else
    FChannel.SendString(Format('%s %d %d', [NatsConstants.Protocol.UNSUB, AId, AMaxMsg]))
end;

procedure TNatsConnection.SendCommand(const ACommand: TBytes; APriority: Boolean);
begin
  FChannel.SendBytes(ACommand);
end;

procedure TNatsConnection.SendPing;
begin
  FChannel.SendString(NatsConstants.Protocol.PING);
end;

procedure TNatsConnection.SendPong;
begin
  FChannel.SendString(NatsConstants.Protocol.PONG);
end;

procedure TNatsConnection.SendSubscribe(const ASubscription: TNatsSubscription);
begin
  if ASubscription.Queue.IsEmpty then
    FChannel.SendString(Format('sub %s %d', [ASubscription.Subject, ASubscription.Id]))
  else
    FChannel.SendString(Format('sub %s %s %d', [ASubscription.Subject, ASubscription.Queue, ASubscription.Id]));
end;

function TNatsConnection.SetChannel(const AHost: string; APort, ATimeout: Integer): TNatsConnection;
begin
  FChannel.Host := AHost;
  FChannel.Port := APort;
  FChannel.Timeout := ATimeout;
  Result := Self;
end;

function TNatsConnection.Subscribe(const ASubject: string; AHandler: TNatsMsgHandler): Integer;
var
  LSub: TNatsSubscription;
begin
  LSub := TNatsSubscription.Create(FGenerator.GetSubNextId, ASubject, AHandler);
  FSubscriptions.Add(LSub.Id, LSub);
  SendSubscribe(LSub);
  Result := LSub.Id;
end;

function TNatsConnection.Subscribe(const ASubject, AQueue: string; AHandler: TNatsMsgHandler): Integer;
var
  LSub: TNatsSubscription;
begin
  LSub := TNatsSubscription.Create(FGenerator.GetSubNextId, ASubject, AHandler);
  LSub.Queue := AQueue;
  FSubscriptions.Add(LSub.Id, LSub);
  Result := LSub.Id;
end;

procedure TNatsConnection.SendCommand(const ACommand: string);
begin
  SendCommand(TEncoding.UTF8.GetBytes(ACommand), False);
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
  LStep: Integer;
begin
  while not Terminated do
  begin
    if not FChannel.Connected then
    begin
      if FStopEvent.WaitFor(1000) = wrSignaled then
        Break;

      Continue;
    end;

    LRead := FChannel.ReceiveString;
    if LRead.IsEmpty then
      Continue;

    LCommand := FParser.Parse(LRead);

    if LCommand.CommandType = TNatsCommandServer.PING then
    begin
      // not here?
      FChannel.SendString(NatsConstants.Protocol.PONG);
      Continue;
    end;

    if LCommand.CommandType = TNatsCommandServer.MSG then
    begin
      LRead := FChannel.ReceiveString;
      LCommand := FParser.ParsePayload(LCommand, LRead);
    end;

    TMonitor.Enter(FQueue);
    try
      FQueue.Enqueue(LCommand);
    finally
      TMonitor.Exit(FQueue);
    end;
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

function TNatsGenerator.GetNewInbox: string;
begin
  TMonitor.Enter(Self);
  try
    Inc(FInboxId);
    Result := 'inbox__' + FInboxId.ToString;
  finally
    TMonitor.Exit(Self);
  end;
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
  LProcess: Boolean;
  LCommand: TNatsCommand;
  LSub: TNatsSubscription;
begin
  while not Terminated do
  begin
    TMonitor.Enter(FQueue);
    try
      LProcess := FQueue.Count > 0;
      if LProcess then
        LCommand := FQueue.Dequeue;
    finally
      TMonitor.Exit(FQueue);
    end;

    if LProcess then
    case LCommand.CommandType of
      TNatsCommandServer.INFO:
      begin
        if Assigned(FConnection.FConnectHandler) then
          FConnection.FConnectHandler(LCommand.GetArgAsInfo.Info);
      end;
      TNatsCommandServer.PING:
      begin
        // ?
      end;
      TNatsCommandServer.PONG:
      begin
        // ?
      end;
      TNatsCommandServer.MSG:
      begin
        if FConnection.FSubscriptions.TryGetValue(LCommand.GetArgAsMsg.Id, LSub) then
        begin
          LSub.Received := LSub.Received + 1;
          if Assigned(LSub.Handler) then
            LSub.Handler(LCommand.GetArgAsMsg);
        end;
      end;
      TNatsCommandServer.OK:
      begin
        // ?
      end;
      TNatsCommandServer.ERR:
      begin
        // ?
      end;
    end
    else
      Sleep(100);
  end;
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

constructor TNatsSubscription.Create(AId: Integer; const ASubject: string; AHandler: TNatsMsgHandler);
begin
  Received := 0;
  Expected := -1;

  Id := AId;
  Subject := ASubject;
  Handler := AHandler;
end;

end.
