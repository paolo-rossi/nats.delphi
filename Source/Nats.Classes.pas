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
unit Nats.Classes;

interface

{$SCOPEDENUMS ON}

uses
  System.SysUtils, System.Classes, System.SyncObjs, System.Rtti,
  System.Generics.Collections,

  Nats.Consts,
  Nats.Entities;

type
  TNatsCommandClient = (CONNECT, PING, PONG, PUB, HPUB, SUB, UNSUB); // Added HPUB for client sending
  TNatsCommandServer = (INFO, PING, PONG, MSG, HMSG, OK, ERR); // Added HMSG for server receiving

  TNatsArgsINFO = record
  private
    FInfoStr: string;
    procedure SetInfoStr(const Value: string);
  public
    Info: TNatsServerInfo;
    property InfoStr: string read FInfoStr write SetInfoStr;
  end;

  TNatsHeader = TPair<string, string>;
  TNatsHeaders = TArray<TNatsHeader>;
  TNatsHeadersHelper = record helper for TNatsHeaders
    procedure Add(const AName, AValue: string);
    procedure CopyHeaders(const AHeaders: TNatsHeaders);
    function GetIndex(const AName: string): Integer;

    function GetHeader(const AName: string): string;
    procedure SetHeader(const AName, AValue: string);

    function Count: Integer;
    function Text: string;

    function GetHeaderAsInt(const AName: string; ADefault: UInt64): UInt64;
  end;


  TNatsArgsMSG = record
    Id: Integer;            // Subscription ID
    Subject: string;
    ReplyTo: string;
    PayloadBytes: Integer;  // Length of the actual message payload
    Payload: string;        // The message payload
    HeaderBytes: Integer;   // Length of the header block (for HMSG)
    TotalMsgBytes: Integer; // Total bytes for HMSG (HeaderBytes + PayloadBytes)
    Headers: TNatsHeaders;  // Parsed NATS headers
  end;

  TNatsCommand = record
    CommandType: TNatsCommandServer;
    Arguments: TValue;

    function GetArgAsInfo: TNatsArgsINFO;
    function GetArgAsMsg: TNatsArgsMSG;
  end;

  /// <summary>
  ///   Thread-safe command queue with a wait/signal handshake: the reader
  ///   thread enqueues, the consumer thread blocks in Dequeue until something
  ///   arrives instead of polling, and Wake releases it at shutdown
  /// </summary>
  TNatsCommandQueue = class
  private
    FItems: TQueue<TNatsCommand>;
    FLock: TCriticalSection;
    FEvent: TLightweightEvent;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Enqueue(const ACommand: TNatsCommand);
    /// <summary>
    ///   Waits up to ATimeoutMs for a command; False when none arrived
    /// </summary>
    function Dequeue(out ACommand: TNatsCommand; ATimeoutMs: Cardinal): Boolean;
    /// <summary>
    ///   Releases anyone blocked in Dequeue, without enqueuing anything
    /// </summary>
    procedure Wake;
    procedure Clear;
    function Count: Integer;
  end;

  TNatsMsgHandler = reference to procedure (const AMsg: TNatsArgsMSG);
  TNatsPingHandler = reference to procedure ();
  TNatsConnectHandler = reference to procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions);
  TNatsDisconnectHandler = reference to procedure ();

  TNatsThread = class abstract(TThread)
  protected
    FStopEvent: TLightweightEvent;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Stop;
  end;

implementation

{ TNatsCommandQueue }

constructor TNatsCommandQueue.Create;
begin
  inherited Create;
  FItems := TQueue<TNatsCommand>.Create;
  FLock := TCriticalSection.Create;
  FEvent := TLightweightEvent.Create;
end;

destructor TNatsCommandQueue.Destroy;
begin
  FEvent.Free;
  FItems.Free;
  FLock.Free;
  inherited;
end;

procedure TNatsCommandQueue.Enqueue(const ACommand: TNatsCommand);
begin
  FLock.Enter;
  try
    FItems.Enqueue(ACommand);
  finally
    FLock.Leave;
  end;
  FEvent.SetEvent;
end;

function TNatsCommandQueue.Dequeue(out ACommand: TNatsCommand; ATimeoutMs: Cardinal): Boolean;

  function TryTake: Boolean;
  begin
    FLock.Enter;
    try
      Result := FItems.Count > 0;
      if Result then
        ACommand := FItems.Dequeue;
      { Reset while holding the lock: an Enqueue between here and the WaitFor
        below signals the event again, so nothing is missed }
      if FItems.Count = 0 then
        FEvent.ResetEvent;
    finally
      FLock.Leave;
    end;
  end;

begin
  Result := TryTake;
  if Result then
    Exit;

  if FEvent.WaitFor(ATimeoutMs) <> TWaitResult.wrSignaled then
    Exit(False);

  Result := TryTake;
end;

procedure TNatsCommandQueue.Wake;
begin
  FEvent.SetEvent;
end;

procedure TNatsCommandQueue.Clear;
begin
  FLock.Enter;
  try
    FItems.Clear;
    FEvent.ResetEvent;
  finally
    FLock.Leave;
  end;
end;

function TNatsCommandQueue.Count: Integer;
begin
  FLock.Enter;
  try
    Result := FItems.Count;
  finally
    FLock.Leave;
  end;
end;

{ TNatsThread }
constructor TNatsThread.Create;
begin
  inherited Create(True);
  FStopEvent := TLightweightEvent.Create;
end;

destructor TNatsThread.Destroy;
begin
  FStopEvent.Free;
  inherited;
end;

procedure TNatsThread.Stop;
begin
  FStopEvent.SetEvent;
  Terminate;
end;

{ TNatsCommand }
function TNatsCommand.GetArgAsInfo: TNatsArgsINFO;
begin
  Result := Arguments.AsType<TNatsArgsINFO>;
end;

function TNatsCommand.GetArgAsMsg: TNatsArgsMSG;
begin
  Result := Arguments.AsType<TNatsArgsMSG>;
end;

{ TNatsArgsINFO }
procedure TNatsArgsINFO.SetInfoStr(const Value: string);
begin
  FInfoStr := Value;
  Info := TNatsServerInfo.FromJSONString(Value); // Uses FromJSONString from Nats.Entities
end;

{ TNatsHeadersHelper }

procedure TNatsHeadersHelper.Add(const AName, AValue: string);
begin
  Self := Self + [TNatsHeader.Create(AName, AValue)];
end;

procedure TNatsHeadersHelper.CopyHeaders(const AHeaders: TNatsHeaders);
begin
  for var pair in AHeaders do
    Self.Add(pair.Key, pair.Value);
end;

function TNatsHeadersHelper.Count: Integer;
begin
  Result := Length(Self);
end;

function TNatsHeadersHelper.GetHeader(const AName: string): string;
begin
  Result := '';
  for var pair in Self do
    if pair.Key = AName then
      Exit(pair.Value);
end;

function TNatsHeadersHelper.GetHeaderAsInt(const AName: string; ADefault: UInt64): UInt64;
var
  LHeader: string;
begin
  LHeader := GetHeader(AName);
  if LHeader.IsEmpty then
    Result := ADefault
  else
    Result := StrToUInt64(LHeader);
end;

function TNatsHeadersHelper.GetIndex(const AName: string): Integer;
begin
  Result := -1;
  for var LIndex := 0 to Length(Self) - 1 do
    if Self[LIndex].Key = AName then
      Exit(LIndex);
end;

procedure TNatsHeadersHelper.SetHeader(const AName, AValue: string);
begin
  var idx := GetIndex(AName);
  if idx = -1 then
    Self.Add(AName, AValue)
  else
    Self[idx].Value := AValue;
end;

function TNatsHeadersHelper.Text: string;
begin
  Result := '';
  { NATS headers use the HTTP form "Key: Value", the same form TNatsParser reads }
  for var pair in Self do
    Result := Result + pair.Key + NatsConstants.COL + NatsConstants.SPC +
      pair.Value + NatsConstants.CR_LF;
end;

end.
