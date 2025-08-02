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
unit Nats.Socket.Indy;

interface

uses
  System.SysUtils, System.Classes,
  IdTCPClient, IdTCPConnection, IdGlobal,
  Nats.Consts,
  Nats.Socket,
  Nats.Exceptions;

type
      TNatsSocketIndy = class(TNatsSocket)
      private
        FClient: TIdTCPClient;
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
      public
        procedure Open(); override;
        procedure Close(); override;

        procedure SendBytes(const AValue: TBytes); override;
        procedure SendString(const AValue: string); override;

        function ReceiveString: string; override;
        function ReceiveBytes: TBytes; override;
        function ReceiveExactBytes(ACount: Integer): TBytes; override;
      end;

    implementation

    { Utils }
    function IdBytesToBytes(const AIdBytes: TIdBytes): TBytes;
    begin
      SetLength(Result, Length(AIdBytes));
      if Length(AIdBytes) > 0 then
        Move(AIdBytes[0], Result[0], Length(AIdBytes));
    end;

    { TNatsSocketIndy }

    procedure TNatsSocketIndy.Close;
    begin
      if Assigned(FClient) and FClient.Connected then
        FClient.Disconnect;
    end;

    constructor TNatsSocketIndy.Create;
    begin
      inherited Create; // Call inherited constructor
      FClient := TIdTCPClient.Create(nil);
      FClient.ReadTimeout := NatsConstants.DEFAULT_PING_INTERVAL * 3;
      // Extract host from DEFAULT_URI if it contains protocol
      var LDefaultHost: string;
      LDefaultHost := NatsConstants.DEFAULT_URI_;
      if LDefaultHost.StartsWith('nats://') then
        LDefaultHost := LDefaultHost.Substring(Length('nats://'));
      if Pos(':', LDefaultHost) > 0 then // Remove port if present in host part
        LDefaultHost := LDefaultHost.Substring(0, Pos(':', LDefaultHost) -1);

      FClient.Host := LDefaultHost;
      FClient.Port := NatsConstants.DEFAULT_PORT;
    end;

    destructor TNatsSocketIndy.Destroy;
    begin
      FClient.Free;
      inherited;
    end;

    function TNatsSocketIndy.GetConnected: Boolean;
    begin
      Result := Assigned(FClient) and FClient.Connected;
    end;

    function TNatsSocketIndy.GetHost: string;
    begin
      Result := FClient.Host;
    end;

    function TNatsSocketIndy.GetPort: Integer;
    begin
      Result := FClient.Port;
    end;

    function TNatsSocketIndy.GetTimeout: Cardinal;
    begin
      Result := FClient.ReadTimeout;
    end;

    function TNatsSocketIndy.GetMaxLineLength: Cardinal;
    begin
      Result := FClient.IOHandler.MaxLineLength;
    end;

    procedure TNatsSocketIndy.Open;
    begin
      if FClient.Host = '' then
        raise ENatsException.Create('Host not set for NATS connection.');
      if FClient.Port = 0 then // Should be set by constructor, but as a fallback
        FClient.Port := NatsConstants.DEFAULT_PORT;
      try
        FClient.Connect;
      except
        on E: Exception do
          raise ENatsException.CreateFmt('Failed to connect to NATS server %s:%d. Error: %s', [FClient.Host, FClient.Port, E.Message]);
      end;
    end;

    function TNatsSocketIndy.ReceiveBytes: TBytes;
    var
      LRes: string;
    begin
      LRes := FClient.IOHandler.ReadLn(IndyTextEncoding_UTF8);
      Result := TEncoding.UTF8.GetBytes(LRes);
    end;

    function TNatsSocketIndy.ReceiveString: string;
    begin
      Result := FClient.IOHandler.ReadLn(NatsConstants.CR_LF, IndyTextEncoding_UTF8);
    end;

    function TNatsSocketIndy.ReceiveExactBytes(ACount: Integer): TBytes;
    var
      LIdBytes: TIdBytes;
    begin
      if ACount <= 0 then
      begin
        SetLength(Result, 0);
        Exit;
      end;
      if not Assigned(FClient.IOHandler) then
        raise ENatsException.Create('IOHandler not assigned in TNatsSocketIndy.ReceiveExactBytes');

      // ReadBytes reads exactly ACount bytes into LIdBytes.
      FClient.IOHandler.ReadBytes(LIdBytes, ACount, False); // False for AAppend (replace content)
      Result := IdBytesToBytes(LIdBytes);

      if Length(Result) <> ACount then // Should not happen if ReadBytes succeeds without exception
        raise ENatsException.CreateFmt('Failed to read exactly %d bytes. Read %d bytes.', [ACount, Length(Result)]);
    end;

    procedure TNatsSocketIndy.SendBytes(const AValue: TBytes);
    begin
      FClient.IOHandler.Write(TIdBytes(AValue));
      FClient.IOHandler.Write(NatsConstants.CR_LF);
    end;

    procedure TNatsSocketIndy.SendString(const AValue: string);
    begin
      FClient.IOHandler.Write(AValue, IndyTextEncoding_UTF8);
      FClient.IOHandler.Write(NatsConstants.CR_LF);
    end;

    procedure TNatsSocketIndy.SetHost(const Value: string);
    begin
      FClient.Host := Value;
    end;

    procedure TNatsSocketIndy.SetMaxLineLength(const Value: Cardinal);
    begin
      FClient.IOHandler.MaxLineLength := Value;
    end;

    procedure TNatsSocketIndy.SetPort(const Value: Integer);
    begin
      FClient.Port := Value;
    end;

    procedure TNatsSocketIndy.SetTimeout(const Value: Cardinal);
    begin
      FClient.ReadTimeout := Value;
    end;


    initialization
      TNatsSocketRegistry.Register<TNatsSocketIndy>('Indy', True);
    finalization
      // Clean up if needed
    end.
