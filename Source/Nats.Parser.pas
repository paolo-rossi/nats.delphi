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
unit NATS.Parser;

interface

{$SCOPEDENUMS ON}

uses
  System.SysUtils,System.Classes, System.Rtti, System.Generics.Collections,

  Nats.Entities,
  Nats.Classes;

type
      TNatsParser = class
      private
        function ParseINFO(const ACommand: string): TNatsCommand;
        function ParseMSG(const ACommand: string): TNatsCommand;
        function ParseHMSG(const ACommand: string): TNatsCommand; // New for HMSG
      public
        constructor Create;
        // Parse the initial command line (e.g., "MSG subject sid len")
        function Parse(const ACommand: string): TNatsCommand;
        // Parse headers from a raw string block
        procedure ParseHeaders(const AHeaderBlock: string; ADestHeaders: TStringList);
        // Set payload for a command (used after headers and payload are read separately)
        function SetCommandPayload(var ACmd: TNatsCommand; const APayload: string): TNatsCommand;
      end;

    implementation

    uses
      Nats.Consts,
      Nats.Exceptions;

    { TNatsParser }

    constructor TNatsParser.Create;
    begin
      // Initialization if needed
    end;

    function TNatsParser.Parse(const ACommand: string): TNatsCommand;
    var
      LTrimmedCmd: string;
    begin
      LTrimmedCmd := Trim(ACommand);

      if LTrimmedCmd.StartsWith(NatsConstants.Protocol.INFO) then
        Exit(ParseINFO(LTrimmedCmd));

       if LTrimmedCmd.StartsWith(NatsConstants.Protocol.HMSG) then // Check HMSG before MSG
        Exit(ParseHMSG(LTrimmedCmd));

       if LTrimmedCmd.StartsWith(NatsConstants.Protocol.MSG) then
        Exit(ParseMSG(LTrimmedCmd));

      if LTrimmedCmd.StartsWith(NatsConstants.Protocol.PING) then
      begin
        Result.CommandType := TNatsCommandServer.PING;
        Exit(Result);
      end;

      if LTrimmedCmd.StartsWith(NatsConstants.Protocol.PONG) then
      begin
        Result.CommandType := TNatsCommandServer.PONG;
        Exit(Result);
      end;

      if LTrimmedCmd.StartsWith(NatsConstants.Protocol.OK) then
      begin
        Result.CommandType := TNatsCommandServer.OK;
        Exit(Result);
      end;

      if LTrimmedCmd.StartsWith(NatsConstants.Protocol.ERR) then
      begin
        Result.CommandType := TNatsCommandServer.ERR;
        Exit(Result);
      end;

      raise ENatsException.Create('Parsing error or NATS command not supported: ' + ACommand);
    end;

    procedure TNatsParser.ParseHeaders(const AHeaderBlock: string; ADestHeaders: TStringList);
    var
      Lines: TArray<string>;
      S: string;
      P: Integer;
      Key, Value: string;
      IsFirstLine: Boolean;
    begin
      ADestHeaders.Clear;
      Lines := AHeaderBlock.Split([NatsConstants.CR_LF]);
      IsFirstLine := True;

      for S in Lines do
      begin
        if Trim(S) = '' then Continue; // Skip empty lines

        if IsFirstLine and S.StartsWith(NatsConstants.CLIENT_HEADER_VERSION) then // Check for NATS/1.0
        begin
          IsFirstLine := False;
          Continue; // Skip the version line itself from being parsed as a Key:Value
        end;
        IsFirstLine := False; // No longer the first line after one iteration

        P := Pos(':', S);
        if P > 0 then
        begin
          Key := Trim(Copy(S, 1, P - 1));
          Value := Trim(Copy(S, P + 1, Length(S)));
          ADestHeaders.AddPair(Key, Value);
        end
        else
        begin
          // This might be a malformed header or the NATS/1.0 line if not handled above
          // For robustness, one might log this or handle it based on strictness
        end;
      end;
    end;

    function TNatsParser.SetCommandPayload(var ACmd: TNatsCommand; const APayload: string): TNatsCommand;
    var
      LArg: TNatsArgsMSG;
    begin
      if (ACmd.CommandType = TNatsCommandServer.MSG) or (ACmd.CommandType = TNatsCommandServer.HMSG) then
      begin
        LArg := ACmd.Arguments.AsType<TNatsArgsMSG>;
        LArg.Payload := APayload;
        // PayloadBytes should accurately reflect the bytes of the *actual* payload,
        // not necessarily Length(APayload) if there are multi-byte UTF8 characters.
        // The server sends the byte count, so we trust that.
        // This method is more about associating the read payload string with the command.
        ACmd.Arguments := TValue.From<TNatsArgsMSG>(LArg);
      end;
      Result := ACmd;
    end;

    function TNatsParser.ParseINFO(const ACommand: string): TNatsCommand;
    var
      LArg: TNatsArgsINFO;
      LJsonInfoPart: string;
    begin
      Result.CommandType := TNatsCommandServer.INFO;
      // INFO is followed by a space, then the JSON payload.
      // Example: "INFO {...}"
      LJsonInfoPart := Trim(Copy(ACommand, Length(NatsConstants.Protocol.INFO) + 2, MaxInt));
      if LJsonInfoPart = '' then
        raise ENatsException.Create('Malformed NATS command received (INFO): Missing JSON payload. Command: ' + ACommand);

      LArg.InfoStr := LJsonInfoPart;
      Result.Arguments := TValue.From<TNatsArgsINFO>(LArg);
    end;

    function TNatsParser.ParseMSG(const ACommand: string): TNatsCommand;
    var
      LSplit: TArray<string>;
      LArg: TNatsArgsMSG;
    begin
      Result.CommandType := TNatsCommandServer.MSG;

      LSplit := ACommand.Split([NatsConstants.SPC]);
      // MSG <subject> <sid> [reply-to] <#bytes>
      if (Length(LSplit) < 4) or (Length(LSplit) > 5) then
         raise ENatsException.Create('Malformed NATS command received (MSG): Incorrect number of arguments. Command: ' + ACommand);

      LArg.Subject := LSplit[1];
      LArg.Id := StrToIntDef(LSplit[2], -1);
      if LArg.Id = -1 then raise ENatsException.Create('Malformed NATS command received (MSG): Invalid SID. Command: ' + ACommand);

      if Length(LSplit) = 4 then // MSG <subject> <sid> <#bytes>
      begin
        LArg.ReplyTo := '';
        LArg.PayloadBytes := StrToIntDef(LSplit[3], -1);
        if LArg.PayloadBytes = -1 then raise ENatsException.Create('Malformed NATS command received (MSG): Invalid payload bytes. Command: ' + ACommand);
      end
      else // Length(LSplit) = 5 then // MSG <subject> <sid> <reply-to> <#bytes>
      begin
        LArg.ReplyTo := LSplit[3];
        LArg.PayloadBytes := StrToIntDef(LSplit[4], -1);
         if LArg.PayloadBytes = -1 then raise ENatsException.Create('Malformed NATS command received (MSG): Invalid payload bytes. Command: ' + ACommand);
      end;

      LArg.HeaderBytes := 0;
      LArg.TotalMsgBytes := LArg.PayloadBytes;
      Result.Arguments := TValue.From<TNatsArgsMSG>(LArg);
    end;

    function TNatsParser.ParseHMSG(const ACommand: string): TNatsCommand;
    var
      LSplit: TArray<string>;
      LArg: TNatsArgsMSG;
    begin
      Result.CommandType := TNatsCommandServer.HMSG;

      LSplit := ACommand.Split([NatsConstants.SPC]);
      // HMSG <subject> <sid> [reply-to] <#header_bytes> <#total_bytes>
      if (Length(LSplit) < 5) or (Length(LSplit) > 6) then
        raise ENatsException.Create('Malformed NATS command received (HMSG): Incorrect number of arguments. Command: ' + ACommand);

      LArg.Subject := LSplit[1];
      LArg.Id := StrToIntDef(LSplit[2], -1);
      if LArg.Id = -1 then raise ENatsException.Create('Malformed NATS command received (HMSG): Invalid SID. Command: ' + ACommand);

      if Length(LSplit) = 5 then // HMSG <subject> <sid> <#header_bytes> <#total_bytes>
      begin
        LArg.ReplyTo := '';
        LArg.HeaderBytes := StrToIntDef(LSplit[3], -1);
        LArg.TotalMsgBytes := StrToIntDef(LSplit[4], -1);
      end
      else // Length(LSplit) = 6 then // HMSG <subject> <sid> <reply-to> <#header_bytes> <#total_bytes>
      begin
        LArg.ReplyTo := LSplit[3];
        LArg.HeaderBytes := StrToIntDef(LSplit[4], -1);
        LArg.TotalMsgBytes := StrToIntDef(LSplit[5], -1);
      end;

      if LArg.HeaderBytes = -1 then raise ENatsException.Create('Malformed NATS command received (HMSG): Invalid header bytes. Command: ' + ACommand);
      if LArg.TotalMsgBytes = -1 then raise ENatsException.Create('Malformed NATS command received (HMSG): Invalid total bytes. Command: ' + ACommand);

      LArg.PayloadBytes := LArg.TotalMsgBytes - LArg.HeaderBytes;
      if LArg.PayloadBytes < 0 then
        raise ENatsException.Create('Invalid byte counts in HMSG: HeaderBytes > TotalMsgBytes. Command: ' + ACommand);

      Result.Arguments := TValue.From<TNatsArgsMSG>(LArg);
    end;

    end.
