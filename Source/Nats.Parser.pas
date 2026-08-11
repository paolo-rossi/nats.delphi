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
unit Nats.Parser;

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
        // Parse headers from a raw string block into ADestHeaders (which is
        // overwritten, hence "var": TNatsHeaders is a dynamic array)
        procedure ParseHeaders(const AHeaderBlock: string; var ADestHeaders: TNatsHeaders);
        // Attach the payload to a command, once it has been read off the wire.
        // A procedure, not a function that also takes a var parameter: the old
        // signature let a caller do both and left it unclear which one mattered
        procedure SetCommandPayload(var ACommand: TNatsCommand; const APayload: TBytes);
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
        { keep the reason - 'Authorization Violation' and 'Permissions
          Violation' need to reach the application, not be thrown away }
        Result.Arguments := TValue.From<string>(
          Trim(LTrimmedCmd.Substring(Length(NatsConstants.Protocol.ERR))).DeQuotedString(''''));
        Exit(Result);
      end;

      raise ENatsProtocolError.Create('Parsing error or NATS command not supported: ' + ACommand);
    end;

    procedure TNatsParser.ParseHeaders(const AHeaderBlock: string; var ADestHeaders: TNatsHeaders);
    var
      Lines: TArray<string>;
      S: string;
      P: Integer;
      LKey, LValue: string;
      IsFirstLine: Boolean;
    begin
      ADestHeaders := [];
      Lines := AHeaderBlock.Split([NatsConstants.CR_LF]);
      IsFirstLine := True;

      for S in Lines do
      begin
        if Trim(S) = '' then
          Continue; // Skip empty lines

        if IsFirstLine and S.StartsWith(NatsConstants.CLIENT_HEADER_VERSION) then // Check for NATS/1.0
        begin
          IsFirstLine := False;
          Continue; // Skip the version line itself from being parsed as a Key:Value
        end;
        IsFirstLine := False; // No longer the first line after one iteration

        P := Pos(':', S);
        if P > 0 then
        begin
          LKey := Trim(Copy(S, 1, P - 1));
          LValue := Trim(Copy(S, P + 1, Length(S)));
          ADestHeaders := ADestHeaders + [TNatsHeader.Create(LKey, LValue)];
        end
        else
        begin
          // This might be a malformed header or the NATS/1.0 line if not handled above
          // For robustness, one might log this or handle it based on strictness
        end;
      end;
    end;

    procedure TNatsParser.SetCommandPayload(var ACommand: TNatsCommand; const APayload: TBytes);
    var
      LArg: TNatsArgsMSG;
    begin
      if (ACommand.CommandType <> TNatsCommandServer.MSG) and
         (ACommand.CommandType <> TNatsCommandServer.HMSG) then
        Exit;

      LArg := ACommand.Arguments.AsType<TNatsArgsMSG>;

      { Keep the bytes as they arrived - decoding to a string is lossy for
        anything that is not text - and offer the UTF-8 reading alongside.
        PayloadBytes stays the count the server declared: it is authoritative,
        and Length(Payload) is not, because a character is not a byte. }
      LArg.PayloadData := APayload;

      { GetString RAISES on bytes that are not valid UTF-8. Letting that
        propagate would take the whole connection down over one binary
        message, so a payload that is not text simply has no string form -
        PayloadData still holds every byte of it. }
      try
        LArg.Payload := TEncoding.UTF8.GetString(APayload);
      except
        on E: Exception do
          LArg.Payload := '';
      end;

      ACommand.Arguments := TValue.From<TNatsArgsMSG>(LArg);
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
        raise ENatsProtocolError.Create('Malformed NATS command received (INFO): Missing JSON payload. Command: ' + ACommand);

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
         raise ENatsProtocolError.Create('Malformed NATS command received (MSG): Incorrect number of arguments. Command: ' + ACommand);

      LArg.Subject := LSplit[1];
      LArg.Id := StrToIntDef(LSplit[2], -1);
      if LArg.Id = -1 then raise ENatsProtocolError.Create('Malformed NATS command received (MSG): Invalid SID. Command: ' + ACommand);

      if Length(LSplit) = 4 then // MSG <subject> <sid> <#bytes>
      begin
        LArg.ReplyTo := '';
        LArg.PayloadBytes := StrToIntDef(LSplit[3], -1);
        if LArg.PayloadBytes = -1 then raise ENatsProtocolError.Create('Malformed NATS command received (MSG): Invalid payload bytes. Command: ' + ACommand);
      end
      else // Length(LSplit) = 5 then // MSG <subject> <sid> <reply-to> <#bytes>
      begin
        LArg.ReplyTo := LSplit[3];
        LArg.PayloadBytes := StrToIntDef(LSplit[4], -1);
         if LArg.PayloadBytes = -1 then raise ENatsProtocolError.Create('Malformed NATS command received (MSG): Invalid payload bytes. Command: ' + ACommand);
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
        raise ENatsProtocolError.Create('Malformed NATS command received (HMSG): Incorrect number of arguments. Command: ' + ACommand);

      LArg.Subject := LSplit[1];
      LArg.Id := StrToIntDef(LSplit[2], -1);
      if LArg.Id = -1 then raise ENatsProtocolError.Create('Malformed NATS command received (HMSG): Invalid SID. Command: ' + ACommand);

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

      if LArg.HeaderBytes = -1 then raise ENatsProtocolError.Create('Malformed NATS command received (HMSG): Invalid header bytes. Command: ' + ACommand);
      if LArg.TotalMsgBytes = -1 then raise ENatsProtocolError.Create('Malformed NATS command received (HMSG): Invalid total bytes. Command: ' + ACommand);

      LArg.PayloadBytes := LArg.TotalMsgBytes - LArg.HeaderBytes;
      if LArg.PayloadBytes < 0 then
        raise ENatsProtocolError.Create('Invalid byte counts in HMSG: HeaderBytes > TotalMsgBytes. Command: ' + ACommand);

      Result.Arguments := TValue.From<TNatsArgsMSG>(LArg);
    end;

    end.
