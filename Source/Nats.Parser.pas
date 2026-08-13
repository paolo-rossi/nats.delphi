{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
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
        procedure ParseHeaders(const AHeaderBlock: string; var ADestHeaders: TNatsHeaders); overload;
        /// <summary>
        ///   The full form. A header block's first line may carry a status -
        ///   "NATS/1.0 404 No Messages" - which is NOT a header pair and has no
        ///   place to live in ADestHeaders. AStatus is 0 when there is none
        /// </summary>
        procedure ParseHeaders(const AHeaderBlock: string; var ADestHeaders: TNatsHeaders;
          out AStatus: Integer; out ADescription: string); overload;
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
      LStatus: Integer;
      LDescription: string;
    begin
      { For callers that only want the pairs. There is one implementation, so
        the two forms can never drift apart }
      ParseHeaders(AHeaderBlock, ADestHeaders, LStatus, LDescription);
    end;

    procedure TNatsParser.ParseHeaders(const AHeaderBlock: string; var ADestHeaders: TNatsHeaders;
      out AStatus: Integer; out ADescription: string);
    var
      LLines: TArray<string>;
      LLine, LKey, LValue, LRest: string;
      LPos, LSpace: Integer;
      LIsFirstLine: Boolean;
    begin
      ADestHeaders := [];
      AStatus := 0;
      ADescription := '';

      LLines := AHeaderBlock.Split([NatsConstants.CR_LF]);
      LIsFirstLine := True;

      for LLine in LLines do
      begin
        if Trim(LLine) = '' then
          Continue; // Skip empty lines

        if LIsFirstLine and LLine.StartsWith(NatsConstants.CLIENT_HEADER_VERSION) then // NATS/1.0
        begin
          LIsFirstLine := False;

          { The version line is never a header pair, but it is not always just a
            version either: anything after "NATS/1.0" is a status, the code
            first and then an optional description. Discarding it - which is
            what this used to do - makes "404 No Messages" arrive as an empty
            message with no headers, indistinguishable from a real one }
          LRest := Trim(LLine.Substring(Length(NatsConstants.CLIENT_HEADER_VERSION)));
          if LRest <> '' then
          begin
            LSpace := Pos(NatsConstants.SPC, LRest);
            if LSpace > 0 then
            begin
              AStatus := StrToIntDef(Copy(LRest, 1, LSpace - 1), 0);
              ADescription := Trim(Copy(LRest, LSpace + 1, MaxInt));
            end
            else
              AStatus := StrToIntDef(LRest, 0);   // a bare code, no description
          end;

          Continue;
        end;
        LIsFirstLine := False; // No longer the first line after one iteration

        LPos := Pos(NatsConstants.COL, LLine);
        if LPos > 0 then
        begin
          LKey := Trim(Copy(LLine, 1, LPos - 1));
          LValue := Trim(Copy(LLine, LPos + 1, Length(LLine)));
          ADestHeaders := ADestHeaders + [TNatsHeader.Create(LKey, LValue)];
        end;
        { A line with no colon is malformed. Dropping it is deliberate: the
          alternative is failing the whole message over one bad header, and the
          byte counts have already told us where the block ends }
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

      { Zero the whole record first. A local record's unmanaged fields are
        whatever was on the stack, and a plain MSG never touches Status - which
        made HasStatus true at random. Default() also covers any field added
        here later }
      LArg := Default(TNatsArgsMSG);

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

      { See ParseMSG: a local record starts as stack garbage. Status is filled
        in later by ParseHeaders, but only if there is a header block to read }
      LArg := Default(TNatsArgsMSG);

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
