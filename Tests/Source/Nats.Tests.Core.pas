{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Tests.Core;

{******************************************************************************}
{                                                                              }
{  Pure unit tests: parser, header helper, entities, NUID. No socket, no       }
{  threads, no server.                                                         }
{                                                                              }
{  Tests marked [KNOWN BUG §n] assert correct NATS behaviour and currently      }
{  FAIL. "n" refers to the section in Docs\Core-Protocol-Review.md.            }
{                                                                              }
{******************************************************************************}

interface

uses
  System.SysUtils, System.Classes, System.Rtti, System.Generics.Collections,

  DUnitX.TestFramework,

  Nats.Consts,
  Nats.Classes,
  Nats.Entities,
  Nats.Parser,
  Nats.Nuid,
  Nats.Exceptions;

type
  [TestFixture]
  TNatsParserTests = class
  private
    FParser: TNatsParser;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure Parse_INFO_DeserializesServerInfo;
    [Test]
    procedure Parse_INFO_WithoutPayload_Raises;
    [Test]
    procedure Parse_PING_ReturnsPingCommand;
    [Test]
    procedure Parse_PONG_ReturnsPongCommand;
    [Test]
    procedure Parse_OK_ReturnsOkCommand;
    [Test]
    procedure Parse_ERR_ReturnsErrCommand;
    [Test]
    procedure Parse_UnknownCommand_Raises;

    [Test]
    procedure Parse_MSG_WithoutReplyTo;
    [Test]
    procedure Parse_MSG_WithReplyTo;
    [Test]
    procedure Parse_MSG_InvalidSid_Raises;
    [Test]
    procedure Parse_MSG_TooFewArguments_Raises;

    [Test]
    procedure Parse_HMSG_IsNotParsedAsMSG;
    [Test]
    procedure Parse_HMSG_WithoutReplyTo;
    [Test]
    procedure Parse_HMSG_WithReplyTo;
    [Test]
    procedure Parse_HMSG_HeaderBytesExceedingTotal_Raises;

    [Test]
    procedure SetCommandPayload_AttachesPayloadToMsg;
    [Test]
    procedure SetCommandPayload_KeepsTheRawBytes;

    // [KNOWN BUG §4] ADestHeaders is passed by value, so the caller never sees
    // the parsed headers
    [Test]
    procedure ParseHeaders_FillsTheDestinationArray;
    [Test]
    procedure ParseHeaders_SkipsTheVersionLine;
  end;

  [TestFixture]
  TNatsHeadersTests = class
  public
    [Test]
    procedure Add_AppendsHeader;
    [Test]
    procedure GetHeader_ReturnsValue;
    [Test]
    procedure GetHeader_UnknownName_ReturnsEmpty;
    [Test]
    procedure GetIndex_UnknownName_ReturnsMinusOne;
    [Test]
    procedure SetHeader_ReplacesExistingValue;
    [Test]
    procedure SetHeader_AddsWhenMissing;
    [Test]
    procedure GetHeaderAsInt_ParsesValue;
    [Test]
    procedure GetHeaderAsInt_MissingName_ReturnsDefault;
    [Test]
    procedure CopyHeaders_AppendsAllPairs;

    // §3: Text must emit "Key: Value", not "Key=Value"
    [Test]
    procedure Text_UsesColonSeparator;
    // §3 + §4: what we write must be readable by what we read
    [Test]
    procedure Text_RoundTripsThroughParseHeaders;
  end;

  [TestFixture]
  TNatsEntitiesTests = class
  public
    [Test]
    procedure ServerInfo_FromJSONString_ReadsProtocolFields;
    [Test]
    procedure ServerInfo_FromJSONString_IgnoresUnknownFields;
    [Test]
    procedure ConnectOptions_ToJSONString_UsesProtocolFieldNames;
    [Test]
    procedure ConnectOptions_RoundTrip;
    // §19: without this flag the server refuses HPUB and strips headers from
    // whatever it delivers
    [Test]
    procedure ConnectOptions_HeaderSupportFlagSerializes;
  end;

  [TestFixture]
  TNatsNuidTests = class
  public
    [Test]
    procedure NextNuid_HasExpectedLength;
    [Test]
    procedure NextNuid_IsUniqueAcrossCalls;
    [Test]
    procedure NextNuid_UsesBase62Alphabet;
  end;

implementation

const
  { A realistic INFO payload from nats-server 2.10 }
  INFO_JSON =
    '{"server_id":"NDHJZQZ4YQXQ","server_name":"nats-1","version":"2.10.11",' +
    '"proto":1,"git_commit":"1e2c1f0","go":"go1.21.6","host":"0.0.0.0","port":4222,' +
    '"headers":true,"auth_required":false,"tls_required":false,"tls_available":false,' +
    '"max_payload":1048576,"jetstream":true,"client_id":7,"client_ip":"127.0.0.1"}';

  { 'NATS/1.0'#13#10 = 10 bytes, 'K: V'#13#10 = 6 bytes, terminator #13#10 = 2 bytes }
  HEADER_BLOCK = 'NATS/1.0'#13#10'K: V'#13#10#13#10;
  HEADER_BLOCK_LEN = 18;

{ TNatsParserTests }

procedure TNatsParserTests.Setup;
begin
  FParser := TNatsParser.Create;
end;

procedure TNatsParserTests.TearDown;
begin
  FParser.Free;
end;

procedure TNatsParserTests.Parse_INFO_DeserializesServerInfo;
var
  LCommand: TNatsCommand;
  LInfo: TNatsServerInfo;
begin
  LCommand := FParser.Parse(NatsConstants.Protocol.INFO + ' ' + INFO_JSON);

  Assert.IsTrue(LCommand.CommandType = TNatsCommandServer.INFO, 'command type must be INFO');

  LInfo := LCommand.GetArgAsInfo.Info;
  Assert.AreEqual('nats-1', LInfo.ServerName);
  Assert.AreEqual('2.10.11', LInfo.Version);
  Assert.AreEqual(1, LInfo.Proto);
  Assert.AreEqual(4222, LInfo.Port);
  Assert.AreEqual(1048576, LInfo.MaxPayload);
  Assert.IsTrue(LInfo.Headers, 'headers must be True');
  Assert.IsTrue(LInfo.Jetstream, 'jetstream must be True');
  Assert.IsFalse(LInfo.TlsRequired, 'tls_required must be False');
end;

procedure TNatsParserTests.Parse_INFO_WithoutPayload_Raises;
begin
  Assert.WillRaise(
    procedure
    begin
      FParser.Parse(NatsConstants.Protocol.INFO);
    end,
    ENatsProtocolError, 'INFO without a JSON payload must be rejected');
end;

procedure TNatsParserTests.Parse_PING_ReturnsPingCommand;
begin
  Assert.IsTrue(FParser.Parse(NatsConstants.Protocol.PING).CommandType = TNatsCommandServer.PING);
end;

procedure TNatsParserTests.Parse_PONG_ReturnsPongCommand;
begin
  Assert.IsTrue(FParser.Parse(NatsConstants.Protocol.PONG).CommandType = TNatsCommandServer.PONG);
end;

procedure TNatsParserTests.Parse_OK_ReturnsOkCommand;
begin
  Assert.IsTrue(FParser.Parse(NatsConstants.Protocol.OK).CommandType = TNatsCommandServer.OK);
end;

procedure TNatsParserTests.Parse_ERR_ReturnsErrCommand;
begin
  Assert.IsTrue(
    FParser.Parse(NatsConstants.Protocol.ERR + ' ''Authorization Violation''').CommandType =
    TNatsCommandServer.ERR);
end;

procedure TNatsParserTests.Parse_UnknownCommand_Raises;
begin
  Assert.WillRaise(
    procedure
    begin
      FParser.Parse('WAT this is not nats');
    end,
    ENatsProtocolError);
end;

procedure TNatsParserTests.Parse_MSG_WithoutReplyTo;
var
  LArgs: TNatsArgsMSG;
begin
  LArgs := FParser.Parse('MSG foo.bar 7 11').GetArgAsMsg;

  Assert.AreEqual('foo.bar', LArgs.Subject);
  Assert.AreEqual(7, LArgs.Id);
  Assert.AreEqual('', LArgs.ReplyTo);
  Assert.AreEqual(11, LArgs.PayloadBytes);
  Assert.AreEqual(0, LArgs.HeaderBytes);
  Assert.AreEqual(11, LArgs.TotalMsgBytes);
end;

procedure TNatsParserTests.Parse_MSG_WithReplyTo;
var
  LArgs: TNatsArgsMSG;
begin
  LArgs := FParser.Parse('MSG foo.bar 7 _INBOX.42 11').GetArgAsMsg;

  Assert.AreEqual('foo.bar', LArgs.Subject);
  Assert.AreEqual(7, LArgs.Id);
  Assert.AreEqual('_INBOX.42', LArgs.ReplyTo);
  Assert.AreEqual(11, LArgs.PayloadBytes);
end;

procedure TNatsParserTests.Parse_MSG_InvalidSid_Raises;
begin
  Assert.WillRaise(
    procedure
    begin
      FParser.Parse('MSG foo.bar not-a-number 11');
    end,
    ENatsProtocolError);
end;

procedure TNatsParserTests.Parse_MSG_TooFewArguments_Raises;
begin
  Assert.WillRaise(
    procedure
    begin
      FParser.Parse('MSG foo.bar 7');
    end,
    ENatsProtocolError);
end;

procedure TNatsParserTests.Parse_HMSG_IsNotParsedAsMSG;
begin
  Assert.IsTrue(
    FParser.Parse('HMSG foo 1 18 23').CommandType = TNatsCommandServer.HMSG,
    'HMSG must be matched before MSG');
end;

procedure TNatsParserTests.Parse_HMSG_WithoutReplyTo;
var
  LArgs: TNatsArgsMSG;
begin
  LArgs := FParser.Parse('HMSG foo 1 18 23').GetArgAsMsg;

  Assert.AreEqual('foo', LArgs.Subject);
  Assert.AreEqual(1, LArgs.Id);
  Assert.AreEqual('', LArgs.ReplyTo);
  Assert.AreEqual(18, LArgs.HeaderBytes);
  Assert.AreEqual(23, LArgs.TotalMsgBytes);
  Assert.AreEqual(5, LArgs.PayloadBytes, 'payload bytes = total - header');
end;

procedure TNatsParserTests.Parse_HMSG_WithReplyTo;
var
  LArgs: TNatsArgsMSG;
begin
  LArgs := FParser.Parse('HMSG foo 1 _INBOX.9 18 23').GetArgAsMsg;

  Assert.AreEqual('_INBOX.9', LArgs.ReplyTo);
  Assert.AreEqual(18, LArgs.HeaderBytes);
  Assert.AreEqual(5, LArgs.PayloadBytes);
end;

procedure TNatsParserTests.Parse_HMSG_HeaderBytesExceedingTotal_Raises;
begin
  Assert.WillRaise(
    procedure
    begin
      FParser.Parse('HMSG foo 1 30 23');
    end,
    ENatsProtocolError);
end;

procedure TNatsParserTests.SetCommandPayload_AttachesPayloadToMsg;
var
  LCommand: TNatsCommand;
begin
  LCommand := FParser.Parse('MSG foo 1 5');
  FParser.SetCommandPayload(LCommand, TEncoding.UTF8.GetBytes('hello'));

  Assert.AreEqual('hello', LCommand.GetArgAsMsg.Payload);
end;

procedure TNatsParserTests.SetCommandPayload_KeepsTheRawBytes;
var
  LCommand: TNatsCommand;
  LBinary: TBytes;
  LArgs: TNatsArgsMSG;
begin
  // 0x00 and 0xFF are not valid UTF-8: decoding to a string loses them
  LBinary := [0, 255, 16, 200, 7];

  LCommand := FParser.Parse('MSG foo 1 5');
  FParser.SetCommandPayload(LCommand, LBinary);

  LArgs := LCommand.GetArgAsMsg;
  Assert.AreEqual(5, Length(LArgs.PayloadData), 'the raw payload must be preserved');
  Assert.AreEqual<TBytes>(LBinary, LArgs.PayloadData,
    'a binary payload must survive byte for byte');
end;

procedure TNatsParserTests.ParseHeaders_FillsTheDestinationArray;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;
  FParser.ParseHeaders(HEADER_BLOCK, LHeaders);

  // §4: ADestHeaders is a var parameter, so the parsed headers reach the caller
  Assert.AreEqual(1, LHeaders.Count, 'the parsed headers must reach the caller');
  Assert.AreEqual('V', LHeaders.GetHeader('K'));
end;

procedure TNatsParserTests.ParseHeaders_SkipsTheVersionLine;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;
  FParser.ParseHeaders('NATS/1.0'#13#10'A: 1'#13#10'B: 2'#13#10#13#10, LHeaders);

  Assert.AreEqual(2, LHeaders.Count, 'NATS/1.0 must not become a header');
  Assert.AreEqual('1', LHeaders.GetHeader('A'));
  Assert.AreEqual('2', LHeaders.GetHeader('B'));
end;

{ TNatsHeadersTests }

procedure TNatsHeadersTests.Add_AppendsHeader;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;
  Assert.AreEqual(0, LHeaders.Count);

  LHeaders.Add('Nats-Msg-Id', 'abc');
  LHeaders.Add('Nats-Expected-Stream', 'ORDERS');

  Assert.AreEqual(2, LHeaders.Count);
end;

procedure TNatsHeadersTests.GetHeader_ReturnsValue;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;
  LHeaders.Add('Nats-Msg-Id', 'abc');

  Assert.AreEqual('abc', LHeaders.GetHeader('Nats-Msg-Id'));
end;

procedure TNatsHeadersTests.GetHeader_UnknownName_ReturnsEmpty;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;
  LHeaders.Add('A', '1');

  Assert.AreEqual('', LHeaders.GetHeader('B'));
end;

procedure TNatsHeadersTests.GetIndex_UnknownName_ReturnsMinusOne;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;
  LHeaders.Add('A', '1');

  Assert.AreEqual(0, LHeaders.GetIndex('A'));
  Assert.AreEqual(-1, LHeaders.GetIndex('B'));
end;

procedure TNatsHeadersTests.SetHeader_ReplacesExistingValue;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;
  LHeaders.Add('A', '1');
  LHeaders.SetHeader('A', '2');

  Assert.AreEqual(1, LHeaders.Count, 'SetHeader must not append a duplicate');
  Assert.AreEqual('2', LHeaders.GetHeader('A'));
end;

procedure TNatsHeadersTests.SetHeader_AddsWhenMissing;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;
  LHeaders.SetHeader('A', '1');

  Assert.AreEqual(1, LHeaders.Count);
  Assert.AreEqual('1', LHeaders.GetHeader('A'));
end;

procedure TNatsHeadersTests.GetHeaderAsInt_ParsesValue;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;
  LHeaders.Add('Nats-Sequence', '4242');

  Assert.AreEqual(UInt64(4242), LHeaders.GetHeaderAsInt('Nats-Sequence', 0));
end;

procedure TNatsHeadersTests.GetHeaderAsInt_MissingName_ReturnsDefault;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;

  Assert.AreEqual(UInt64(7), LHeaders.GetHeaderAsInt('Nope', 7));
end;

procedure TNatsHeadersTests.CopyHeaders_AppendsAllPairs;
var
  LSource, LDest: TNatsHeaders;
begin
  LSource := nil;
  LSource.Add('A', '1');
  LSource.Add('B', '2');

  LDest := nil;
  LDest.CopyHeaders(LSource);

  Assert.AreEqual(2, LDest.Count);
  Assert.AreEqual('1', LDest.GetHeader('A'));
  Assert.AreEqual('2', LDest.GetHeader('B'));
end;

procedure TNatsHeadersTests.Text_UsesColonSeparator;
var
  LHeaders: TNatsHeaders;
begin
  LHeaders := nil;
  LHeaders.Add('Nats-Msg-Id', 'abc');

  Assert.AreEqual('Nats-Msg-Id: abc'#13#10, LHeaders.Text,
    'NATS headers use the HTTP "Key: Value" form');
end;

procedure TNatsHeadersTests.Text_RoundTripsThroughParseHeaders;
var
  LParser: TNatsParser;
  LSource, LParsed: TNatsHeaders;
begin
  LSource := nil;
  LSource.Add('A', '1');
  LSource.Add('B', '2');

  LParser := TNatsParser.Create;
  try
    LParsed := nil;
    // [KNOWN BUG §3 + §4] what the client writes must be readable by the parser
    LParser.ParseHeaders(NatsConstants.CLIENT_HEADER_VERSION + NatsConstants.CR_LF +
      LSource.Text + NatsConstants.CR_LF, LParsed);

    Assert.AreEqual(2, LParsed.Count);
    Assert.AreEqual('1', LParsed.GetHeader('A'));
    Assert.AreEqual('2', LParsed.GetHeader('B'));
  finally
    LParser.Free;
  end;
end;

{ TNatsEntitiesTests }

procedure TNatsEntitiesTests.ServerInfo_FromJSONString_ReadsProtocolFields;
var
  LInfo: TNatsServerInfo;
begin
  LInfo := TNatsServerInfo.FromJSONString(INFO_JSON);

  Assert.AreEqual('NDHJZQZ4YQXQ', LInfo.ServerId);
  Assert.AreEqual('127.0.0.1', LInfo.ClientIp);
  Assert.AreEqual(7, LInfo.ClientId);
  Assert.AreEqual(1048576, LInfo.MaxPayload);
end;

procedure TNatsEntitiesTests.ServerInfo_FromJSONString_IgnoresUnknownFields;
var
  LInfo: TNatsServerInfo;
begin
  // real servers send fields this record does not declare (cluster, connect_urls, ldm, ...)
  LInfo := TNatsServerInfo.FromJSONString(
    '{"server_name":"nats-1","cluster":"c1","connect_urls":["10.0.0.1:4222"],"ldm":false}');

  Assert.AreEqual('nats-1', LInfo.ServerName);
end;

procedure TNatsEntitiesTests.ConnectOptions_ToJSONString_UsesProtocolFieldNames;
var
  LOptions: TNatsConnectOptions;
  LJson: string;
begin
  LOptions := Default(TNatsConnectOptions);
  LOptions.Lang := 'Delphi';
  LOptions.Version := NatsConstants.CLIENT_VERSION;
  LOptions.Protocol := 1;
  LOptions.Echo := True;
  LOptions.User := 'joe';

  LJson := LOptions.ToJSONString;

  // the wire format is lowercase snake_case; Neon derives it from the PascalCase
  // field names through TNeonCase.SnakeCase (see NatsJSONConfig)
  Assert.IsTrue(LJson.Contains('"lang":"Delphi"'), 'missing lang in ' + LJson);
  Assert.IsTrue(LJson.Contains('"protocol":1'), 'missing protocol in ' + LJson);
  Assert.IsTrue(LJson.Contains('"user":"joe"'), 'missing user in ' + LJson);
  Assert.IsTrue(LJson.Contains('"auth_token"'), 'missing auth_token in ' + LJson);
end;

procedure TNatsEntitiesTests.ConnectOptions_RoundTrip;
var
  LSource, LParsed: TNatsConnectOptions;
begin
  LSource := Default(TNatsConnectOptions);
  LSource.Verbose := True;
  LSource.Name := 'client-1';
  LSource.Protocol := 1;

  LParsed := TNatsConnectOptions.FromJSONString(LSource.ToJSONString);

  Assert.IsTrue(LParsed.Verbose, 'verbose must survive the round trip');
  Assert.AreEqual('client-1', LParsed.Name);
  Assert.AreEqual(1, LParsed.Protocol);
end;

procedure TNatsEntitiesTests.ConnectOptions_HeaderSupportFlagSerializes;
var
  LOptions: TNatsConnectOptions;
begin
  // verified against nats-server 2.10.26: a client that does not send
  // "headers":true has its connection closed when it sends HPUB, and receives
  // header-carrying messages as plain MSG with the headers stripped.
  // That the connection turns this on by default is asserted by
  // TNatsConnectionProtocolTests.Connect_DeclaresHeaderSupport.
  LOptions := Default(TNatsConnectOptions);
  LOptions.Headers := True;

  Assert.IsTrue(LOptions.ToJSONString.Contains('"headers":true'),
    'the header support flag must reach the CONNECT payload');
  Assert.IsTrue(TNatsConnectOptions.FromJSONString(LOptions.ToJSONString).Headers,
    'the header support flag must survive a round trip');
end;

{ TNatsNuidTests }

procedure TNatsNuidTests.NextNuid_HasExpectedLength;
begin
  // 12 bytes of prefix + 10 sequential = 22 base62 characters
  Assert.AreEqual(22, Length(TNUID.NextNuid));
end;

procedure TNatsNuidTests.NextNuid_IsUniqueAcrossCalls;
var
  LSeen: TDictionary<string, Boolean>;
  LIndex: Integer;
  LNuid: string;
begin
  // a case sensitive container matters here: the base62 alphabet uses both cases
  LSeen := TDictionary<string, Boolean>.Create;
  try
    for LIndex := 1 to 10000 do
    begin
      LNuid := TNUID.NextNuid;
      Assert.IsFalse(LSeen.ContainsKey(LNuid), 'duplicate NUID produced: ' + LNuid);
      LSeen.Add(LNuid, True);
    end;
  finally
    LSeen.Free;
  end;
end;

procedure TNatsNuidTests.NextNuid_UsesBase62Alphabet;
const
  DIGITS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
var
  LNuid: string;
  LChar: Char;
begin
  LNuid := TNUID.NextNuid;
  for LChar in LNuid do
    Assert.IsTrue(Pos(LChar, DIGITS) > 0,
      Format('NUID "%s" contains a non base62 character "%s"', [LNuid, LChar]));
end;

initialization
  TDUnitX.RegisterTestFixture(TNatsParserTests);
  TDUnitX.RegisterTestFixture(TNatsHeadersTests);
  TDUnitX.RegisterTestFixture(TNatsEntitiesTests);
  TDUnitX.RegisterTestFixture(TNatsNuidTests);

end.
