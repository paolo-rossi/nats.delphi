{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.JetStream.Message;

interface

uses
  System.SysUtils, System.DateUtils,

  Nats.Consts,
  Nats.Classes,
  Nats.Connection,
  Nats.Exceptions,
  Nats.JetStream.Consts,
  Nats.JetStream.Entities;

type
  /// <summary>
  ///   Everything a JetStream consumer knows about a delivered message, taken
  ///   from its reply-to subject
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     JetStream puts the metadata in the SUBJECT rather than in headers:
  ///   </para>
  ///   <para>
  ///     $JS.ACK.&lt;domain&gt;.&lt;hash&gt;.&lt;stream&gt;.&lt;consumer&gt;.
  ///     &lt;delivered&gt;.&lt;stream seq&gt;.&lt;consumer seq&gt;.
  ///     &lt;timestamp&gt;.&lt;pending&gt;.&lt;random&gt;
  ///   </para>
  ///   <para>
  ///     That same subject is where an acknowledgement gets published, so it is
  ///     both the metadata and the address to answer on.
  ///   </para>
  /// </remarks>
  TJetStreamMsgMetadata = record
  public
    /// <summary>
    ///   The JetStream domain, empty when the server has none configured. A V1
    ///   subject has no domain token at all, which also reads as empty
    /// </summary>
    Domain: string;
    /// Opaque account identifier; empty on a V1 subject
    AccountHash: string;
    Stream: string;
    Consumer: string;
    /// <summary>
    ///   How many times this message has been delivered, counting this one - so
    ///   it starts at 1, never 0. Compare against the consumer's MaxDeliver
    /// </summary>
    NumDelivered: UInt64;
    /// Position in the stream: stable, and what a replay seeks to
    StreamSeq: UInt64;
    /// Position in this consumer's own delivery sequence
    ConsumerSeq: UInt64;
    /// <summary>
    ///   Nanoseconds since the Unix epoch, exactly as the server sent it. This
    ///   is the authoritative value - TimestampUTC is a lossy convenience
    /// </summary>
    TimestampNanos: UInt64;
    /// How many messages are still waiting for this consumer
    NumPending: UInt64;

    /// <summary>
    ///   True when the server has delivered this message before, i.e. a
    ///   previous delivery was never acknowledged
    /// </summary>
    function IsRedelivery: Boolean;

    /// <summary>
    ///   TimestampNanos as a UTC TDateTime. Lossy: TDateTime is a count of days
    ///   in a Double and cannot hold nanoseconds
    /// </summary>
    function TimestampUTC: TDateTime;

    /// <summary>
    ///   Parses a delivered message's reply-to subject.
    /// </summary>
    /// <remarks>
    ///   False simply means ASubject is not a $JS.ACK subject - an ordinary
    ///   core NATS reply-to, an empty one, or a malformed one. That is a normal
    ///   thing to encounter rather than an error, so it is reported and not
    ///   raised. AMetadata is left empty whenever the result is False, never
    ///   half filled.
    /// </remarks>
    class function TryParse(const ASubject: string;
      out AMetadata: TJetStreamMsgMetadata): Boolean; static;
  end;

  /// <summary>
  ///   Acknowledging was attempted on a message that cannot be acknowledged, or
  ///   that already has been
  /// </summary>
  EJetStreamAckError = class(ENatsException);

  /// <summary>
  ///   A message delivered by a JetStream consumer: the message itself, its
  ///   metadata, and the ability to answer for itself
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     This exists because a core handler receives a bare TNatsArgsMSG with
  ///     no connection alongside it, so Ack could never be a method on the
  ///     record - and putting one there would drag JetStream into
  ///     Nats.Classes.pas and break the rule that core knows nothing about it.
  ///   </para>
  ///   <para>
  ///     An interface, so the caller never has to free anything. Fetch hands
  ///     back a whole batch of these.
  ///   </para>
  /// </remarks>
  IJetStreamMsg = interface
    ['{B9E2A1D4-6C37-4F58-9A0B-2E5D7C81F643}']
    function GetSubject: string;
    function GetPayload: string;
    function GetPayloadData: TBytes;
    function GetHeaders: TNatsHeaders;
    function GetMetadata: TJetStreamMsgMetadata;
    function GetAckSubject: string;
    function GetAcknowledged: Boolean;

    /// <summary>
    ///   Done with it. Fire and forget: this returns as soon as the ack is on
    ///   the wire, not when the server has recorded it - use AckSync when that
    ///   difference matters
    /// </summary>
    procedure Ack;
    /// <summary>
    ///   Ack and wait for the server to confirm. BLOCKS, so it must never be
    ///   called from a message handler - see the remarks on TJetStreamContext
    /// </summary>
    procedure AckSync(ATimeoutMs: Cardinal = NatsConstants.DEFAULT_REQUEST_TIMEOUT);
    /// Could not handle it: redeliver, and do not wait out AckWait first
    procedure Nak; overload;
    /// As Nak, but not before ADelay has passed
    procedure Nak(ADelay: TJetStreamDuration); overload;
    /// <summary>
    ///   Still working. Resets AckWait without acknowledging, and unlike the
    ///   others may be called as often as needed
    /// </summary>
    procedure InProgress;
    /// Never redeliver this one, whatever MaxDeliver says
    procedure Term;

    property Subject: string read GetSubject;
    /// The payload decoded as UTF-8; use PayloadData for anything binary
    property Payload: string read GetPayload;
    property PayloadData: TBytes read GetPayloadData;
    property Headers: TNatsHeaders read GetHeaders;
    /// Stream, consumer, sequences and delivery count, from the ack subject
    property Metadata: TJetStreamMsgMetadata read GetMetadata;
    /// Where an ack goes - the delivered message's reply-to
    property AckSubject: string read GetAckSubject;
    /// <summary>
    ///   True once Ack, Nak or Term has been sent. InProgress does not set it,
    ///   because it settles nothing
    /// </summary>
    property Acknowledged: Boolean read GetAcknowledged;
  end;

  /// <summary>
  ///   Runs on the consumer thread for a push subscription, and on the caller's
  ///   own thread for a Fetch. See TNatsMsgHandler for what that implies
  /// </summary>
  TJetStreamMsgHandler = reference to procedure(const AMsg: IJetStreamMsg);

  TJetStreamMsg = class(TInterfacedObject, IJetStreamMsg)
  private
    FConnection: TNatsConnection;
    FData: TNatsArgsMSG;
    FMetadata: TJetStreamMsgMetadata;
    FAcknowledged: Boolean;

    function GetSubject: string;
    function GetPayload: string;
    function GetPayloadData: TBytes;
    function GetHeaders: TNatsHeaders;
    function GetMetadata: TJetStreamMsgMetadata;
    function GetAckSubject: string;
    function GetAcknowledged: Boolean;

    /// <summary>
    ///   The one place an ack is written. ASettles marks the message answered
    ///   for, which is every ack except a progress report
    /// </summary>
    procedure SendAck(const APayload: string; ASettles: Boolean);
    /// Raises unless this message can still be acknowledged
    procedure CheckAckable;
  public
    /// <summary>
    ///   Wraps a delivered message. Raises unless its reply-to really is a
    ///   $JS.ACK subject - use TryWrap where that is not already known
    /// </summary>
    constructor Create(AConnection: TNatsConnection; const AData: TNatsArgsMSG);

    /// <summary>
    ///   False means this is not a JetStream delivery: an ordinary core NATS
    ///   message, or a status message, both of which are normal things to meet
    ///   on a subscription and neither of which can be acked
    /// </summary>
    class function TryWrap(AConnection: TNatsConnection; const AData: TNatsArgsMSG;
      out AMsg: IJetStreamMsg): Boolean; static;

    procedure Ack;
    procedure AckSync(ATimeoutMs: Cardinal = NatsConstants.DEFAULT_REQUEST_TIMEOUT);
    procedure Nak; overload;
    procedure Nak(ADelay: TJetStreamDuration); overload;
    procedure InProgress;
    procedure Term;
  end;

implementation

const
  NANOS_PER_SECOND = UInt64(1000000000);

{ TJetStreamMsgMetadata }

function TJetStreamMsgMetadata.IsRedelivery: Boolean;
begin
  { The first delivery is 1, so anything above it has been tried before. Zero
    means "never parsed", which is not a redelivery either }
  Result := NumDelivered > 1;
end;

function TJetStreamMsgMetadata.TimestampUTC: TDateTime;
begin
  Result := UnixToDateTime(Int64(TimestampNanos div NANOS_PER_SECOND), True) +
    (TimestampNanos mod NANOS_PER_SECOND) / NANOS_PER_SECOND / SecsPerDay;
end;

class function TJetStreamMsgMetadata.TryParse(const ASubject: string;
  out AMetadata: TJetStreamMsgMetadata): Boolean;
var
  LTokens: TArray<string>;
  LMeta: TJetStreamMsgMetadata;
begin
  { Filled in only once every token has parsed, so a caller that ignores the
    result never sees a half-built record }
  AMetadata := Default(TJetStreamMsgMetadata);
  LMeta := Default(TJetStreamMsgMetadata);
  Result := False;

  if ASubject.IsEmpty then
    Exit;

  LTokens := ASubject.Split([NatsConstants.SEP]);

  { Count FIRST, then index. V1 has no domain and no account hash, so reading it
    at V2 positions does not fail - it silently returns the wrong field for
    every single one, and they all look like plausible values. V2 is a minimum
    rather than an equality because a later server may append tokens }
  if (Length(LTokens) <> JetStreamConstants.Ack.V1_TOKEN_COUNT) and
     (Length(LTokens) < JetStreamConstants.Ack.V2_TOKEN_COUNT) then
    Exit;

  if (LTokens[JetStreamConstants.Ack.POS_JS] <> JetStreamConstants.Ack.TOKEN_JS) or
     (LTokens[JetStreamConstants.Ack.POS_ACK] <> JetStreamConstants.Ack.TOKEN_ACK) then
    Exit;

  { Normalise V1 onto the V2 layout by inserting the two tokens it lacks, so
    everything below indexes exactly one way }
  if Length(LTokens) = JetStreamConstants.Ack.V1_TOKEN_COUNT then
    Insert([String.Empty, String.Empty], LTokens, JetStreamConstants.Ack.POS_DOMAIN);

  LMeta.Domain := LTokens[JetStreamConstants.Ack.POS_DOMAIN];
  if LMeta.Domain = JetStreamConstants.Ack.NO_DOMAIN then
    LMeta.Domain := String.Empty;   // the placeholder means "no domain", not a domain called '_'

  LMeta.AccountHash := LTokens[JetStreamConstants.Ack.POS_ACCOUNT_HASH];
  LMeta.Stream := LTokens[JetStreamConstants.Ack.POS_STREAM];
  LMeta.Consumer := LTokens[JetStreamConstants.Ack.POS_CONSUMER];

  if LMeta.Stream.IsEmpty or LMeta.Consumer.IsEmpty then
    Exit;   // an ack subject always names both

  { Strict about the numbers: a token that is not a number means this is not the
    subject we think it is, and reporting sequence 0 would be worse than saying
    so - a consumer would ack the wrong message }
  if not TryStrToUInt64(LTokens[JetStreamConstants.Ack.POS_NUM_DELIVERED], LMeta.NumDelivered) then
    Exit;
  if not TryStrToUInt64(LTokens[JetStreamConstants.Ack.POS_STREAM_SEQ], LMeta.StreamSeq) then
    Exit;
  if not TryStrToUInt64(LTokens[JetStreamConstants.Ack.POS_CONSUMER_SEQ], LMeta.ConsumerSeq) then
    Exit;
  if not TryStrToUInt64(LTokens[JetStreamConstants.Ack.POS_TIMESTAMP], LMeta.TimestampNanos) then
    Exit;
  if not TryStrToUInt64(LTokens[JetStreamConstants.Ack.POS_NUM_PENDING], LMeta.NumPending) then
    Exit;

  AMetadata := LMeta;
  Result := True;
end;

{ TJetStreamMsg }

constructor TJetStreamMsg.Create(AConnection: TNatsConnection; const AData: TNatsArgsMSG);
begin
  inherited Create;

  if not Assigned(AConnection) then
    raise EJetStreamAckError.Create('A JetStream message needs a connection to ack over');

  { The metadata and the ack address are the same string, so a reply-to that
    does not parse means there is nothing to answer on either }
  if not TJetStreamMsgMetadata.TryParse(AData.ReplyTo, FMetadata) then
    raise EJetStreamAckError.CreateFmt(
      'Not a JetStream delivery: the reply-to [%s] is not a %s subject',
      [AData.ReplyTo, JetStreamConstants.Ack.PREFIX]);

  FConnection := AConnection;
  FData := AData;
end;

class function TJetStreamMsg.TryWrap(AConnection: TNatsConnection;
  const AData: TNatsArgsMSG; out AMsg: IJetStreamMsg): Boolean;
var
  LMeta: TJetStreamMsgMetadata;
begin
  AMsg := nil;

  { A status message is control flow and carries no ack subject, so it can
    never be wrapped - and letting one through would hand the application a
    phantom empty message it would then try to ack }
  Result := not AData.HasStatus and
    TJetStreamMsgMetadata.TryParse(AData.ReplyTo, LMeta);

  if Result then
    AMsg := TJetStreamMsg.Create(AConnection, AData);
end;

function TJetStreamMsg.GetSubject: string;
begin
  Result := FData.Subject;
end;

function TJetStreamMsg.GetPayload: string;
begin
  Result := FData.Payload;
end;

function TJetStreamMsg.GetPayloadData: TBytes;
begin
  Result := FData.PayloadData;
end;

function TJetStreamMsg.GetHeaders: TNatsHeaders;
begin
  Result := FData.Headers;
end;

function TJetStreamMsg.GetMetadata: TJetStreamMsgMetadata;
begin
  Result := FMetadata;
end;

function TJetStreamMsg.GetAckSubject: string;
begin
  Result := FData.ReplyTo;
end;

function TJetStreamMsg.GetAcknowledged: Boolean;
begin
  Result := FAcknowledged;
end;

procedure TJetStreamMsg.CheckAckable;
begin
  { Acking twice is a bug worth reporting rather than swallowing. The second
    ack lands on a subject the server has already retired, so it does nothing -
    and the code that sent it goes on believing it settled something }
  if FAcknowledged then
    raise EJetStreamAckError.CreateFmt(
      'Message %d of stream [%s] has already been acknowledged',
      [FMetadata.StreamSeq, FMetadata.Stream]);
end;

procedure TJetStreamMsg.SendAck(const APayload: string; ASettles: Boolean);
begin
  CheckAckable;

  { The flag goes up BEFORE the publish. If the publish raises, the message is
    left settled on purpose: the connection is the thing that failed, and
    retrying the ack on a dead connection cannot help. The server redelivers
    after AckWait, which is exactly the right outcome }
  if ASettles then
    FAcknowledged := True;

  FConnection.Publish(FData.ReplyTo, APayload);
end;

procedure TJetStreamMsg.Ack;
begin
  SendAck(JetStreamConstants.Ack.PAYLOAD_ACK, True);
end;

procedure TJetStreamMsg.AckSync(ATimeoutMs: Cardinal);
var
  LReply: TNatsArgsMSG;
begin
  CheckAckable;
  FAcknowledged := True;

  { The server answers an ack sent as a request, which is the only way to know
    it was recorded rather than merely written to a socket }
  if not FConnection.RequestSync(FData.ReplyTo,
       JetStreamConstants.Ack.PAYLOAD_ACK, LReply, ATimeoutMs) then
    raise EJetStreamAckError.CreateFmt(
      'The server did not confirm the ack for message %d of stream [%s] within %d ms',
      [FMetadata.StreamSeq, FMetadata.Stream, ATimeoutMs]);
end;

procedure TJetStreamMsg.Nak;
begin
  SendAck(JetStreamConstants.Ack.PAYLOAD_NAK, True);
end;

procedure TJetStreamMsg.Nak(ADelay: TJetStreamDuration);
begin
  { Nanoseconds, like every other duration here. Milliseconds would be accepted
    and would ask for a redelivery a million times sooner than intended }
  SendAck(Format(JetStreamConstants.Ack.PAYLOAD_NAK_DELAY, [Int64(ADelay)]), True);
end;

procedure TJetStreamMsg.InProgress;
begin
  { Deliberately does NOT settle: this says "not yet", so it may be sent as
    often as the work takes }
  SendAck(JetStreamConstants.Ack.PAYLOAD_PROGRESS, False);
end;

procedure TJetStreamMsg.Term;
begin
  SendAck(JetStreamConstants.Ack.PAYLOAD_TERM, True);
end;

end.
