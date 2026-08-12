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
  Nats.JetStream.Consts;

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

end.
