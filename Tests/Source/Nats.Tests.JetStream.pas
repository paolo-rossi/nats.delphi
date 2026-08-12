{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Tests.JetStream;

{******************************************************************************}
{                                                                              }
{  JetStream unit tests. Pure and offline - no connection, no server.           }
{                                                                              }
{******************************************************************************}

interface

uses
  System.SysUtils, System.DateUtils,

  DUnitX.TestFramework,

  Nats.JetStream.Consts,
  Nats.JetStream.Message;

type
  [TestFixture]
  TJetStreamMetadataTests = class
  public
    { V2 - what any current server sends }

    [Test]
    procedure V2Subject_ParsesEveryField;
    [Test]
    procedure V2Subject_WithExtraTokens_StillParses;
    [Test]
    procedure V2Subject_PlaceholderDomain_ReadsAsEmpty;
    [Test]
    procedure V2Subject_RealDomain_IsKept;

    { V1 - the trap. Nine tokens, no domain and no account hash }

    [Test]
    procedure V1Subject_ParsesEveryField;
    [Test]
    procedure V1Subject_HasNoDomainOrAccountHash;
    // reading a V1 subject at V2 positions does not fail, it silently returns
    // the wrong field for every one of them - so the counts must not be confused
    [Test]
    procedure V1AndV2_WithTheSameValues_ParseIdentically;

    { not an ack subject at all }

    [Test]
    procedure EmptySubject_Fails;
    [Test]
    procedure CoreNatsInbox_Fails;
    [Test]
    procedure WrongPrefix_Fails;
    [Test]
    procedure TooFewTokens_Fails;
    [Test]
    procedure TenTokens_Fails;
    [Test]
    procedure NonNumericSequence_Fails;
    [Test]
    procedure FailedParse_LeavesMetadataEmpty;

    { derived values }

    [Test]
    procedure FirstDelivery_IsNotARedelivery;
    [Test]
    procedure SecondDelivery_IsARedelivery;
    [Test]
    procedure Timestamp_ConvertsToUtc;
  end;

implementation

const
  { $JS.ACK.<domain>.<hash>.<stream>.<consumer>.<delivered>.<stream seq>.
    <consumer seq>.<timestamp>.<pending>.<random> }
  V2_SUBJECT = '$JS.ACK.hub.ACCHASH.ORDERS.workers.3.42.7.1700000000123456789.5.rand01';

  { the same delivery as seen from a pre-2.2 server: no domain, no hash, no
    trailing random token }
  V1_SUBJECT = '$JS.ACK.ORDERS.workers.3.42.7.1700000000123456789.5';

  TIMESTAMP_NANOS = UInt64(1700000000123456789);

{ TJetStreamMetadataTests }

procedure TJetStreamMetadataTests.V2Subject_ParsesEveryField;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT, LMeta), 'a V2 ack subject must parse');

  Assert.AreEqual('hub', LMeta.Domain);
  Assert.AreEqual('ACCHASH', LMeta.AccountHash);
  Assert.AreEqual('ORDERS', LMeta.Stream);
  Assert.AreEqual('workers', LMeta.Consumer);
  Assert.AreEqual(UInt64(3), LMeta.NumDelivered);
  Assert.AreEqual(UInt64(42), LMeta.StreamSeq);
  Assert.AreEqual(UInt64(7), LMeta.ConsumerSeq);
  Assert.AreEqual(TIMESTAMP_NANOS, LMeta.TimestampNanos);
  Assert.AreEqual(UInt64(5), LMeta.NumPending);
end;

procedure TJetStreamMetadataTests.V2Subject_WithExtraTokens_StillParses;
var
  LMeta: TJetStreamMsgMetadata;
begin
  { A later server may append tokens. Appending must not break a client, so the
    V2 count is a minimum and not an equality }
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT + '.future.tokens', LMeta),
    'extra trailing tokens must be tolerated, not rejected');
  Assert.AreEqual('ORDERS', LMeta.Stream);
  Assert.AreEqual(UInt64(42), LMeta.StreamSeq);
end;

procedure TJetStreamMetadataTests.V2Subject_PlaceholderDomain_ReadsAsEmpty;
var
  LMeta: TJetStreamMsgMetadata;
begin
  // a server with no domain configured sends '_', not an empty token
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK._.ACCHASH.ORDERS.workers.1.42.7.1700000000123456789.5.rand01', LMeta));

  Assert.AreEqual('', LMeta.Domain,
    'the placeholder means no domain, not a domain named "_"');
end;

procedure TJetStreamMetadataTests.V2Subject_RealDomain_IsKept;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT, LMeta));
  Assert.AreEqual('hub', LMeta.Domain, 'a real domain must survive');
end;

procedure TJetStreamMetadataTests.V1Subject_ParsesEveryField;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V1_SUBJECT, LMeta), 'a V1 ack subject must parse');

  Assert.AreEqual('ORDERS', LMeta.Stream);
  Assert.AreEqual('workers', LMeta.Consumer);
  Assert.AreEqual(UInt64(3), LMeta.NumDelivered);
  Assert.AreEqual(UInt64(42), LMeta.StreamSeq);
  Assert.AreEqual(UInt64(7), LMeta.ConsumerSeq);
  Assert.AreEqual(TIMESTAMP_NANOS, LMeta.TimestampNanos);
  Assert.AreEqual(UInt64(5), LMeta.NumPending);
end;

procedure TJetStreamMetadataTests.V1Subject_HasNoDomainOrAccountHash;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V1_SUBJECT, LMeta));

  Assert.AreEqual('', LMeta.Domain, 'V1 has no domain token to read');
  Assert.AreEqual('', LMeta.AccountHash, 'nor an account hash');
end;

procedure TJetStreamMetadataTests.V1AndV2_WithTheSameValues_ParseIdentically;
var
  LV1, LV2: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V1_SUBJECT, LV1));
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT, LV2));

  { The two subjects describe the same delivery. Indexing by position without
    checking the token count first would shift every V1 field by two and make
    these disagree - while still looking like perfectly plausible numbers }
  Assert.AreEqual(LV2.Stream, LV1.Stream);
  Assert.AreEqual(LV2.Consumer, LV1.Consumer);
  Assert.AreEqual(LV2.NumDelivered, LV1.NumDelivered);
  Assert.AreEqual(LV2.StreamSeq, LV1.StreamSeq);
  Assert.AreEqual(LV2.ConsumerSeq, LV1.ConsumerSeq);
  Assert.AreEqual(LV2.TimestampNanos, LV1.TimestampNanos);
  Assert.AreEqual(LV2.NumPending, LV1.NumPending);
end;

procedure TJetStreamMetadataTests.EmptySubject_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse('', LMeta));
end;

procedure TJetStreamMetadataTests.CoreNatsInbox_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  { An ordinary request reply-to. Not JetStream, and not an error either - every
    core NATS request produces one of these }
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse('_INBOX.aBcDeFgHiJkLmNoPqRsTuV', LMeta));
end;

procedure TJetStreamMetadataTests.WrongPrefix_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  // right shape, wrong verb: $JS.API is a request, not an acknowledgement
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse(
    '$JS.API.hub.ACCHASH.ORDERS.workers.3.42.7.1700000000123456789.5.rand01', LMeta),
    'the first two tokens must be checked, not assumed');
end;

procedure TJetStreamMetadataTests.TooFewTokens_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse('$JS.ACK.ORDERS.workers', LMeta));
end;

procedure TJetStreamMetadataTests.TenTokens_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  { Ten and eleven tokens are neither layout. Accepting them would mean guessing
    which fields are missing }
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK.ORDERS.workers.3.42.7.1700000000123456789.5.extra', LMeta),
    'a count between the two layouts is not a subject we can read');
end;

procedure TJetStreamMetadataTests.NonNumericSequence_Fails;
var
  LMeta: TJetStreamMsgMetadata;
begin
  { Reporting sequence 0 here would be worse than failing: a consumer would go
    on to acknowledge the wrong message }
  Assert.IsFalse(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK.ORDERS.workers.3.NOTANUMBER.7.1700000000123456789.5', LMeta));
end;

procedure TJetStreamMetadataTests.FailedParse_LeavesMetadataEmpty;
var
  LMeta: TJetStreamMsgMetadata;
begin
  // seeded, so a half-filled record would be visible
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT, LMeta));
  Assert.AreEqual('ORDERS', LMeta.Stream, 'guard: the seed must have taken');

  Assert.IsFalse(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK.ORDERS.workers.3.NOTANUMBER.7.1700000000123456789.5', LMeta));

  Assert.AreEqual('', LMeta.Stream, 'a failed parse must not leave the old value behind');
  Assert.AreEqual(UInt64(0), LMeta.StreamSeq);
end;

procedure TJetStreamMetadataTests.FirstDelivery_IsNotARedelivery;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK.ORDERS.workers.1.42.7.1700000000123456789.5', LMeta));

  Assert.AreEqual(UInt64(1), LMeta.NumDelivered, 'delivery counting starts at 1, not 0');
  Assert.IsFalse(LMeta.IsRedelivery);
end;

procedure TJetStreamMetadataTests.SecondDelivery_IsARedelivery;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(
    '$JS.ACK.ORDERS.workers.2.42.7.1700000000123456789.5', LMeta));

  Assert.IsTrue(LMeta.IsRedelivery, 'a second delivery means the first was never acked');
end;

procedure TJetStreamMetadataTests.Timestamp_ConvertsToUtc;
var
  LMeta: TJetStreamMsgMetadata;
begin
  Assert.IsTrue(TJetStreamMsgMetadata.TryParse(V2_SUBJECT, LMeta));

  // 1700000000 seconds after the epoch is 2023-11-14 22:13:20 UTC
  Assert.AreEqual(EncodeDateTime(2023, 11, 14, 22, 13, 20, 0), LMeta.TimestampUTC, 0.0001,
    'the nanosecond timestamp must convert to the right instant');
end;

initialization
  TDUnitX.RegisterTestFixture(TJetStreamMetadataTests);

end.
