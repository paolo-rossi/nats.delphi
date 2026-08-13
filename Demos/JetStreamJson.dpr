{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
{                                                                              }
{  Prints every JetStream entity as pretty JSON, so the wire format can be     }
{  eyeballed against the NATS spec.                                            }
{                                                                              }
{  Field values are deliberately few and obvious - the point is to read the    }
{  KEYS and the shape, not the data. What to check:                            }
{                                                                              }
{    - key names are snake_case          (max_msgs_per_subject, not MaxMsgs..) }
{    - enums are strings                 ("workqueue", not 2)                  }
{    - durations are nanoseconds         (60 s -> 60000000000)                 }
{    - unset numbers/strings are absent  (server then applies its own default) }
{                                                                              }
{******************************************************************************}
program JetStreamJson;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Rtti,
  Neon.Core.Types,
  Neon.Core.Persistence,
  Neon.Core.Persistence.JSON,
  Nats.JetStream.Consts in '..\Source\Nats.JetStream.Consts.pas',
  Nats.JetStream.Entities in '..\Source\Nats.JetStream.Entities.pas';

type
  /// A class only because Delphi has no generic standalone routines
  TDump = class
    class procedure Print<T: record>(const ATitle: string; const AValue: T); static;
  end;

/// The library's own config, with Neon's pretty printer switched on
function PrettyConfig: INeonConfiguration;
begin
  Result := JetStreamJSONConfig.SetPrettyPrint(True);
end;

class procedure TDump.Print<T>(const ATitle: string; const AValue: T);
begin
  Writeln('--- ', ATitle, ' ', StringOfChar('-', 60 - Length(ATitle)));
  Writeln(TNeon.ValueToJSONString(TValue.From<T>(AValue), PrettyConfig));
  Writeln;
end;

{ --------------------------------------------------------------------------- }

procedure DumpStreamConfig;
var
  LConfig: TJetStreamStreamConfig;
begin
  { Only a name and a subject. Everything else is left alone on purpose: this
    is what $JS.API.STREAM.CREATE actually receives for a default stream }
  LConfig := Default(TJetStreamStreamConfig);
  LConfig.Name := 'ORDERS';
  LConfig.Subjects := ['orders.*'];

  TDump.Print<TJetStreamStreamConfig>('StreamConfig (minimal)', LConfig);

  { Now with the interesting bits set. Note MaxAge and DuplicateWindow: both
    are nanoseconds, and FromSeconds/FromMinutes is the only sane way to build
    one - writing the zeroes by hand is how a stream ends up with a 60 ms age
    limit instead of 60 s }
  //LConfig.Retention := TJetStreamRetention.WorkQueue;
  LConfig.Storage := TJetStreamStorage.Memory;
  LConfig.Discard := TJetStreamDiscard.New;
  LConfig.MaxMsgs := 100;
  LConfig.MaxAge := TJetStreamDuration.FromSeconds(60);
  LConfig.DuplicateWindow := TJetStreamDuration.FromMinutes(2);
  LConfig.NumReplicas := 3;
  LConfig.DenyDelete := True;

  TDump.Print<TJetStreamStreamConfig>('StreamConfig (populated)', LConfig);
end;

procedure DumpStreamInfo;
var
  LInfo: TJetStreamStreamInfo;
begin
  LInfo := Default(TJetStreamStreamInfo);
  LInfo.Config.Name := 'ORDERS';
  LInfo.Config.Subjects := ['orders.*'];
  LInfo.Created := '2026-08-12T09:00:00Z';
  LInfo.State.Messages := 3;
  LInfo.State.Bytes := 147;
  LInfo.State.FirstSeq := 1;
  LInfo.State.LastSeq := 3;
  LInfo.State.ConsumerCount := 1;
  LInfo.State.FirstTs := '2026-08-12T09:00:01Z';
  LInfo.State.LastTs := '2026-08-12T09:00:03Z';

  TDump.Print<TJetStreamStreamInfo>('StreamInfo', LInfo);
end;

procedure DumpConsumerConfig;
var
  LConfig: TJetStreamConsumerConfig;
begin
  { A pull consumer: DeliverSubject stays empty, and must therefore NOT appear
    in the JSON - sending it empty is what makes the server reject it }
  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := 'workers';
  LConfig.AckPolicy := TJetStreamAckPolicy.Explicit;
  LConfig.AckWait := TJetStreamDuration.FromSeconds(30);
  LConfig.MaxDeliver := 5;
  LConfig.FilterSubject := 'orders.*';

  TDump.Print<TJetStreamConsumerConfig>('ConsumerConfig (pull)', LConfig);

  // a push consumer is the same record plus a delivery subject
  LConfig.DeliverSubject := 'deliver.orders';
  LConfig.DeliverGroup := 'group-1';

  TDump.Print<TJetStreamConsumerConfig>('ConsumerConfig (push)', LConfig);
end;

procedure DumpConsumerInfo;
var
  LInfo: TJetStreamConsumerInfo;
begin
  LInfo := Default(TJetStreamConsumerInfo);
  LInfo.StreamName := 'ORDERS';
  LInfo.Name := 'workers';
  LInfo.Created := '2026-08-12T09:00:00Z';
  LInfo.Config.DurableName := 'workers';
  LInfo.Config.AckPolicy := TJetStreamAckPolicy.Explicit;
  LInfo.Delivered.ConsumerSeq := 2;
  LInfo.Delivered.StreamSeq := 2;
  LInfo.AckFloor.ConsumerSeq := 1;
  LInfo.AckFloor.StreamSeq := 1;
  LInfo.NumAckPending := 1;
  LInfo.NumPending := 1;

  TDump.Print<TJetStreamConsumerInfo>('ConsumerInfo', LInfo);
end;

procedure DumpPubAck;
var
  LAck: TJetStreamPubAck;
begin
  LAck := Default(TJetStreamPubAck);
  LAck.Stream := 'ORDERS';
  LAck.Seq := 42;

  TDump.Print<TJetStreamPubAck>('PubAck', LAck);

  // a duplicate is NOT stored again - seq points at the original
  LAck.Duplicate := True;
  TDump.Print<TJetStreamPubAck>('PubAck (duplicate)', LAck);
end;

procedure DumpApiResponse;
var
  LResponse: TJetStreamApiResponse;
begin
  LResponse := Default(TJetStreamApiResponse);
  LResponse.ResponseType := 'io.nats.jetstream.api.v1.stream_info_response';
  LResponse.Error.Code := 404;
  LResponse.Error.ErrCode := 10059;
  LResponse.Error.Description := 'stream not found';

  // "type" is a Delphi reserved word, so check it still comes out as "type"
  TDump.Print<TJetStreamApiResponse>('ApiResponse (error)', LResponse);
end;

procedure DumpLists;
var
  LStreams: TJetStreamStreamListResponse;
  LConsumers: TJetStreamConsumerListResponse;
begin
  LStreams := Default(TJetStreamStreamListResponse);
  LStreams.Total := 2;
  LStreams.Offset := 0;
  LStreams.Limit := 256;
  SetLength(LStreams.Streams, 2);
  LStreams.Streams[0].Config.Name := 'ORDERS';
  LStreams.Streams[1].Config.Name := 'EVENTS';

  TDump.Print<TJetStreamStreamListResponse>('StreamListResponse', LStreams);

  LConsumers := Default(TJetStreamConsumerListResponse);
  LConsumers.Total := 1;
  LConsumers.Limit := 256;
  SetLength(LConsumers.Consumers, 1);
  LConsumers.Consumers[0].StreamName := 'ORDERS';
  LConsumers.Consumers[0].Name := 'workers';

  TDump.Print<TJetStreamConsumerListResponse>('ConsumerListResponse', LConsumers);
end;

{ --------------------------------------------------------------------------- }

begin
  try
    Writeln('JetStream entities as JSON');
    Writeln('==========================');
    Writeln;

    DumpStreamConfig;
    DumpStreamInfo;
    DumpConsumerConfig;
    DumpConsumerInfo;
    DumpPubAck;
    DumpApiResponse;
    DumpLists;

    ReadLn;
  except
    on E: Exception do
    begin
      Writeln(Format('%s: %s', [E.ClassName, E.Message]));
      ExitCode := 1;
    end;
  end;
end.
