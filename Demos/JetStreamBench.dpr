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
{  Serialization benchmark for the JetStream entity layer.                     }
{                                                                              }
{  Every JetStream API call is one record serialized out and one deserialized  }
{  back, so this is the per-call overhead the client adds on top of the round  }
{  trip. Run it with an iteration count:  JetStreamBench.exe [iterations]      }
{                                                                              }
{******************************************************************************}
program JetStreamBench;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Diagnostics,
  System.Rtti,
  Neon.Core.Types,
  Neon.Core.Persistence,
  Neon.Core.Persistence.JSON,
  Nats.JetStream.Consts in '..\Source\Nats.JetStream.Consts.pas',
  Nats.JetStream.Entities in '..\Source\Nats.JetStream.Entities.pas',
  Nats.JetStream.Message in '..\Source\Nats.JetStream.Message.pas';

type
  /// One measured row of the report
  TBenchResult = record
    Name: string;
    Iterations: Integer;
    Bytes: Integer;
    ElapsedMs: Double;
    function MicrosPerOp: Double;
    function OpsPerSecond: Double;
  end;

  TBenchResults = TArray<TBenchResult>;

var
  GIterations: Integer = 200;
  GSerialize: TBenchResults;
  GDeserialize: TBenchResults;
  /// <summary>
  ///   Somewhere for a deserialized value to go. Read outside the timed region,
  ///   so it costs nothing, but it keeps the result from being an assignment
  ///   the compiler can call unused - and a discarded result is exactly the
  ///   kind of thing an optimiser is entitled to delete
  /// </summary>
  GSink: Integer = 0;

const
  /// <summary>
  ///   Every measurement is repeated and the FASTEST kept. The minimum is the
  ///   run least contaminated by the scheduler, and an average would let one
  ///   descheduled run dominate. Without this the config comparison below
  ///   swings by more than the difference it is trying to measure
  /// </summary>
  REPEATS = 3;

  /// Below this relative difference, two timings are not telling us anything
  NOISE_FLOOR = 0.05;

{ TBenchResult }

function TBenchResult.MicrosPerOp: Double;
begin
  if Iterations = 0 then
    Exit(0);
  Result := (ElapsedMs * 1000) / Iterations;
end;

function TBenchResult.OpsPerSecond: Double;
begin
  if ElapsedMs <= 0 then
    Exit(0);
  Result := Iterations / (ElapsedMs / 1000);
end;

{ ---------------------------------------------------------------------------
  Sample data. Deliberately realistic rather than minimal: a config with every
  field set costs more to serialize than an empty one, and the empty one is not
  what a real call looks like.
  --------------------------------------------------------------------------- }

function SampleStreamConfig: TJetStreamStreamConfig;
begin
  Result := Default(TJetStreamStreamConfig);
  Result.Name := 'ORDERS';
  Result.Subjects := ['orders.*', 'orders.priority.>', 'orders.audit.*'];
  Result.Description := 'order intake stream';
  Result.Retention := TJetStreamRetention.WorkQueue;
  Result.Storage := TJetStreamStorage.Filestore;
  Result.Discard := TJetStreamDiscard.Old;
  Result.MaxConsumers := -1;
  Result.MaxMsgs := 1000000;
  Result.MaxBytes := 1073741824;
  Result.MaxMsgsPerSubject := 1000;
  Result.MaxMsgSize := 1048576;
  Result.MaxAge := TJetStreamDuration.FromMinutes(60);
  Result.DuplicateWindow := TJetStreamDuration.FromMinutes(2);
  Result.NumReplicas := 3;
  Result.DenyDelete := True;
  Result.AllowRollupHdrs := True;
end;

function SampleConsumerConfig: TJetStreamConsumerConfig;
begin
  Result := Default(TJetStreamConsumerConfig);
  Result.DurableName := 'workers';
  Result.Name := 'workers';
  Result.Description := 'order processing workers';
  Result.DeliverPolicy := TJetStreamDeliverPolicy.All;
  Result.AckPolicy := TJetStreamAckPolicy.Explicit;
  Result.AckWait := TJetStreamDuration.FromSeconds(30);
  Result.MaxDeliver := 5;
  Result.FilterSubject := 'orders.*';
  Result.ReplayPolicy := TJetStreamReplayPolicy.Instant;
  Result.MaxWaiting := 512;
  Result.MaxAckPending := 1000;
  Result.MaxBatch := 100;
  Result.MaxExpires := TJetStreamDuration.FromSeconds(30);
  Result.InactiveThreshold := TJetStreamDuration.FromMinutes(5);
  Result.NumReplicas := 3;
end;

function SamplePubAck: TJetStreamPubAck;
begin
  Result := Default(TJetStreamPubAck);
  Result.Stream := 'ORDERS';
  Result.Seq := 918273645;
  Result.Domain := 'hub';
end;

function SampleStreamInfo: TJetStreamStreamInfo;
begin
  Result := Default(TJetStreamStreamInfo);
  Result.Config := SampleStreamConfig;
  Result.Created := '2026-08-12T09:00:00Z';
  Result.State.Messages := 918273;
  Result.State.Bytes := 148392011;
  Result.State.FirstSeq := 1;
  Result.State.LastSeq := 918273;
  Result.State.ConsumerCount := 4;
  Result.State.NumSubjects := 3;
  Result.State.FirstTs := '2026-08-12T09:00:01Z';
  Result.State.LastTs := '2026-08-12T09:45:12Z';
end;

function SampleConsumerInfo: TJetStreamConsumerInfo;
begin
  Result := Default(TJetStreamConsumerInfo);
  Result.StreamName := 'ORDERS';
  Result.Name := 'workers';
  Result.Created := '2026-08-12T09:00:00Z';
  Result.Config := SampleConsumerConfig;
  Result.Delivered.ConsumerSeq := 918200;
  Result.Delivered.StreamSeq := 918200;
  Result.AckFloor.ConsumerSeq := 918150;
  Result.AckFloor.StreamSeq := 918150;
  Result.NumAckPending := 50;
  Result.NumPending := 73;
end;

function SampleStreamList: TJetStreamStreamListResponse;
var
  LIndex: Integer;
begin
  Result := Default(TJetStreamStreamListResponse);
  Result.Total := 10;
  Result.Limit := 256;
  SetLength(Result.Streams, 10);
  for LIndex := 0 to 9 do
  begin
    Result.Streams[LIndex] := SampleStreamInfo;
    Result.Streams[LIndex].Config.Name := Format('ORDERS_%d', [LIndex]);
  end;
end;

{ ---------------------------------------------------------------------------
  Measurement
  --------------------------------------------------------------------------- }

procedure Add(var AResults: TBenchResults; const AResult: TBenchResult);
begin
  AResults := AResults + [AResult];
end;

type
  /// <summary>
  ///   A class only because Delphi has no generic standalone routines - the
  ///   same reason TJetStreamJSON is one
  /// </summary>
  TBench = class
  public
    /// <summary>
    ///   Serializes AValue GIterations times through the PUBLIC API, exactly as
    ///   a caller would - including whatever that API does per call
    /// </summary>
    class function Serialize<T: record>(const AName: string; const AValue: T): TBenchResult; static;
    class function Deserialize<T: record>(const AName: string; const AValue: T): TBenchResult; static;
  end;

class function TBench.Serialize<T>(const AName: string; const AValue: T): TBenchResult;
var
  LWatch: TStopwatch;
  LJson: string;
  LIndex, LPass: Integer;
begin
  LJson := TJetStreamJSON.ToJSON<T>(AValue);   // warm up, and size the payload

  Result.ElapsedMs := -1;
  for LPass := 1 to REPEATS do
  begin
    LWatch := TStopwatch.StartNew;
    for LIndex := 1 to GIterations do
      LJson := TJetStreamJSON.ToJSON<T>(AValue);
    LWatch.Stop;

    if (Result.ElapsedMs < 0) or (LWatch.Elapsed.TotalMilliseconds < Result.ElapsedMs) then
      Result.ElapsedMs := LWatch.Elapsed.TotalMilliseconds;
  end;

  Result.Name := AName;
  Result.Iterations := GIterations;
  Result.Bytes := Length(TEncoding.UTF8.GetBytes(LJson));
end;

class function TBench.Deserialize<T>(const AName: string; const AValue: T): TBenchResult;
var
  LWatch: TStopwatch;
  LJson: string;
  LResult: T;
  LIndex, LPass: Integer;
begin
  LJson := TJetStreamJSON.ToJSON<T>(AValue);
  LResult := TJetStreamJSON.FromJSON<T>(LJson);   // warm up
  GSink := GSink + Ord(not TValue.From<T>(LResult).IsEmpty);

  Result.ElapsedMs := -1;
  for LPass := 1 to REPEATS do
  begin
    LWatch := TStopwatch.StartNew;
    for LIndex := 1 to GIterations do
      LResult := TJetStreamJSON.FromJSON<T>(LJson);
    LWatch.Stop;

    if (Result.ElapsedMs < 0) or (LWatch.Elapsed.TotalMilliseconds < Result.ElapsedMs) then
      Result.ElapsedMs := LWatch.Elapsed.TotalMilliseconds;
  end;

  // after the clock stops: costs nothing, and the result is no longer unused
  GSink := GSink + Ord(not TValue.From<T>(LResult).IsEmpty);

  Result.Name := AName;
  Result.Iterations := GIterations;
  Result.Bytes := Length(TEncoding.UTF8.GetBytes(LJson));
end;

/// <summary>
///   The same work with the Neon configuration built ONCE and reused, to show
///   what share of the cost above is rebuilding it on every call
/// </summary>
function BenchSerializeSharedConfig(const AName: string): TBenchResult;
var
  LWatch: TStopwatch;
  LConfig: INeonConfiguration;
  LValue: TValue;
  LJson: string;
  LIndex, LPass: Integer;
begin
  LConfig := JetStreamJSONConfig;
  LValue := TValue.From<TJetStreamStreamConfig>(SampleStreamConfig);
  LJson := TNeon.ValueToJSONString(LValue, LConfig);

  Result.ElapsedMs := -1;
  for LPass := 1 to REPEATS do
  begin
    LWatch := TStopwatch.StartNew;
    for LIndex := 1 to GIterations do
      LJson := TNeon.ValueToJSONString(LValue, LConfig);
    LWatch.Stop;

    if (Result.ElapsedMs < 0) or (LWatch.Elapsed.TotalMilliseconds < Result.ElapsedMs) then
      Result.ElapsedMs := LWatch.Elapsed.TotalMilliseconds;
  end;

  Result.Name := AName;
  Result.Iterations := GIterations;
  Result.Bytes := Length(TEncoding.UTF8.GetBytes(LJson));
end;

{ ---------------------------------------------------------------------------
  Correctness gate. A benchmark of code that does not work measures nothing, so
  every entity round trips before anything is timed.
  --------------------------------------------------------------------------- }

var
  GChecks: Integer = 0;
  GFailures: Integer = 0;

procedure Check(const AName: string; ACondition: Boolean);
begin
  Inc(GChecks);
  if ACondition then
    Exit;

  Inc(GFailures);
  Writeln(Format('  FAILED: %s', [AName]));
end;

procedure VerifyRoundTrips;
var
  LStreamCfg: TJetStreamStreamConfig;
  LConsumerCfg: TJetStreamConsumerConfig;
  LInfo: TJetStreamStreamInfo;
  LAck: TJetStreamPubAck;
  LList: TJetStreamStreamListResponse;
begin
  Writeln('Verifying round trips');

  LStreamCfg := TJetStreamJSON.FromJSON<TJetStreamStreamConfig>(
    TJetStreamJSON.ToJSON<TJetStreamStreamConfig>(SampleStreamConfig));
  Check('StreamConfig.Name', LStreamCfg.Name = 'ORDERS');
  Check('StreamConfig.Subjects', Length(LStreamCfg.Subjects) = 3);
  Check('StreamConfig.Retention', LStreamCfg.Retention = TJetStreamRetention.WorkQueue);
  Check('StreamConfig.MaxAge', Int64(LStreamCfg.MaxAge) = Int64(TJetStreamDuration.FromMinutes(60)));

  LConsumerCfg := TJetStreamJSON.FromJSON<TJetStreamConsumerConfig>(
    TJetStreamJSON.ToJSON<TJetStreamConsumerConfig>(SampleConsumerConfig));
  Check('ConsumerConfig.DurableName', LConsumerCfg.DurableName = 'workers');
  Check('ConsumerConfig.AckPolicy', LConsumerCfg.AckPolicy = TJetStreamAckPolicy.Explicit);
  Check('ConsumerConfig.AckWait', Int64(LConsumerCfg.AckWait) = Int64(TJetStreamDuration.FromSeconds(30)));

  LInfo := TJetStreamJSON.FromJSON<TJetStreamStreamInfo>(
    TJetStreamJSON.ToJSON<TJetStreamStreamInfo>(SampleStreamInfo));
  Check('StreamInfo.State.Messages', LInfo.State.Messages = 918273);
  Check('StreamInfo.Config.Name', LInfo.Config.Name = 'ORDERS');

  LAck := TJetStreamJSON.FromJSON<TJetStreamPubAck>(
    TJetStreamJSON.ToJSON<TJetStreamPubAck>(SamplePubAck));
  Check('PubAck.Seq', LAck.Seq = 918273645);

  LList := TJetStreamJSON.FromJSON<TJetStreamStreamListResponse>(
    TJetStreamJSON.ToJSON<TJetStreamStreamListResponse>(SampleStreamList));
  Check('StreamList.Streams', Length(LList.Streams) = 10);
  Check('StreamList.Streams[9]', LList.Streams[9].Config.Name = 'ORDERS_9');

  Writeln(Format('  %d checks, %d failed', [GChecks, GFailures]));
  Writeln;
end;

{ ---------------------------------------------------------------------------
  Report
  --------------------------------------------------------------------------- }

procedure WriteTable(const ATitle: string; const AResults: TBenchResults);
var
  LResult: TBenchResult;
begin
  Writeln(ATitle);
  Writeln('  entity                        bytes      total ms     us/op       ops/sec');
  Writeln('  ---------------------------------------------------------------------------');
  for LResult in AResults do
    Writeln(Format('  %-26s %7d %13.2f %9.2f %13.0f',
      [LResult.Name, LResult.Bytes, LResult.ElapsedMs, LResult.MicrosPerOp,
       LResult.OpsPerSecond]));
  Writeln;
end;

function TotalMs(const AResults: TBenchResults): Double;
var
  LResult: TBenchResult;
begin
  Result := 0;
  for LResult in AResults do
    Result := Result + LResult.ElapsedMs;
end;

function FindByName(const AResults: TBenchResults; const AName: string): TBenchResult;
var
  LResult: TBenchResult;
begin
  Result := Default(TBenchResult);
  for LResult in AResults do
    if LResult.Name = AName then
      Exit(LResult);
end;

procedure WriteSummary(const AShared: TBenchResult);
var
  LSer, LDeser: TBenchResult;
  LRoundTripUs, LDeltaUs: Double;
begin
  LSer := FindByName(GSerialize, 'StreamConfig');
  LDeser := FindByName(GDeserialize, 'StreamConfig');
  LRoundTripUs := LSer.MicrosPerOp + LDeser.MicrosPerOp;

  Writeln('Summary');
  Writeln('  ---------------------------------------------------------------------------');
  Writeln(Format('  Iterations per entity        %d', [GIterations]));
  Writeln(Format('  Total serialize              %.0f ms over %d entities',
    [TotalMs(GSerialize), Length(GSerialize)]));
  Writeln(Format('  Total deserialize            %.0f ms over %d entities',
    [TotalMs(GDeserialize), Length(GDeserialize)]));
  Writeln;
  Writeln(Format('  One StreamConfig round trip  %.2f us (%.2f out + %.2f back)',
    [LRoundTripUs, LSer.MicrosPerOp, LDeser.MicrosPerOp]));
  Writeln(Format('  That is %.0f API calls/sec of pure marshalling cost, before any network',
    [1000000 / LRoundTripUs]));
  Writeln;

  { TJetStreamJSON.ToJSON asks JetStreamJSONConfig for a fresh
    TNeonConfiguration on every call and re-registers the nullable serializers
    into it, so the obvious suspicion is that caching it would pay. Measured,
    rather than assumed - and it does not }
  LDeltaUs := LSer.MicrosPerOp - AShared.MicrosPerOp;
  Writeln('  Configuration overhead');
  Writeln(Format('    per-call config (public API)  %8.2f us/op', [LSer.MicrosPerOp]));
  Writeln(Format('    shared config (hoisted)       %8.2f us/op', [AShared.MicrosPerOp]));
  if Abs(LDeltaUs) < LSer.MicrosPerOp * NOISE_FLOOR then
  begin
    Writeln(Format('    -> difference %.2f us/op is under the %.0f%% noise floor: building the',
      [LDeltaUs, NOISE_FLOOR * 100]));
    Writeln('       config is not a measurable cost.');
  end
  else if LDeltaUs > 0 then
  begin
    Writeln(Format('    -> caching the config looks worth %.2f us/op (%.0f%% of the call)',
      [LDeltaUs, 100 * LDeltaUs / LSer.MicrosPerOp]));
    Writeln('       BUT this figure is not stable across runs - confirm it over several');
    Writeln('       before acting on it. See the note below.');
  end
  else
  begin
    Writeln(Format('    -> hoisting measured %.2f us/op SLOWER, which is not a real result',
      [-LDeltaUs]));
    Writeln('       either - it just means the two are within run-to-run drift.');
  end;
  Writeln('       Either way the bulk of the time is Neon walking RTTI on every call,');
  Writeln('       which is where any real optimisation would have to go.');
  Writeln;
  Writeln('  Caveat: absolute us/op drifts with run length on a laptop (thermal and');
  Writeln('  contention) - a long run reads slower than a short one for identical code.');
  Writeln('  Compare figures only within one run, and re-run before trusting a delta.');
  Writeln;

  if GFailures > 0 then
    Writeln(Format('  WARNING: %d round-trip checks FAILED - the timings above are ' +
      'measuring broken code', [GFailures]));
end;

{ --------------------------------------------------------------------------- }

var
  GShared: TBenchResult;
begin
  try
    if (ParamCount >= 1) and (StrToIntDef(ParamStr(1), 0) > 0) then
      GIterations := StrToInt(ParamStr(1));

    Writeln('JetStream entity serialization benchmark');
    Writeln('========================================');
    Writeln(Format('Iterations per entity: %d, best of %d passes', [GIterations, REPEATS]));
    Writeln;

    VerifyRoundTrips;

    Add(GSerialize, TBench.Serialize<TJetStreamPubAck>('PubAck', SamplePubAck));
    Add(GSerialize, TBench.Serialize<TJetStreamStreamConfig>('StreamConfig', SampleStreamConfig));
    Add(GSerialize, TBench.Serialize<TJetStreamConsumerConfig>('ConsumerConfig', SampleConsumerConfig));
    Add(GSerialize, TBench.Serialize<TJetStreamStreamInfo>('StreamInfo', SampleStreamInfo));
    Add(GSerialize, TBench.Serialize<TJetStreamConsumerInfo>('ConsumerInfo', SampleConsumerInfo));
    Add(GSerialize, TBench.Serialize<TJetStreamStreamListResponse>('StreamList (10 streams)', SampleStreamList));

    Add(GDeserialize, TBench.Deserialize<TJetStreamPubAck>('PubAck', SamplePubAck));
    Add(GDeserialize, TBench.Deserialize<TJetStreamStreamConfig>('StreamConfig', SampleStreamConfig));
    Add(GDeserialize, TBench.Deserialize<TJetStreamConsumerConfig>('ConsumerConfig', SampleConsumerConfig));
    Add(GDeserialize, TBench.Deserialize<TJetStreamStreamInfo>('StreamInfo', SampleStreamInfo));
    Add(GDeserialize, TBench.Deserialize<TJetStreamConsumerInfo>('ConsumerInfo', SampleConsumerInfo));
    Add(GDeserialize, TBench.Deserialize<TJetStreamStreamListResponse>('StreamList (10 streams)', SampleStreamList));

    GShared := BenchSerializeSharedConfig('StreamConfig (shared config)');

    WriteTable('Serialize', GSerialize);
    WriteTable('Deserialize', GDeserialize);
    WriteSummary(GShared);
    ReadLn;

    if GFailures > 0 then
      ExitCode := 1;
  except
    on E: Exception do
    begin
      Writeln(Format('%s: %s', [E.ClassName, E.Message]));
      ExitCode := 2;
    end;
  end;
end.
