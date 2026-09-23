{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{******************************************************************************}
{                                                                              }
{  A walk-through of the JetStream API against a live server: streams,         }
{  publishing with acknowledgements (deduplication and expectations), pull     }
{  and push consumption, Key/Value and Object Store.                           }
{                                                                              }
{  Requires a nats-server with JetStream enabled, e.g.:                        }
{      nats-server -js                                                         }
{                                                                              }
{  Every name is NUID-suffixed so consecutive runs never collide, and          }
{  everything this run creates is deleted again in the cleanup section.        }
{                                                                              }
{  The push-consumer handler runs on the connection's consumer thread, so it   }
{  only touches thread-safe state (a counter) and uses plain Ack, which only   }
{  writes - see the remarks on TJetStreamContext for what a handler may NOT    }
{  do (nothing that blocks on a reply).                                        }
{******************************************************************************}
program JetStreamDemo;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.SyncObjs,

  Nats.Consts in '..\Source\Nats.Consts.pas',
  Nats.Socket in '..\Source\Nats.Socket.pas',
  Nats.Socket.Indy in '..\Source\Nats.Socket.Indy.pas',
  Nats.Connection in '..\Source\Nats.Connection.pas',
  Nats.Exceptions in '..\Source\Nats.Exceptions.pas',
  Nats.Nuid in '..\Source\Nats.Nuid.pas',
  Nats.JetStream.Client in '..\Source\Nats.JetStream.Client.pas',
  Nats.JetStream.Consts in '..\Source\Nats.JetStream.Consts.pas',
  Nats.JetStream.Entities in '..\Source\Nats.JetStream.Entities.pas',
  Nats.JetStream.Message in '..\Source\Nats.JetStream.Message.pas',
  Nats.JetStream.KV in '..\Source\Nats.JetStream.KV.pas',
  Nats.JetStream.ObjectStore in '..\Source\Nats.JetStream.ObjectStore.pas';

var
  GConn: TNatsConnection;
  GJs: TJetStreamContext;
  GStream: string;        { the stream name, unique per run }
  GPushSid: Integer;      { the push subscription, for cleanup }
  GPushSeen: Integer;     { how many push deliveries the handler counted }
  GKV: TJetStreamKV;
  GObjStore: TJetStreamObjectStore;

procedure Banner(const ATitle: string);
begin
  Writeln;
  Writeln('=== ', ATitle, ' ', StringOfChar('=', 52 - Length(ATitle)));
end;

procedure DemoConnect;
begin
  Banner('Connecting');
  Writeln('  nats-server at 127.0.0.1:4222 - start it with "nats-server -js"');

  GConn := TNatsConnection.Create;
  GConn
    .SetName('JetStreamDemo')
    .SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, 5000)
    .Open(nil);

  if not GConn.WaitForReady(5000) then
    raise Exception.Create('The handshake did not complete: ' + GConn.LastError);

  GJs := TJetStreamContext.Create(GConn);
  Writeln('  connected, server max_payload ', GConn.MaxPayload);
end;

procedure DemoStreams;
begin
  Banner('Streams and publishing');
  GStream := 'ORDERS_' + TNUID.NextNuid;

  var cfg := Default(TJetStreamStreamConfig);
  cfg.Name := GStream;
  cfg.Subjects := [GStream + '.>'];
  cfg.Storage := TJetStreamStorage.Memory;   { no files left behind }
  var info := GJs.AddStream(cfg);
  Writeln('  created stream ', info.Config.Name);

  { Publishing WAITS for the stream's PubAck - the guarantee that the message
    was stored, not merely accepted and dropped }
  var ack := GJs.Publish(GStream + '.new', '{"id":1,"sku":"A"}');
  Writeln('  published to seq ', ack.Seq, ' on stream ', ack.Stream);

  { A Nats-Msg-Id deduplicates: publishing the same id again inside the
    duplicate window is NOT stored - the ack reports it }
  GJs.Publish(GStream + '.new', '{"id":2,"sku":"B"}',
    TJetStreamPubOptions.New.WithMsgId('order-2'));
  ack := GJs.Publish(GStream + '.new', '{"id":2,"sku":"B"}',
    TJetStreamPubOptions.New.WithMsgId('order-2'));
  Writeln('  duplicate publish: stored=', not ack.Duplicate,
    ' (seq ', ack.Seq, ' is the original)');

  { An expectation makes the server REJECT a publish that would break the
    sequence - the optimistic-concurrency primitive of streams }
  ack := GJs.Publish(GStream + '.new', '{"id":3,"sku":"C"}');
  var seq := ack.Seq;
  ack := GJs.Publish(GStream + '.new', '{"id":4,"sku":"D"}',
    TJetStreamPubOptions.New.WithExpectedLastSeq(seq));
  Writeln('  expected-last-seq held: stored at seq ', ack.Seq);

  info := GJs.StreamInfo(GStream);
  Writeln('  the stream now holds ', info.State.Messages, ' message(s)');
end;

procedure DemoPullConsume;
var
  LConfig: TJetStreamConsumerConfig;
  LInfo: TJetStreamConsumerInfo;
  LMsgs: TArray<IJetStreamMsg>;
  LMsg: IJetStreamMsg;
  LCount: Integer;
begin
  Banner('Pull consumption');

  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := 'workers';
  LConfig.AckPolicy := TJetStreamAckPolicy.Explicit;
  LConfig.FilterSubject := GStream + '.>';
  LInfo := GJs.AddConsumer(GStream, LConfig);
  Writeln('  created durable consumer ', LInfo.Name);

  { Fetch asks for up to N messages and blocks until they arrive, the server
    closes the batch, or the timeout elapses. A short batch is a result, not
    an error }
  LMsgs := GJs.Fetch(GStream, 'workers', 10, 2000);
  LCount := 0;
  for LMsg in LMsgs do
  begin
    Writeln('    fetched: ', LMsg.Payload, ' (seq ', LMsg.Metadata.StreamSeq, ')');
    LMsg.Ack;   { +ACK: done with it }
    Inc(LCount);
  end;
  Writeln('  fetched and acked ', LCount, ' message(s)');

  { Next is Fetch of one - the shape of a work-queue loop }
  if GJs.Next(GStream, 'workers', LMsg, 2000) then
  begin
    Writeln('    next: ', LMsg.Payload);
    LMsg.Ack;
  end
  else
    Writeln('    nothing waiting');

  LInfo := GJs.ConsumerInfo(GStream, 'workers');
  Writeln('  consumer state: acked up to seq ', LInfo.AckFloor.StreamSeq,
    ', ', LInfo.NumPending, ' pending');
end;

procedure DemoPushConsume;
var
  LConfig: TJetStreamConsumerConfig;
  LInfo: TJetStreamConsumerInfo;
begin
  Banner('Push consumption');

  LConfig := Default(TJetStreamConsumerConfig);
  LConfig.DurableName := 'pushers';
  LConfig.AckPolicy := TJetStreamAckPolicy.Explicit;
  { DeliverSubject is what makes this a PUSH consumer; New makes it deliver
    only what is published after it exists, not the stream's history }
  LConfig.DeliverSubject := 'deliver.' + GStream;
  LConfig.DeliverPolicy := TJetStreamDeliverPolicy.New;
  LInfo := GJs.AddConsumer(GStream, LConfig);
  Writeln('  created push consumer ', LInfo.Name, ' on ',
    LInfo.Config.DeliverSubject);

  GPushSeen := 0;
  GPushSid := GJs.SubscribePush(GStream, 'pushers',
    procedure (const AMsg: IJetStreamMsg)
    begin
      { Runs on the connection's consumer thread: only thread-safe state, and
        plain Ack (which only writes) is fine }
      TInterlocked.Increment(GPushSeen);
      AMsg.Ack;
    end);

  { Deliveries arrive asynchronously, so give the handler a moment }
  GJs.Publish(GStream + '.new', 'push me once');
  GJs.Publish(GStream + '.new', 'push me twice');
  Sleep(1500);

  Writeln('  push consumer delivered ', TInterlocked.Add(GPushSeen, 0),
    ' message(s)');
end;

procedure DemoKV;
var
  LBucket: string;
  LRev: UInt64;
  LEntry: TKVEntry;
  LKey: string;
  LStatus: TJetStreamKVStatus;
begin
  Banner('Key/Value');

  LBucket := 'cfg_' + TNUID.NextNuid;
  GKV := TJetStreamKV.CreateBucket(GJs, LBucket);
  Writeln('  created bucket ', LBucket, ' (stream ', GKV.StreamName, ')');

  GKV.Put('db.host', 'localhost');
  LRev := GKV.Put('db.host', 'db1');   { overwrite - a new revision }
  Writeln('  db.host = ', GKV.Get('db.host'), ' @ revision ', LRev);

  { Update is compare-and-set on the revision }
  GKV.Update('db.host', 'db2', LRev);
  Writeln('  updated via CAS to ', GKV.Get('db.host'));

  { PutIfAbsent is NATS's Create: it fails if the key already exists }
  GKV.PutIfAbsent('db.port', '5432');
  try
    GKV.PutIfAbsent('db.port', '1234');
    Writeln('  !! PutIfAbsent should have raised');
  except
    on E: EJetStreamKVError do
      Writeln('  PutIfAbsent on an existing key correctly raised');
  end;

  { Delete is a tombstone, not a removal - the history survives }
  GKV.Delete('db.port');
  Writeln('  db.port after delete -> "', GKV.Get('db.port', '<unset>'), '"');

  for LKey in GKV.Keys do
    Writeln('    key: ', LKey);

  for LEntry in GKV.History('db.host') do
    Writeln('    history: revision ', LEntry.Revision, ' -> "',
      LEntry.ValueString, '"');

  LStatus := GKV.Status;
  Writeln('  bucket status: ', LStatus.Values, ' key(s), ', LStatus.Bytes,
    ' byte(s)');
end;

procedure DemoObjectStore;
var
  LBucket: string;
  LInfo: TJetStreamObjectInfo;
  LBytes: TBytes;
  LObj: TJetStreamObjectInfo;
begin
  Banner('Object Store');

  LBucket := 'files_' + TNUID.NextNuid;
  GObjStore := TJetStreamObjectStore.CreateBucket(GJs, LBucket);
  Writeln('  created bucket ', LBucket, ' (stream ', GObjStore.StreamName, ')');

  GObjStore.PutString('readme.txt', 'Hello from the JetStream demo');
  { 200 KB: over the default 128 KB chunk size, so this is genuinely split }
  GObjStore.Put('blob.bin',
    TEncoding.UTF8.GetBytes(StringOfChar('x', 200 * 1024)));
  { the demo executable itself, read straight off disk }
  GObjStore.PutFile('demo-exe.bin', ParamStr(0));

  Writeln('  readme.txt -> "', GObjStore.GetString('readme.txt'), '"');

  if GObjStore.Get('blob.bin', LBytes) then
    Writeln('  blob.bin came back as ', Length(LBytes), ' byte(s)');

  if GObjStore.Info('demo-exe.bin', LInfo) then
    Writeln('  demo-exe.bin: ', LInfo.Size, ' byte(s) in ', LInfo.Chunks,
      ' chunk(s)');

  for LObj in GObjStore.List do
    Writeln('    stored: ', LObj.Name, ' (', LObj.Size, ' byte(s))');

  GObjStore.Delete('readme.txt');
  Writeln('  readme.txt after delete -> "',
    GObjStore.GetString('readme.txt', '<gone>'), '"');
end;

procedure Cleanup;
begin
  Banner('Cleanup');

  if Assigned(GConn) and GConn.Connected then
  begin
    if GPushSid <> 0 then
      try
        GConn.Unsubscribe(GPushSid);
      except
        on E: Exception do
          Writeln('  (unsubscribe failed: ', E.Message, ')');
      end;

    try
      if Assigned(GKV) then
        TJetStreamKV.DeleteBucket(GJs, GKV.Bucket);
      if Assigned(GObjStore) then
        TJetStreamObjectStore.DeleteBucket(GJs, GObjStore.Bucket);
      if GStream <> '' then
        GJs.DeleteStream(GStream);   { deleting the stream removes its consumers }
      Writeln('  removed the stream and both buckets');
    except
      on E: Exception do
        Writeln('  (cleanup failed: ', E.ClassName, ': ', E.Message, ')');
    end;
  end;

  GJs.Free;
  GConn.Free;
end;

begin
  Writeln('nats.delphi - JetStream API walk-through');
  Writeln(StringOfChar('=', 56));
  try
    try
      DemoConnect;
      DemoStreams;
      DemoPullConsume;
      DemoPushConsume;
      DemoKV;
      DemoObjectStore;
      Banner('Done');
      Writeln('  everything worked. See Docs\JetStream-API.md for the reference.');
    except
      on E: Exception do
      begin
        Writeln;
        Writeln('FAILED: ', E.ClassName, ': ', E.Message);
        Writeln('  Is a nats-server with JetStream running? Start one with "nats-server -js".');
        ExitCode := 1;
      end;
    end;
  finally
    Cleanup;
  end;
end.