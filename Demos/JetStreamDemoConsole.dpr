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
program JetStreamClientDemo;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.Generics.Collections,
  System.DateUtils, // For Now, MilliSecondsBetween
  // NATS Core Library (ensure these are the modified versions)
  Nats.Consts,
  Nats.Entities,      // Core NATS entities
  Nats.Exceptions,
  Nats.Classes,
  Nats.Parser,
  Nats.Socket,
  Nats.Socket.Indy,   // Specific socket implementation
  Nats.Connection,
  // NATS JetStream Library
  NATS.JetStream.Entities,
  NATS.JetStream.Enums,
  NATS.JetStream.Client;

var
  GNatsConn: TNatsConnection;
  GJSContext: TJetStreamContext;
  GIsConnected: Boolean = False;
  GStreamName: string = 'DEMO_STREAM';
  GConsumerName: string = 'DEMO_PULL_CONSUMER';
  GSubjectOrders: string = 'orders.>'; // Wildcard subject for the stream
  GSubjectOrderUSA: string = 'orders.usa';
  GSubjectOrderEU: string = 'orders.eu';

procedure Log(const AMessage: string);
begin
  WriteLn(FormatDateTime('[hh:nn:ss.zzz] ', Now) + AMessage);
end;

procedure PrintSeparator(Title: string = '');
begin
  if Title <> '' then
    WriteLn(StringOfChar('-', 10) + ' ' + Title + ' ' + StringOfChar('-', 40 - Length(Title)))
  else
    WriteLn(StringOfChar('-', 60));
end;

procedure NATSConnectHandler(AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions);
begin
  Log('Connected to NATS Server: ' + AInfo.server_name + ' (' + AInfo.version + ')');
  Log('JetStream available: ' + BoolToStr(AInfo.jetstream, True));
  if not AInfo.jetstream then
  begin
    Log('ERROR: JetStream is not available on this server!');
    Exit;
  end;
  GIsConnected := True;
end;

procedure NATSDisconnectHandler;
begin
  Log('Disconnected from NATS Server.');
  GIsConnected := False;
end;

function SetupAndConnectNATS: Boolean;
begin
  Result := False;
  Log('Setting up NATS connection...');
  GNatsConn := TNatsConnection.Create;
  GNatsConn.Name := 'JetStreamDemoClient';
  GNatsConn.SetChannel('localhost', 4222, 1000); // Use consts from SharedData or define here
  GNatsConn.ConnectOptions.verbose := False;

  try
    GNatsConn.Open(NATSConnectHandler, NATSDisconnectHandler);
    Log('NATS connection opening...');
    // Give some time for connection and INFO processing
    var LStartTime: TDateTime := Now;
    while not GIsConnected and (MilliSecondsBetween(Now, LStartTime) < 5000) do // Wait max 5s
      Sleep(100);

    if GIsConnected then
    begin
      GJSContext := TJetStreamContext.Create(GNatsConn);
      Result := True;
      Log('JetStream context created.');
    end
    else
    begin
      Log('Failed to connect to NATS within timeout.');
      GNatsConn.Free;
      GNatsConn := nil;
    end;
  except
    on E: Exception do
    begin
      Log('Error connecting to NATS: ' + E.Message);
      if Assigned(GNatsConn) then GNatsConn.Free;
      GNatsConn := nil;
    end;
  end;
end;

procedure TeardownNATS;
begin
  Log('Tearing down NATS connection...');
  if Assigned(GJSContext) then FreeAndNil(GJSContext);
  if Assigned(GNatsConn) then
  begin
    if GNatsConn.Connected then GNatsConn.Close;
    FreeAndNil(GNatsConn);
  end;
  GIsConnected := False;
  Log('NATS teardown complete.');
end;

procedure PrintStreamConfig(const AConfig: TJSStreamConfig);
begin
  WriteLn('  Stream Config:');
  WriteLn('    Name: ', AConfig.name);
  WriteLn('    Subjects: ', string.Join(', ', AConfig.subjects));
  WriteLn('    Retention: ', RetentionPolicyStrings[AConfig.retention]);
  WriteLn('    Storage: ', StorageTypeStrings[Aconfig.storage]);
  WriteLn('    Max Msgs: ', AConfig.max_msgs);
  WriteLn('    Max Bytes: ', AConfig.max_bytes);
  // Add more fields as needed
end;

procedure PrintStreamState(const AState: TJSStreamState);
begin
  WriteLn('  Stream State:');
  WriteLn('    Messages: ', AState.messages);
  WriteLn('    Bytes: ', AState.bytes);
  WriteLn('    First Seq: ', AState.first_seq);
  WriteLn('    Last Seq: ', AState.last_seq);
  // Add more fields as needed
end;

procedure PrintConsumerConfig(const AConfig: TJSConsumerConfig);
begin
  WriteLn('  Consumer Config:');
  WriteLn('    Name: ', AConfig.name);
  WriteLn('    Durable Name: ', AConfig.durable_name);
  WriteLn('    Deliver Policy: ', DeliverPolicyStrings[AConfig.deliver_policy]);
  WriteLn('    Ack Policy: ', AckPolicyStrings[AConfig.ack_policy]);
  WriteLn('    Filter Subject: ', AConfig.filter_subject);
  // Add more fields as needed
end;

procedure PrintJSMessage(const AMsg: TJSReceivedMessage; AIndex: Integer);
begin
  WriteLn(Format('  Message [%d]:', [AIndex]));
  WriteLn('    Subject: ', AMsg.Subject);
  WriteLn('    ReplyTo: ', AMsg.ReplyTo);
  WriteLn('    Stream: ', AMsg.Stream);
  WriteLn('    Sequence: ', AMsg.Sequence);
  WriteLn('    Consumer Seq: ', AMsg.ConsumerSequence);
  WriteLn('    Timestamp (ns): ', AMsg.Timestamp);
  WriteLn('    Data: ', TEncoding.UTF8.GetString(AMsg.Data));
  if Assigned(AMsg.Headers) and (AMsg.Headers.Count > 0) then
  begin
    WriteLn('    Headers:');
    WriteLn(AMsg.Headers.Text)
  end;
end;

procedure DemoAccountInfo;
var
  LAccInfoResp: TJSAccountInfoResponse;
begin
  PrintSeparator('JetStream Account Info');
  if not GIsConnected then Exit;

  LAccInfoResp := TJSAccountInfoResponse.Create;
  try
    if GJSContext.GetAccountInfo(LAccInfoResp) then
    begin
      if LAccInfoResp.HasError then
        Log('Error getting account info: (' + LAccInfoResp.error.err_code.ToString + ') ' + LAccInfoResp.error.description)
      else
      begin
        Log('Account Info Retrieved:');
        Log(Format('  Memory: %d bytes, Storage: %d bytes, Streams: %d, Consumers: %d',
          [LAccInfoResp.memory, LAccInfoResp.storage, LAccInfoResp.streams, LAccInfoResp.consumers]));
        // Log limits if populated
      end;
    end
    else
      Log('Failed to call GetAccountInfo.');
  finally
    LAccInfoResp.Free;
  end;
end;

procedure DemoStreamManagement;
var
  LStreamConfig: TJSStreamConfig;
  LStreamCreateResp: TJSStreamCreateResponse;
  LStreamInfoResp: TJSStreamInfoResponse;
  //LStreamDeleteResp: TJSStreamDeleteResponse;
  LStreamNamesResp: TJSStreamNamesResponse;
  S: string;
begin
  PrintSeparator('Stream Management');
  if not GIsConnected then Exit;

  // 1. Create Stream
  Log('Attempting to create stream: ' + GStreamName);
  LStreamConfig.name := GStreamName;
  LStreamConfig.subjects := [GSubjectOrders]; // Stream listens to 'orders.>'
  LStreamConfig.storage := TStorageType.stFile; // Use stMemory for temp if preferred
  LStreamConfig.retention := TRetentionPolicy.rpLimits;
  LStreamConfig.max_msgs := 10000; // Example limit
  LStreamConfig.max_age := 0; // No age limit by default
  LStreamConfig.duplicate_window := 120 * 1000 * 1000; // 2 minutes in ns
  LStreamConfig.num_replicas := 1; // For single server setup

  LStreamCreateResp := TJSStreamCreateResponse.Create;
  try
    if GJSContext.CreateStream(LStreamConfig, LStreamCreateResp) then
    begin
      if LStreamCreateResp.HasError then
        Log(Format('Error creating stream "%s": (%d) %s', [GStreamName, LStreamCreateResp.error.err_code, LStreamCreateResp.error.description]))
      else
      begin
        Log('Stream "' + GStreamName + '" created successfully.');
        PrintStreamConfig(LStreamCreateResp.config);
        PrintStreamState(LStreamCreateResp.state);
      end;
    end
    else
      Log('API call to CreateStream for "' + GStreamName + '" failed.');
  finally
    LStreamCreateResp.Free;
  end;
  ReadLn; // Pause

  // 2. Get Stream Info
  Log('Attempting to get info for stream: ' + GStreamName);
  LStreamInfoResp := TJSStreamInfoResponse.Create;
  try
    if GJSContext.GetStreamInfo(GStreamName, LStreamInfoResp) then
    begin
      if LStreamInfoResp.HasError then
        Log(Format('Error getting info for stream "%s": (%d) %s', [GStreamName, LStreamInfoResp.error.err_code, LStreamInfoResp.error.description]))
      else
      begin
        Log('Stream Info for "' + GStreamName + '":');
        PrintStreamConfig(LStreamInfoResp.config);
        PrintStreamState(LStreamInfoResp.state);
      end;
    end
    else
      Log('API call to GetStreamInfo for "' + GStreamName + '" failed.');
  finally
    LStreamInfoResp.Free;
  end;
  ReadLn;

  // 3. List Streams
  Log('Listing all streams...');
  LStreamNamesResp := TJSStreamNamesResponse.Create;
  try
    if GJSContext.ListStreams(LStreamNamesResp) then
    begin
      if LStreamNamesResp.HasError then
        Log('Error listing streams: ' + LStreamNamesResp.error.description)
      else
      begin
        Log(Format('Found %d streams (Total: %d, Limit: %d, Offset: %d):',
          [Length(LStreamNamesResp.streams), LStreamNamesResp.total, LStreamNamesResp.limit, LStreamNamesResp.offset]));
        for S in LStreamNamesResp.streams do
          Log('  - ' + S);
      end;
    end
    else
      Log('API call to ListStreams failed.');
  finally
    LStreamNamesResp.Free;
  end;
  ReadLn;

  // (Optional: Update Stream - example, might be same as CreateStream if fields overlap)
  // Log('Attempting to update stream: ' + GStreamName);
  // LStreamConfig.description := 'Updated stream description';
  // LStreamConfig.max_msgs := 20000;
  // LStreamCreateResp := TJSStreamCreateResponse.Create; // Re-use or new instance
  // try
  //   if GJSContext.UpdateStream(LStreamConfig, LStreamCreateResp) then ...
  // finally LStreamCreateResp.Free; end;
end;

procedure DemoPublishing;
var
  LPubAck: TJSPubAck;
  LPublishOpts: TJetStreamPublishOptions;
  LMsgData: TBytes;
  I: Integer;
begin
  PrintSeparator('Publishing Messages');
  if not GIsConnected then Exit;

  LPubAck := TJSPubAck.Create;
  try
    // 1. Publish a simple string message
    Log('Publishing string message to: ' + GSubjectOrderUSA);
    if GJSContext.Publish(GSubjectOrderUSA, 'Hello from Delphi (USA)!', LPubAck) then
    begin
      if LPubAck.HasError then
        Log('Error in PubAck: ' + LPubAck.error.description)
      else
        Log(Format('Message published to stream "%s", sequence: %d', [LPubAck.stream, LPubAck.seq]));
    end
    else
      Log('Publish call failed or timed out.');

    // 2. Publish a byte message
    LMsgData := TEncoding.UTF8.GetBytes('Binary data packet for EU');
    Log('Publishing byte message to: ' + GSubjectOrderEU);
    if GJSContext.PublishBytes(GSubjectOrderEU, LMsgData, LPubAck) then
    begin
      if LPubAck.HasError then
        Log('Error in PubAck: ' + LPubAck.error.description)
      else
        Log(Format('Byte Message published to stream "%s", sequence: %d', [LPubAck.stream, LPubAck.seq]));
    end
    else
      Log('PublishBytes call failed or timed out.');

    // 3. Publish with options (e.g., ExpectedLastSeq)
    //LPublishOpts.Create; // Initialize options record
    try
      LPublishOpts.MsgID := TGuid.NewGuid.ToString; // Set a unique message ID
      LPublishOpts.ExpectedStream := GStreamName;
      // LPublishOpts.ExpectedLastSeq := LPubAck.seq; // Example: expect last published sequence

      Log('Publishing string message with options to: ' + GSubjectOrderUSA);
      if GJSContext.Publish(GSubjectOrderUSA, 'Message with Options!', LPubAck, LPublishOpts) then
      begin
        if LPubAck.HasError then
          Log(Format('Error in PubAck (with options): (%d) %s', [LPubAck.error.code, LPubAck.error.description]))
        else
          Log(Format('Message with options published to stream "%s", sequence: %d, MsgID: %s',
            [LPubAck.stream, LPubAck.seq, LPublishOpts.MsgID]));
      end
      else
        Log('Publish call (with options) failed or timed out.');
    finally
      //LPublishOpts.Destroy; // Clean up TJetStreamPublishOptions
    end;

    // Publish a few more for batch fetch later
    for I := 1 to 5 do
    begin
      if GJSContext.Publish(GSubjectOrderEU, 'Batch Message ' + I.ToString, LPubAck) then
        Log(Format('Batch msg %d published, seq: %d', [I, LPubAck.seq]))
      else
        Log(Format('Failed to publish batch msg %d', [I]));
      Sleep(10); // Small delay
    end;

  finally
    LPubAck.Free;
  end;
  ReadLn;
end;

procedure DemoConsumerManagement;
var
  LConsumerConfig: TJSConsumerConfig;
  LConsumerCreateResp: TJSConsumerCreateResponse;
  LConsumerInfoResp: TJSConsumerInfoResponse;
  LConsumerNamesResp: TJSConsumerNamesResponse;
  //LConsumerDeleteResp: TJSConsumerDeleteResponse;
  S: string;
begin
  PrintSeparator('Consumer Management');
  if not GIsConnected then Exit;

  // 1. Create a Pull Consumer
  Log('Attempting to create pull consumer: ' + GConsumerName + ' on stream ' + GStreamName);
  LConsumerConfig.durable_name := GConsumerName;
  LConsumerConfig.ack_policy := TAckPolicy.apExplicit; // Must ack messages
  LConsumerConfig.deliver_policy := TDeliverPolicy.dpAll; // Deliver all messages from start (for new consumer)
  LConsumerConfig.filter_subject := GSubjectOrderEU; // Only messages for EU for this consumer

  LConsumerCreateResp := TJSConsumerCreateResponse.Create;
  try
    if GJSContext.CreateConsumer(GStreamName, LConsumerConfig, LConsumerCreateResp) then
    begin
      if LConsumerCreateResp.HasError then
        Log(Format('Error creating consumer "%s": (%d) %s', [GConsumerName, LConsumerCreateResp.error.err_code, LConsumerCreateResp.error.description]))
      else
      begin
        Log('Consumer "' + LConsumerCreateResp.name + '" created successfully.');
        PrintConsumerConfig(LConsumerCreateResp.config);
      end;
    end
    else
      Log('API call to CreateConsumer failed.');
  finally
    LConsumerCreateResp.Free;
  end;
  ReadLn;

  // 2. Get Consumer Info
  Log('Attempting to get info for consumer: ' + GConsumerName);
  LConsumerInfoResp := TJSConsumerInfoResponse.Create;
  try
    if GJSContext.GetConsumerInfo(GStreamName, GConsumerName, LConsumerInfoResp) then
    begin
      if LConsumerInfoResp.HasError then
        Log(Format('Error getting info for consumer "%s": (%d) %s', [GConsumerName, LConsumerInfoResp.error.err_code, LConsumerInfoResp.error.description]))
      else
      begin
        Log('Consumer Info for "' + LConsumerInfoResp.name + '":');
        PrintConsumerConfig(LConsumerInfoResp.config);
        Log(Format('  Num Pending: %d, Num Ack Pending: %d', [LConsumerInfoResp.num_pending, LConsumerInfoResp.num_ack_pending]));
      end;
    end
    else
      Log('API call to GetConsumerInfo failed.');
  finally
    LConsumerInfoResp.Free;
  end;
  ReadLn;

  // 3. List Consumers
  Log('Listing consumers for stream: ' + GStreamName);
  LConsumerNamesResp := TJSConsumerNamesResponse.Create;
  try
    if GJSContext.ListConsumers(GStreamName, LConsumerNamesResp) then
    begin
      if LConsumerNamesResp.HasError then
        Log('Error listing consumers: ' + LConsumerNamesResp.error.description)
      else
      begin
        Log(Format('Found %d consumers for stream "%s":', [Length(LConsumerNamesResp.consumers), GStreamName]));
        for S in LConsumerNamesResp.consumers do
          Log('  - ' + S);
      end;
    end
    else
      Log('API call to ListConsumers failed.');
  finally
    LConsumerNamesResp.Free;
  end;
  ReadLn;
end;

procedure DemoMessageConsumption;
var
  LMessages: TList<TJSReceivedMessage>;
  LMsg: TJSReceivedMessage;
  I: Integer;
  LFetchCount: Integer;
begin
  PrintSeparator('Message Consumption (Pull Consumer)');
  if not GIsConnected then Exit;

  LMessages := TList<TJSReceivedMessage>.Create;
  try
    // 1. Fetch a batch of messages
    LFetchCount := 3; // Fetch up to 3 messages
    Log(Format('Attempting to fetch %d messages for consumer "%s"...', [LFetchCount, GConsumerName]));
    // Expires in 5 seconds (in nanoseconds), no_wait = false (wait for messages)
    if GJSContext.FetchMessages(GStreamName, GConsumerName, LFetchCount, LMessages, 5 * 1000 * 1000) then
    begin
      Log(Format('Fetch call completed. Received %d messages:', [LMessages.Count]));
      if LMessages.Count = 0 then
        Log('  No messages available or fetch timed out on server side (check status headers if available).')
      else
      begin
        for I := 0 to LMessages.Count - 1 do
        begin
          LMsg := LMessages[I]; // This is a record
          PrintJSMessage(LMsg, I);
          Log('  Acking message seq: ' + LMsg.Sequence.ToString);
          GJSContext.Ack(LMsg); // Ack the message using its ReplyTo
        end;
      end;
    end
    else
      Log('FetchMessages API call itself failed or timed out client-side.');

    // Free TJSReceivedMessage contents (specifically Headers)
    LMessages.Clear;
    ReadLn;

    // 2. Fetch with no_wait (should return immediately)
    Log('Attempting to fetch with no_wait...');
    if GJSContext.FetchMessages(GStreamName, GConsumerName, 5, LMessages, 0, 0, True) then
    begin
      Log(Format('Fetch (no_wait) call completed. Received %d messages:', [LMessages.Count]));
      if LMessages.Count = 0 then
        Log('  No messages immediately available (as expected with no_wait if queue is empty or caught up).')
      else
      begin
         for I := 0 to LMessages.Count - 1 do
        begin
          LMsg := LMessages[I];
          PrintJSMessage(LMsg, I);
          Log('  Acking message seq: ' + LMsg.Sequence.ToString);
          GJSContext.Ack(LMsg);
        end;
      end;
    end
    else
      Log('FetchMessages (no_wait) API call failed.');

    LMessages.Clear;
  finally
    LMessages.Free;
  end;
  ReadLn;
end;

procedure CleanupResources;
var
  LConsumerDeleteResp: TJSConsumerDeleteResponse;
  LStreamDeleteResp: TJSStreamDeleteResponse;
begin
  PrintSeparator('Cleaning Up Resources');
  if not GIsConnected or not Assigned(GJSContext) then Exit;

  // Delete Consumer
  Log('Attempting to delete consumer: ' + GConsumerName);
  LConsumerDeleteResp := TJSConsumerDeleteResponse.Create;
  try
    if GJSContext.DeleteConsumer(GStreamName, GConsumerName, LConsumerDeleteResp) then
    begin
      if LConsumerDeleteResp.HasError then
        Log(Format('Error deleting consumer "%s": %s', [GConsumerName, LConsumerDeleteResp.error.description]))
      else if LConsumerDeleteResp.success then
        Log('Consumer "' + GConsumerName + '" deleted successfully.')
      else
        Log('DeleteConsumer for "' + GConsumerName + '" did not report success.');
    end
    else
      Log('API call to DeleteConsumer failed.');
  finally
    LConsumerDeleteResp.Free;
  end;

  // Delete Stream
  Log('Attempting to delete stream: ' + GStreamName);
  LStreamDeleteResp := TJSStreamDeleteResponse.Create;
  try
    if GJSContext.DeleteStream(GStreamName, LStreamDeleteResp) then
    begin
      if LStreamDeleteResp.HasError then
        Log(Format('Error deleting stream "%s": %s', [GStreamName, LStreamDeleteResp.error.description]))
      else if LStreamDeleteResp.success then
        Log('Stream "' + GStreamName + '" deleted successfully.')
      else
        Log('DeleteStream for "' + GStreamName + '" did not report success.');
    end
    else
      Log('API call to DeleteStream failed.');
  finally
    LStreamDeleteResp.Free;
  end;
end;

begin
  ReportMemoryLeaksOnShutdown := True;
  Log('JetStream Demo Application Started.');
  try
    if SetupAndConnectNATS then
    begin
      DemoAccountInfo;
      DemoStreamManagement;   // Creates GStreamName
      DemoPublishing;         // Publishes to GStreamName subjects
      DemoConsumerManagement; // Creates GConsumerName on GStreamName
      DemoMessageConsumption; // Consumes from GConsumerName

      Log('Demo sequence complete. Press Enter to cleanup and exit.');
      ReadLn;

      CleanupResources;       // Deletes GConsumerName and GStreamName
    end
    else
    begin
      Log('Could not connect to NATS. Demo aborted.');
      ReadLn;
    end;
  except
    on E: Exception do
    begin
      Log('Unhandled Exception: ' + E.ClassName + ': ' + E.Message);
      ReadLn;
    end;
  end;
  TeardownNATS;
  Log('JetStream Demo Application Finished. Press Enter to close.');
  ReadLn;
end.
