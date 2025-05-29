unit JetStreamVCLDemoForm;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls, Vcl.ComCtrls, Vcl.ExtCtrls,
  System.Generics.Collections, System.DateUtils,
  // NATS Core Library
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
  NATS.JetStream.Client;

type
  TfrmJetStreamVCLDemo = class(TForm)
    pcMain: TPageControl;
    tsConnect: TTabSheet;
    grpConnect: TGroupBox;
    lblHost: TLabel;
    edtHost: TEdit;
    lblPort: TLabel;
    edtPort: TEdit;
    btnConnect: TButton;
    btnDisconnect: TButton;
    lblStatus: TLabel;
    tsAccountInfo: TTabSheet;
    mmAccountInfo: TMemo;
    btnGetAccountInfo: TButton;
    tsStreams: TTabSheet;
    grpStreamList: TGroupBox;
    lbStreams: TListBox;
    btnListStreams: TButton;
    grpStreamCreate: TGroupBox;
    lblStreamNameCreate: TLabel;
    edtStreamNameCreate: TEdit;
    lblStreamSubjectsCreate: TLabel;
    edtStreamSubjectsCreate: TEdit;
    btnStreamCreate: TButton;
    grpStreamDetails: TGroupBox;
    mmStreamInfo: TMemo;
    btnStreamInfo: TButton;
    btnStreamDelete: TButton;
    edtStreamNameDetail: TEdit;
    lblStreamNameDetail: TLabel;
    tsPublish: TTabSheet;
    grpPublish: TGroupBox;
    lblPublishSubject: TLabel;
    edtPublishSubject: TEdit;
    lblPublishMessage: TLabel;
    edtPublishMessage: TEdit;
    btnPublish: TButton;
    mmPubAck: TMemo;
    lblPublishStream: TLabel;
    edtPublishExpectedStream: TEdit;
    tsConsumers: TTabSheet;
    grpConsumerList: TGroupBox;
    lbConsumers: TListBox;
    btnListConsumers: TButton;
    edtConsumerStreamNameList: TEdit;
    lblConsumerStreamNameList: TLabel;
    grpConsumerCreate: TGroupBox;
    lblConsumerStreamNameCreate: TLabel;
    edtConsumerStreamNameCreate: TEdit;
    lblConsumerNameCreate: TLabel;
    edtConsumerNameCreate: TEdit;
    btnConsumerCreate: TButton;
    grpConsumerDetails: TGroupBox;
    mmConsumerInfo: TMemo;
    btnConsumerInfo: TButton;
    btnConsumerDelete: TButton;
    edtConsumerNameDetail: TEdit;
    lblConsumerNameDetail: TLabel;
    edtConsumerStreamNameDetail: TEdit;
    lblConsumerStreamNameDetail: TLabel;
    tsConsume: TTabSheet;
    grpConsume: TGroupBox;
    lblConsumeStream: TLabel;
    edtConsumeStream: TEdit;
    lblConsumeConsumer: TLabel;
    edtConsumeConsumer: TEdit;
    btnFetchMessages: TButton;
    mmMessages: TMemo;
    btnAckSelected: TButton;
    lblConsumeBatchSize: TLabel;
    edtConsumeBatchSize: TEdit;
    sbMain: TStatusBar;
    mmLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure btnConnectClick(Sender: TObject);
    procedure btnDisconnectClick(Sender: TObject);
    procedure btnGetAccountInfoClick(Sender: TObject);
    procedure btnListStreamsClick(Sender: TObject);
    procedure btnStreamCreateClick(Sender: TObject);
    procedure btnStreamInfoClick(Sender: TObject);
    procedure btnStreamDeleteClick(Sender: TObject);
    procedure btnPublishClick(Sender: TObject);
    procedure btnListConsumersClick(Sender: TObject);
    procedure btnConsumerCreateClick(Sender: TObject);
    procedure btnConsumerInfoClick(Sender: TObject);
    procedure btnConsumerDeleteClick(Sender: TObject);
    procedure btnFetchMessagesClick(Sender: TObject);
    procedure btnAckSelectedClick(Sender: TObject);
    procedure lbStreamsClick(Sender: TObject);
    procedure lbConsumersClick(Sender: TObject);
  private
    FNatsConn: TNatsConnection;
    FJSContext: TJetStreamContext;
    FIsConnected: Boolean;
    FLastFetchedMessages: TList<TJSReceivedMessage>; // To hold messages for acking

    procedure Log(const AMessage: string);
    procedure NATSConnectHandler(AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions);
    procedure NATSDisconnectHandler;
    procedure UpdateUIConnectedState;

    procedure ClearStreamDetails;
    procedure ClearConsumerDetails;
    procedure ClearMessageDetails;

    function GetSelectedStreamName: string;
    function GetSelectedConsumerName(out AStreamName: string): string;
  public
    { Public declarations }
  end;

var
  frmJetStreamVCLDemo: TfrmJetStreamVCLDemo;

implementation

{$R *.dfm}

procedure TfrmJetStreamVCLDemo.FormCreate(Sender: TObject);
begin
  FIsConnected := False;
  FNatsConn := nil;
  FJSContext := nil;
  FLastFetchedMessages := TList<TJSReceivedMessage>.Create;
  UpdateUIConnectedState;
  Log('JetStream VCL Demo Initialized.');
  // Set some defaults
  edtHost.Text := Nats.Consts.NATS_HOST; // Assuming NATS_HOST is defined in Nats.Consts
  edtPort.Text := Nats.Consts.NATS_PORT.ToString; // Assuming NATS_PORT is defined
  edtStreamNameCreate.Text := 'VCL_STREAM';
  edtStreamSubjectsCreate.Text := 'vcl.test.>';
  edtPublishSubject.Text := 'vcl.test.data';
  edtPublishExpectedStream.Text := 'VCL_STREAM';
  edtConsumerStreamNameCreate.Text := 'VCL_STREAM';
  edtConsumerNameCreate.Text := 'VCL_PULL_CONSUMER';
  edtConsumeStream.Text := 'VCL_STREAM';
  edtConsumeConsumer.Text := 'VCL_PULL_CONSUMER';
  edtConsumeBatchSize.Text := '5';
end;

procedure TfrmJetStreamVCLDemo.FormDestroy(Sender: TObject);
begin
  if Assigned(FNatsConn) and FNatsConn.Connected then
  begin
    FNatsConn.Close;
  end;
  FreeAndNil(FJSContext);
  FreeAndNil(FNatsConn);
  if Assigned(FLastFetchedMessages) then
  begin
    for var LMsg in FLastFetchedMessages do LMsg.FreeHeaders; // Free headers of stored messages
    FLastFetchedMessages.Free;
  end;
end;

procedure TfrmJetStreamVCLDemo.Log(const AMessage: string);
begin
  // Ensure UI updates are on the main thread
  TThread.Queue(nil, procedure
  begin
    mmLog.Lines.Add(FormatDateTime('[hh:nn:ss.zzz] ', Now) + AMessage);
    // Auto-scroll
    mmLog.SelStart := mmLog.GetTextLen;
    mmLog.SelLength := 0;
    mmLog.ScrollBy(0, mmLog.Lines.Count * 2); // Approximate scroll
  end);
end;

procedure TfrmJetStreamVCLDemo.NATSConnectHandler(AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions);
begin
  Log('Connected to NATS Server: ' + AInfo.server_name + ' (' + AInfo.version + ')');
  Log('JetStream available: ' + BoolToStr(AInfo.jetstream, True));
  if not AInfo.jetstream then
  begin
    Log('ERROR: JetStream is not available on this server!');
    // Optionally disconnect or disable JS features
    TThread.Queue(nil, procedure begin sbMain.SimpleText := 'Connected (JetStream NOT available)'; end);
    Exit;
  end;
  FIsConnected := True;
  TThread.Queue(nil, procedure
  begin
    UpdateUIConnectedState;
    sbMain.SimpleText := 'Connected to ' + AInfo.server_name + ' (JetStream Available)';
    // Create JetStream context now that we are connected
    if Assigned(FJSContext) then FreeAndNil(FJSContext);
    FJSContext := TJetStreamContext.Create(FNatsConn);
    Log('JetStream context created.');
  end);
end;

procedure TfrmJetStreamVCLDemo.NATSDisconnectHandler;
begin
  Log('Disconnected from NATS Server.');
  FIsConnected := False;
  TThread.Queue(nil, procedure
  begin
    UpdateUIConnectedState;
    sbMain.SimpleText := 'Disconnected';
    if Assigned(FJSContext) then FreeAndNil(FJSContext);
  end);
end;

procedure TfrmJetStreamVCLDemo.UpdateUIConnectedState;
begin
  btnConnect.Enabled := not FIsConnected;
  btnDisconnect.Enabled := FIsConnected;
  edtHost.Enabled := not FIsConnected;
  edtPort.Enabled := not FIsConnected;

  // Enable/disable JetStream feature buttons
  btnGetAccountInfo.Enabled := FIsConnected and Assigned(FJSContext);
  btnListStreams.Enabled := FIsConnected and Assigned(FJSContext);
  btnStreamCreate.Enabled := FIsConnected and Assigned(FJSContext);
  btnStreamInfo.Enabled := FIsConnected and Assigned(FJSContext) and (lbStreams.ItemIndex <> -1);
  btnStreamDelete.Enabled := FIsConnected and Assigned(FJSContext) and (lbStreams.ItemIndex <> -1);
  btnPublish.Enabled := FIsConnected and Assigned(FJSContext);
  btnListConsumers.Enabled := FIsConnected and Assigned(FJSContext);
  btnConsumerCreate.Enabled := FIsConnected and Assigned(FJSContext);
  btnConsumerInfo.Enabled := FIsConnected and Assigned(FJSContext) and (lbConsumers.ItemIndex <> -1);
  btnConsumerDelete.Enabled := FIsConnected and Assigned(FJSContext) and (lbConsumers.ItemIndex <> -1);
  btnFetchMessages.Enabled := FIsConnected and Assigned(FJSContext);
  btnAckSelected.Enabled := FIsConnected and Assigned(FJSContext) and (FLastFetchedMessages.Count > 0); // Simplified
end;

procedure TfrmJetStreamVCLDemo.ClearStreamDetails;
begin
  mmStreamInfo.Clear;
  edtStreamNameDetail.Clear;
end;

procedure TfrmJetStreamVCLDemo.ClearConsumerDetails;
begin
  mmConsumerInfo.Clear;
  edtConsumerStreamNameDetail.Clear;
  edtConsumerNameDetail.Clear;
end;

procedure TfrmJetStreamVCLDemo.ClearMessageDetails;
begin
  mmMessages.Clear;
  for var LMsg in FLastFetchedMessages do LMsg.FreeHeaders;
  FLastFetchedMessages.Clear;
  btnAckSelected.Enabled := False;
end;

function TfrmJetStreamVCLDemo.GetSelectedStreamName: string;
begin
  Result := '';
  if lbStreams.ItemIndex <> -1 then
    Result := lbStreams.Items[lbStreams.ItemIndex];
end;

function TfrmJetStreamVCLDemo.GetSelectedConsumerName(out AStreamName: string): string;
var
  S: string;
begin
  Result := '';
  AStreamName := '';
  if lbConsumers.ItemIndex <> -1 then
  begin
    S := lbConsumers.Items[lbConsumers.ItemIndex]; // Format: "ConsumerName (on StreamName)"
    var LParts := S.Split(['(', ' ', ')']); // Basic split
    if Length(LParts) >= 1 then Result := Trim(LParts[0]);
    if Length(LParts) >= 3 then AStreamName := Trim(LParts[2]); // Assuming "on" is LParts[1]
  end;
end;


procedure TfrmJetStreamVCLDemo.btnConnectClick(Sender: TObject);
begin
  if FIsConnected then Exit;
  Log('Attempting to connect to NATS...');
  sbMain.SimpleText := 'Connecting...';

  if Assigned(FNatsConn) then FreeAndNil(FNatsConn);
  FNatsConn := TNatsConnection.Create;
  FNatsConn.Name := 'JetStreamVCLDemo';
  FNatsConn.SetChannel(edtHost.Text, StrToIntDef(edtPort.Text, Nats.Consts.NATS_PORT), 5000);
  FNatsConn.ConnectOptions.verbose := False; // Set as needed

  try
    FNatsConn.Open(NATSConnectHandler, NATSDisconnectHandler);
    // Connection status will be updated by NATSConnectHandler
  except
    on E: Exception do
    begin
      Log('Error initiating NATS connection: ' + E.Message);
      sbMain.SimpleText := 'Connection Error';
      FreeAndNil(FNatsConn);
    end;
  end;
end;

procedure TfrmJetStreamVCLDemo.btnDisconnectClick(Sender: TObject);
begin
  if FIsConnected and Assigned(FNatsConn) then
  begin
    Log('Disconnecting from NATS...');
    FNatsConn.Close; // This should trigger NATSDisconnectHandler
    // NATSDisconnectHandler will update FIsConnected and UI
  end;
end;

procedure TfrmJetStreamVCLDemo.btnGetAccountInfoClick(Sender: TObject);
var
  LAccInfoResp: TJSAccountInfoResponse;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  Log('Getting JetStream Account Info...');
  mmAccountInfo.Clear;
  LAccInfoResp := TJSAccountInfoResponse.Create;
  try
    if FJSContext.GetAccountInfo(LAccInfoResp) then
    begin
      if LAccInfoResp.HasError then
      begin
        Log('Error getting account info: (' + LAccInfoResp.error.err_code.ToString + ') ' + LAccInfoResp.error.description);
        mmAccountInfo.Lines.Add('Error: ' + LAccInfoResp.error.description);
      end
      else
      begin
        Log('Account Info Retrieved.');
        mmAccountInfo.Lines.Add('Memory: ' + LAccInfoResp.memory.ToString + ' bytes');
        mmAccountInfo.Lines.Add('Storage: ' + LAccInfoResp.storage.ToString + ' bytes');
        mmAccountInfo.Lines.Add('Streams: ' + LAccInfoResp.streams.ToString);
        mmAccountInfo.Lines.Add('Consumers: ' + LAccInfoResp.consumers.ToString);
        mmAccountInfo.Lines.Add('Domain: ' + LAccInfoResp.domain);
        // Add more details from LAccInfoResp.limits etc. if populated
      end;
    end
    else
      Log('API call to GetAccountInfo failed or timed out.');
  finally
    LAccInfoResp.Free;
  end;
end;

procedure TfrmJetStreamVCLDemo.btnListStreamsClick(Sender: TObject);
var
  LStreamNamesResp: TJSStreamNamesResponse;
  S: string;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  Log('Listing JetStream Streams...');
  lbStreams.Clear;
  ClearStreamDetails;

  LStreamNamesResp := TJSStreamNamesResponse.Create;
  try
    if FJSContext.ListStreams(LStreamNamesResp) then
    begin
      if LStreamNamesResp.HasError then
        Log('Error listing streams: ' + LStreamNamesResp.error.description)
      else
      begin
        Log(Format('Found %d streams.', [Length(LStreamNamesResp.streams)]));
        for S in LStreamNamesResp.streams do
          lbStreams.Items.Add(S);
      end;
    end
    else
      Log('API call to ListStreams failed or timed out.');
  finally
    LStreamNamesResp.Free;
  end;
  UpdateUIConnectedState;
end;

procedure TfrmJetStreamVCLDemo.btnStreamCreateClick(Sender: TObject);
var
  LStreamConfig: TJSStreamConfig;
  LStreamCreateResp: TJSStreamCreateResponse;
  LSubjectsArray: TArray<string>;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  if (edtStreamNameCreate.Text = '') or (edtStreamSubjectsCreate.Text = '') then
  begin
    Log('Stream Name and Subjects cannot be empty for creation.');
    Exit;
  end;

  Log('Creating stream: ' + edtStreamNameCreate.Text);
  LStreamConfig.name := edtStreamNameCreate.Text;
  LSubjectsArray := edtStreamSubjectsCreate.Text.Split([',']);
  for var I := Low(LSubjectsArray) to High(LSubjectsArray) do
    LSubjectsArray[I] := Trim(LSubjectsArray[I]);
  LStreamConfig.subjects := LSubjectsArray;
  LStreamConfig.storage := TStorageType.stFile; // Default, could add UI for this
  LStreamConfig.retention := TRetentionPolicy.rpLimits;
  LStreamConfig.max_msgs := 100000;
  LStreamConfig.num_replicas := 1;
  LStreamConfig.duplicate_window := 2 * 60 * 1000 * 1000 * 1000; // 2 minutes in ns

  LStreamCreateResp := TJSStreamCreateResponse.Create;
  try
    if FJSContext.CreateStream(LStreamConfig, LStreamCreateResp) then
    begin
      if LStreamCreateResp.HasError then
        Log(Format('Error creating stream "%s": (%d) %s', [LStreamConfig.name, LStreamCreateResp.error.err_code, LStreamCreateResp.error.description]))
      else
      begin
        Log('Stream "' + LStreamConfig.name + '" created successfully.');
        btnListStreamsClick(nil); // Refresh list
      end;
    end
    else
      Log('API call to CreateStream for "' + LStreamConfig.name + '" failed.');
  finally
    LStreamCreateResp.Free;
  end;
end;

procedure TfrmJetStreamVCLDemo.btnStreamInfoClick(Sender: TObject);
var
  LStreamName: string;
  LStreamInfoResp: TJSStreamInfoResponse;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  LStreamName := GetSelectedStreamName;
  if LStreamName = '' then
  begin
    Log('No stream selected to get info.');
    Exit;
  end;

  Log('Getting info for stream: ' + LStreamName);
  mmStreamInfo.Clear;
  LStreamInfoResp := TJSStreamInfoResponse.Create;
  try
    if FJSContext.GetStreamInfo(LStreamName, LStreamInfoResp) then
    begin
      if LStreamInfoResp.HasError then
      begin
        Log(Format('Error getting info for stream "%s": (%d) %s', [LStreamName, LStreamInfoResp.error.err_code, LStreamInfoResp.error.description]));
        mmStreamInfo.Lines.Add('Error: ' + LStreamInfoResp.error.description);
      end
      else
      begin
        Log('Stream Info for "' + LStreamName + '" retrieved.');
        mmStreamInfo.Lines.Add('Name: ' + LStreamInfoResp.config.name);
        mmStreamInfo.Lines.Add('Subjects: ' + string.Join(', ', LStreamInfoResp.config.subjects));
        mmStreamInfo.Lines.Add('Retention: ' + RetentionPolicyStrings[LStreamInfoResp.config.retention]);
        mmStreamInfo.Lines.Add('Storage: ' + StorageTypeStrings[LStreamInfoResp.config.storage]);
        mmStreamInfo.Lines.Add('Messages: ' + LStreamInfoResp.state.messages.ToString);
        mmStreamInfo.Lines.Add('Bytes: ' + LStreamInfoResp.state.bytes.ToString);
        mmStreamInfo.Lines.Add('First Seq: ' + LStreamInfoResp.state.first_seq.ToString);
        mmStreamInfo.Lines.Add('Last Seq: ' + LStreamInfoResp.state.last_seq.ToString);
        mmStreamInfo.Lines.Add('Created: ' + LStreamInfoResp.created);
      end;
    end
    else
      Log('API call to GetStreamInfo for "' + LStreamName + '" failed.');
  finally
    LStreamInfoResp.Free;
  end;
end;

procedure TfrmJetStreamVCLDemo.btnStreamDeleteClick(Sender: TObject);
var
  LStreamName: string;
  LStreamDeleteResp: TJSStreamDeleteResponse;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  LStreamName := GetSelectedStreamName;
  if LStreamName = '' then
  begin
    Log('No stream selected to delete.');
    Exit;
  end;
  if MessageDlg('Are you sure you want to delete stream "' + LStreamName + '"?', mtConfirmation, [mbYes, mbNo], 0) <> mrYes then
    Exit;

  Log('Deleting stream: ' + LStreamName);
  LStreamDeleteResp := TJSStreamDeleteResponse.Create;
  try
    if FJSContext.DeleteStream(LStreamName, LStreamDeleteResp) then
    begin
      if LStreamDeleteResp.HasError then
        Log(Format('Error deleting stream "%s": %s', [LStreamName, LStreamDeleteResp.error.description]))
      else if LStreamDeleteResp.success then
      begin
        Log('Stream "' + LStreamName + '" deleted successfully.');
        btnListStreamsClick(nil); // Refresh list
        ClearStreamDetails;
      end
      else
        Log('DeleteStream for "' + LStreamName + '" did not report success.');
    end
    else
      Log('API call to DeleteStream failed.');
  finally
    LStreamDeleteResp.Free;
  end;
end;

procedure TfrmJetStreamVCLDemo.btnPublishClick(Sender: TObject);
var
  LPubAck: TJSPubAck;
  LPublishOpts: TJetStreamPublishOptions;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  if (edtPublishSubject.Text = '') or (edtPublishMessage.Text = '') then
  begin
    Log('Publish Subject and Message cannot be empty.');
    Exit;
  end;

  Log('Publishing to ' + edtPublishSubject.Text);
  mmPubAck.Clear;
  LPubAck := TJSPubAck.Create;
  LPublishOpts.Create; // Use record constructor
  try
    LPublishOpts.ExpectedStream := edtPublishExpectedStream.Text; // Optional
    // LPublishOpts.MsgID := TGuid.NewGuid.ToString; // Optional

    if GJSContext.Publish(edtPublishSubject.Text, edtPublishMessage.Text, LPubAck, LPublishOpts) then
    begin
      if LPubAck.HasError then
      begin
        Log(Format('Error in PubAck: (%d) %s', [LPubAck.error.code, LPubAck.error.description]));
        mmPubAck.Lines.Add('Error: ' + LPubAck.error.description);
      end
      else
      begin
        Log(Format('Message published. Stream: %s, Seq: %d, Duplicate: %s',
          [LPubAck.stream, LPubAck.seq, BoolToStr(LPubAck.duplicate, True)]));
        mmPubAck.Lines.Add('Stream: ' + LPubAck.stream);
        mmPubAck.Lines.Add('Sequence: ' + LPubAck.seq.ToString);
        mmPubAck.Lines.Add('Duplicate: ' + BoolToStr(LPubAck.duplicate, True));
      end;
    end
    else
      Log('Publish call failed or timed out.');
  finally
    LPubAck.Free;
    LPublishOpts.Destroy; // Use record destructor
  end;
end;

procedure TfrmJetStreamVCLDemo.btnListConsumersClick(Sender: TObject);
var
  LConsumerNamesResp: TJSConsumerNamesResponse;
  S: string;
  LStreamName: string;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  LStreamName := Trim(edtConsumerStreamNameList.Text);
  if LStreamName = '' then
  begin
    Log('Please enter a Stream Name to list consumers.');
    Exit;
  end;

  Log('Listing consumers for stream: ' + LStreamName);
  lbConsumers.Clear;
  ClearConsumerDetails;

  LConsumerNamesResp := TJSConsumerNamesResponse.Create;
  try
    if FJSContext.ListConsumers(LStreamName, LConsumerNamesResp) then
    begin
      if LConsumerNamesResp.HasError then
        Log('Error listing consumers: ' + LConsumerNamesResp.error.description)
      else
      begin
        Log(Format('Found %d consumers for stream "%s":', [Length(LConsumerNamesResp.consumers), LStreamName]));
        for S in LConsumerNamesResp.consumers do
          lbConsumers.Items.Add(S + ' (on ' + LStreamName + ')'); // Store stream name for context
      end;
    end
    else
      Log('API call to ListConsumers failed.');
  finally
    LConsumerNamesResp.Free;
  end;
  UpdateUIConnectedState;
end;

procedure TfrmJetStreamVCLDemo.btnConsumerCreateClick(Sender: TObject);
var
  LConfig: TJSConsumerConfig;
  LResponse: TJSConsumerCreateResponse;
  LStreamName, LConsumerName: string;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  LStreamName := Trim(edtConsumerStreamNameCreate.Text);
  LConsumerName := Trim(edtConsumerNameCreate.Text); // This will be durable name

  if (LStreamName = '') or (LConsumerName = '') then
  begin
    Log('Stream Name and Consumer (Durable) Name are required for creation.');
    Exit;
  end;

  Log('Creating pull consumer "' + LConsumerName + '" on stream "' + LStreamName + '"');
  LConfig.durable_name := LConsumerName;
  LConfig.ack_policy := TAckPolicy.apExplicit;
  LConfig.deliver_policy := TDeliverPolicy.dpAll; // Default
  // LConfig.filter_subject := 'specific.subject'; // Optional filter

  LResponse := TJSConsumerCreateResponse.Create;
  try
    if FJSContext.CreateConsumer(LStreamName, LConfig, LResponse) then
    begin
      if LResponse.HasError then
        Log(Format('Error creating consumer "%s": (%d) %s', [LConsumerName, LResponse.error.err_code, LResponse.error.description]))
      else
      begin
        Log('Consumer "' + LResponse.name + '" created successfully.');
        // Refresh list for the current stream if it matches
        if SameText(LStreamName, edtConsumerStreamNameList.Text) then
          btnListConsumersClick(nil);
      end;
    end
    else
      Log('API call to CreateConsumer failed.');
  finally
    LResponse.Free;
  end;
end;

procedure TfrmJetStreamVCLDemo.btnConsumerInfoClick(Sender: TObject);
var
  LStreamName, LConsumerName: string;
  LResponse: TJSConsumerInfoResponse;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  LConsumerName := GetSelectedConsumerName(LStreamName);
  if (LConsumerName = '') or (LStreamName = '') then
  begin
    Log('No consumer selected or stream context missing.');
    Exit;
  end;

  Log('Getting info for consumer: ' + LConsumerName + ' on stream ' + LStreamName);
  mmConsumerInfo.Clear;
  LResponse := TJSConsumerInfoResponse.Create;
  try
    if FJSContext.GetConsumerInfo(LStreamName, LConsumerName, LResponse) then
    begin
      if LResponse.HasError then
      begin
        Log(Format('Error getting info for consumer "%s": (%d) %s', [LConsumerName, LResponse.error.err_code, LResponse.error.description]));
        mmConsumerInfo.Lines.Add('Error: ' + LResponse.error.description);
      end
      else
      begin
        Log('Consumer Info for "' + LResponse.name + '" retrieved.');
        mmConsumerInfo.Lines.Add('Name: ' + LResponse.name);
        mmConsumerInfo.Lines.Add('Stream: ' + LResponse.stream_name);
        mmConsumerInfo.Lines.Add('Durable: ' + LResponse.config.durable_name);
        mmConsumerInfo.Lines.Add('Ack Policy: ' + AckPolicyStrings[LResponse.config.ack_policy]);
        mmConsumerInfo.Lines.Add('Deliver Policy: ' + DeliverPolicyStrings[LResponse.config.deliver_policy]);
        mmConsumerInfo.Lines.Add('Num Pending: ' + LResponse.num_pending.ToString);
        mmConsumerInfo.Lines.Add('Num Ack Pending: ' + LResponse.num_ack_pending.ToString);
        mmConsumerInfo.Lines.Add('Created: ' + LResponse.created);
      end;
    end
    else
      Log('API call to GetConsumerInfo failed.');
  finally
    LResponse.Free;
  end;
end;

procedure TfrmJetStreamVCLDemo.btnConsumerDeleteClick(Sender: TObject);
var
  LStreamName, LConsumerName: string;
  LResponse: TJSConsumerDeleteResponse;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  LConsumerName := GetSelectedConsumerName(LStreamName);
  if (LConsumerName = '') or (LStreamName = '') then
  begin
    Log('No consumer selected or stream context missing.');
    Exit;
  end;
  if MessageDlg('Are you sure you want to delete consumer "' + LConsumerName + '" on stream "' + LStreamName + '"?',
     mtConfirmation, [mbYes, mbNo], 0) <> mrYes then
    Exit;

  Log('Deleting consumer: ' + LConsumerName);
  LResponse := TJSConsumerDeleteResponse.Create;
  try
    if FJSContext.DeleteConsumer(LStreamName, LConsumerName, LResponse) then
    begin
      if LResponse.HasError then
        Log(Format('Error deleting consumer "%s": %s', [LConsumerName, LResponse.error.description]))
      else if LResponse.success then
      begin
        Log('Consumer "' + LConsumerName + '" deleted successfully.');
        if SameText(LStreamName, edtConsumerStreamNameList.Text) then
          btnListConsumersClick(nil); // Refresh list
        ClearConsumerDetails;
      end
      else
        Log('DeleteConsumer for "' + LConsumerName + '" did not report success.');
    end
    else
      Log('API call to DeleteConsumer failed.');
  finally
    LResponse.Free;
  end;
end;

procedure TfrmJetStreamVCLDemo.btnFetchMessagesClick(Sender: TObject);
var
  LStreamName, LConsumerName: string;
  LBatchSize: Integer;
  LExpiresNS: Int64;
  LMsg: TJSReceivedMessage;
  I: Integer;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  LStreamName := Trim(edtConsumeStream.Text);
  LConsumerName := Trim(edtConsumeConsumer.Text);
  LBatchSize := StrToIntDef(edtConsumeBatchSize.Text, 1);
  if LBatchSize <= 0 then LBatchSize := 1;

  if (LStreamName = '') or (LConsumerName = '') then
  begin
    Log('Stream and Consumer names are required to fetch messages.');
    Exit;
  end;

  ClearMessageDetails; // Clears FLastFetchedMessages and frees their headers
  Log(Format('Fetching %d messages from %s/%s...', [LBatchSize, LStreamName, LConsumerName]));

  // Example: Wait up to 5 seconds for messages (5 * 10^9 ns)
  LExpiresNS := 5 * 1000 * 1000 * 1000;

  // FLastFetchedMessages is already created. FetchMessages will clear and populate it.
  if GJSContext.FetchMessages(LStreamName, LConsumerName, LBatchSize, FLastFetchedMessages, LExpiresNS) then
  begin
    Log(Format('Fetch call completed. Received %d data messages:', [FLastFetchedMessages.Count]));
    if FLastFetchedMessages.Count = 0 then
    begin
      mmMessages.Lines.Add('No data messages available or fetch timed out server-side.');
      Log('  (Check logs for any control messages like 404 No Messages or 408 Timeout from server if headers were parsed)');
    end
    else
    begin
      for I := 0 to FLastFetchedMessages.Count - 1 do
      begin
        LMsg := FLastFetchedMessages[I]; // Access record from list
        mmMessages.Lines.Add(Format('Msg %d - Seq: %s, Subject: %s', [I + 1, LMsg.Sequence.ToString, LMsg.Subject]));
        mmMessages.Lines.Add('  Data: ' + TEncoding.UTF8.GetString(LMsg.Data));
        if Assigned(LMsg.Headers) and (LMsg.Headers.Count > 0) then
        begin
          mmMessages.Lines.Add('  Headers:');
          for var J := 0 to LMsg.Headers.Count - 1 do
             mmMessages.Lines.Add('    ' + LMsg.Headers.Names[J] + ': ' + LMsg.Headers.ValueFromIndex[J]);
        end;
        mmMessages.Lines.Add(''); // Separator
      end;
    end;
  end
  else
    Log('FetchMessages API call itself failed or timed out client-side.');

  btnAckSelected.Enabled := FLastFetchedMessages.Count > 0;
end;

procedure TfrmJetStreamVCLDemo.btnAckSelectedClick(Sender: TObject);
var
  LMsg: TJSReceivedMessage;
begin
  if not (FIsConnected and Assigned(FJSContext)) then Exit;
  if FLastFetchedMessages.Count = 0 then
  begin
    Log('No messages fetched to acknowledge.');
    Exit;
  end;

  Log(Format('Acking %d fetched messages...', [FLastFetchedMessages.Count]));
  // For simplicity, ack all fetched messages. A real UI might allow selective ack.
  for LMsg in FLastFetchedMessages do
  begin
    if LMsg.ReplyTo <> '' then
    begin
      Log('  Acking message Stream: ' + LMsg.Stream + ', Seq: ' + LMsg.Sequence.ToString);
      GJSContext.Ack(LMsg); // Uses ReplyTo from TJSReceivedMessage
    end
    else
      Log('  Message Stream: ' + LMsg.Stream + ', Seq: ' + LMsg.Sequence.ToString + ' has no ReplyTo, cannot ACK.');
  end;

  // Clear messages after attempting to ack them
  ClearMessageDetails;
  Log('Messages processed and cleared.');
end;

procedure TfrmJetStreamVCLDemo.lbStreamsClick(Sender: TObject);
begin
  ClearStreamDetails;
  edtStreamNameDetail.Text := GetSelectedStreamName;
  btnStreamInfo.Enabled := FIsConnected and Assigned(FJSContext) and (edtStreamNameDetail.Text <> '');
  btnStreamDelete.Enabled := btnStreamInfo.Enabled;
  // Auto-populate stream name for consumer list and creation
  edtConsumerStreamNameList.Text := edtStreamNameDetail.Text;
  edtConsumerStreamNameCreate.Text := edtStreamNameDetail.Text;
  edtConsumeStream.Text := edtStreamNameDetail.Text;
end;

procedure TfrmJetStreamVCLDemo.lbConsumersClick(Sender: TObject);
var
  LStreamName, LConsumerName: string;
begin
  ClearConsumerDetails;
  LConsumerName := GetSelectedConsumerName(LStreamName);
  edtConsumerStreamNameDetail.Text := LStreamName;
  edtConsumerNameDetail.Text := LConsumerName;
  btnConsumerInfo.Enabled := FIsConnected and Assigned(FJSContext) and (LConsumerName <> '');
  btnConsumerDelete.Enabled := btnConsumerInfo.Enabled;
  // Auto-populate for consumption
  edtConsumeStream.Text := LStreamName;
  edtConsumeConsumer.Text := LConsumerName;
end;

initialization
  // Define NATS_HOST, NATS_PORT, NATS_TIMEOUT in Nats.Consts.pas if not already there
  // Example in Nats.Consts.pas:
  // const
  //   NATS_HOST = 'localhost';
  //   NATS_PORT = 4222;
  //   NATS_TIMEOUT = 5000; // Default timeout for some operations

end.
