unit NATS.JetStream.Client;

interface

uses
  System.SysUtils, System.Classes, System.SyncObjs, System.Generics.Collections,
  System.DateUtils, System.StrUtils,
  Nats.Classes,
  Nats.Connection,
  Nats.JetStream.Enums,
  NATS.JetStream.Entities;

type
  TJetStreamPublishOptions = record
    MsgID: string;
    ExpectedStream: string;
    ExpectedLastSeq: UInt64;
    ExpectedLastMsgID: string;
    CustomHeaders: TStringList;

    class operator Initialize (out Dest: TJetStreamPublishOptions);
    class operator Finalize (var Dest: TJetStreamPublishOptions);
  end;

  TJetStreamContext = class
  private
    FNatsConn: TNatsConnection;
    FDefaultTimeoutMS: Cardinal;
    FClientID: string;

    function DoRequest(const ASubject: string; const ARequestPayload: string;
      var AResponsePayload: string; ATimeoutMS: Cardinal): Boolean;

    // Updated DoApiRequest to handle class types for TResponse
    function DoApiRequest<TRequest: record; TResponse: TJetStreamBaseResponse, constructor>(const ASubject: string;
      const ARequestData: TRequest; var AResponseData: TResponse; ATimeoutMS: Cardinal): Boolean; overload;

    function DoApiRequest<TResponse: TJetStreamBaseResponse, constructor>(const ASubject: string;
      const ARequestJson: string; var AResponseData: TResponse; ATimeoutMS: Cardinal): Boolean; overload;

    function DoApiRequest<TResponse: TJetStreamBaseResponse, constructor>(const ASubject: string; // For requests with empty body
      var AResponseData: TResponse; ATimeoutMS: Cardinal): Boolean; overload;

    function FetchNextMessagesInternal(const AStreamName, AConsumerName: string;
      const ARequestData: TJSMessageGetRequest;
      var AMessages: TList<TJSReceivedMessage>;
      ATimeoutMS: Cardinal): Boolean;

    function ToJSReceivedMessage(const ANatsMsg: TNatsArgsMSG): TJSReceivedMessage;

  public
    constructor Create(ANatsConnection: TNatsConnection; ADefaultTimeoutMS: Cardinal = 5000);

    function GetAccountInfo(var AAccountInfo: TJSAccountInfoResponse): Boolean;
    function CreateStream(const AConfig: TJSStreamConfig; var AResponse: TJSStreamCreateResponse): Boolean;
    function UpdateStream(const AConfig: TJSStreamConfig; var AResponse: TJSStreamCreateResponse): Boolean;
    function GetStreamInfo(const AStreamName: string; var AResponse: TJSStreamInfoResponse): Boolean;
    function DeleteStream(const AStreamName: string; var AResponse: TJSStreamDeleteResponse): Boolean;
    function ListStreams(var AResponse: TJSStreamNamesResponse; const ADomain: string = ''): Boolean;

    function Publish(const ASubject: string; const AMessage: string;
                     var APubAck: TJSPubAck; AOptions: TJetStreamPublishOptions): Boolean; overload;
    function Publish(const ASubject: string; const AMessage: string;
                     var APubAck: TJSPubAck): Boolean; overload;

    function PublishBytes(const ASubject: string; const AData: TBytes;
                          var APubAck: TJSPubAck; AOptions: TJetStreamPublishOptions): Boolean; overload;
    function PublishBytes(const ASubject: string; const AData: TBytes;
                          var APubAck: TJSPubAck): Boolean; overload;

    function CreateConsumer(const AStreamName: string; const AConfig: TJSConsumerConfig; var AResponse: TJSConsumerCreateResponse): Boolean;
    function GetConsumerInfo(const AStreamName, AConsumerName: string; var AResponse: TJSConsumerInfoResponse): Boolean;
    function DeleteConsumer(const AStreamName, AConsumerName: string; var AResponse: TJSConsumerDeleteResponse): Boolean;
    function ListConsumers(const AStreamName: string; var AResponse: TJSConsumerNamesResponse): Boolean;

    function FetchMessages(const AStreamName, AConsumerName: string; ABatchSize: Integer;
                           var AMessages: TList<TJSReceivedMessage>;
                           AExpiresNS: Int64 = 0;
                           AMaxBytes: Integer = 0;
                           ANoWait: Boolean = False): Boolean;

    procedure Ack(const AMessage: TJSReceivedMessage; const APayload: string = '+ACK'); overload;
    procedure Ack(const AReplyToSubject: string; const APayload: string = '+ACK'); overload;

    property DefaultTimeoutMS: Cardinal read FDefaultTimeoutMS write FDefaultTimeoutMS;
  end;

implementation

uses
  System.JSON,Rest.Json, Nats.Nuid, Nats.Consts, Nats.Exceptions,Math,
  Nats.Json.Utils;



{ TJetStreamContext }

constructor TJetStreamContext.Create(ANatsConnection: TNatsConnection; ADefaultTimeoutMS: Cardinal);
begin
  inherited Create;
  if not Assigned(ANatsConnection) then
    raise EArgumentNilException.Create('ANatsConnection cannot be nil for TJetStreamContext.');
  FNatsConn := ANatsConnection;
  FDefaultTimeoutMS := ADefaultTimeoutMS;
  FClientID := 'js-client-' + TGuid.NewGuid.ToString.Substring(0,4);
end;

function TJetStreamContext.DoApiRequest<TRequest, TResponse>(
  const ASubject: string; const ARequestData: TRequest;
  var AResponseData: TResponse; ATimeoutMS: Cardinal): Boolean;
var
  LRequestJson, LResponseJson: string;
begin
  Result := False;
  LRequestJson := TNatsJsonUtils.RecordToJson(ARequestData);// TRequest is still a record
  //LRequestJson := TJson.ObjectToJsonString(ARequestData);

  // AResponseData (class) must be created by the caller of DoApiRequest's public API methods
  // or we create it here and the caller must free it.
  // For var parameter, caller should create and pass it.
  // If AResponseData is nil, we cannot deserialize into it.
  if not Assigned(AResponseData) then
    AResponseData := TResponse.Create; // Ensure instance exists if passed as nil

  if DoRequest(ASubject, LRequestJson, LResponseJson, ATimeoutMS) then
  begin
    if LResponseJson = '' then
      raise ENatsException.CreateFmt('Empty response payload from JetStream API: %s', [ASubject]);
    try
      // TNatsJsonUtils.JsonToObject will populate the fields of the existing AResponseData instance.
      TNatsJsonUtils.JsonToObject(LResponseJson,AResponseData);

      // Check for API-level error within the successfully deserialized response
      // AResponseData is TJetStreamBaseResponse or descendant
      if AResponseData.HasError then
      begin
        Result := False; // JS API returned an error
      end
      else
        Result := True; // No JS API error
    except
      on E: Exception do
        raise ENatsException.CreateFmt('Failed to process JetStream API response from %s: %s. JSON: %s', [ASubject, E.Message, LResponseJson]);
    end;
  end;
  // If DoRequest returned false (timeout or NATS error), Result is already false.
  // If Result is false here, and AResponseData was created inside this function, it might leak.
  // It's better if the public API methods manage creation/freeing of response objects.

end;

function TJetStreamContext.DoApiRequest<TResponse>(const ASubject,
  ARequestJson: string; var AResponseData: TResponse;
  ATimeoutMS: Cardinal): Boolean;
var
  LResponseJson: string;
begin
  Result := False;


  // AResponseData (class) must be created by the caller of DoApiRequest's public API methods
  // or we create it here and the caller must free it.
  // For var parameter, caller should create and pass it.
  // If AResponseData is nil, we cannot deserialize into it.
  if not Assigned(AResponseData) then
    AResponseData := TResponse.Create; // Ensure instance exists if passed as nil

  if DoRequest(ASubject, ARequestJson, LResponseJson, ATimeoutMS) then
  begin
    if LResponseJson = '' then
      raise ENatsException.CreateFmt('Empty response payload from JetStream API: %s', [ASubject]);
    try
      // TNatsJsonUtils.JsonToObject will populate the fields of the existing AResponseData instance.
      TNatsJsonUtils.JsonToObject(LResponseJson,AResponseData);

      // Check for API-level error within the successfully deserialized response
      // AResponseData is TJetStreamBaseResponse or descendant
      if AResponseData.HasError then
      begin
        Result := False; // JS API returned an error
      end
      else
        Result := True; // No JS API error
    except
      on E: Exception do
        raise ENatsException.CreateFmt('Failed to process JetStream API response from %s: %s. JSON: %s', [ASubject, E.Message, LResponseJson]);
    end;
  end;
  // If DoRequest returned false (timeout or NATS error), Result is already false.
  // If Result is false here, and AResponseData was created inside this function, it might leak.
  // It's better if the public API methods manage creation/freeing of response objects.


end;

function TJetStreamContext.DoApiRequest<TResponse>(const ASubject: string;
  var AResponseData: TResponse; ATimeoutMS: Cardinal): Boolean;
var
  LResponseJson: string;
begin
  Result := False;
  if not Assigned(AResponseData) then
    AResponseData := TResponse.Create;

  if DoRequest(ASubject, '{}', LResponseJson, ATimeoutMS) then // Empty JSON for no-body requests
  begin
    if LResponseJson = '' then
      raise ENatsException.CreateFmt('Empty response payload from JetStream API (no body req): %s', [ASubject]);
    try
      TNatsJsonUtils.JsonToObject(LResponseJson,AResponseData);
      //TJson.JsonToObject(AResponseData, LResponseJson);
      if AResponseData.HasError then
         Result := False
      else
        Result := True;
    except
      on E: Exception do
        raise ENatsException.CreateFmt('Failed to process JetStream API response from %s (no body req): %s. JSON: %s', [ASubject, E.Message, LResponseJson]);
    end;
  end;

end;

function TJetStreamContext.DoRequest(const ASubject: string; const ARequestPayload: string;
  var AResponsePayload: string; ATimeoutMS: Cardinal): Boolean;
var
  LReplySubject: string;
  LSubscriptionId: Integer;
  LResponseEvent: TEvent;
  LReceivedNatsMsg: TNatsArgsMSG;
  LSuccess: Boolean;
  LHandler: TNatsMsgHandler;
  LStartTime: TDateTime;
begin
  Result := False;
  AResponsePayload := '';
  if not FNatsConn.Connected then
    raise ENatsException.Create('NATS not connected for JetStream request.');

  LReplySubject := FNatsConn.GetNewInbox;
  LResponseEvent := TEvent.Create(nil, True, False, '', False);
  LSuccess := False;

  LHandler := procedure(const AMsg: TNatsArgsMSG)
  begin
    LReceivedNatsMsg := AMsg;
    LSuccess := True;
    LResponseEvent.SetEvent;
  end;

  try
    LSubscriptionId := FNatsConn.Subscribe(LReplySubject, LHandler);
    FNatsConn.Unsubscribe(LSubscriptionId, 1);

    FNatsConn.Publish(ASubject, ARequestPayload, LReplySubject);

    LStartTime := Now;
    while True do
    begin
      case LResponseEvent.WaitFor(100) of
        wrSignaled:
          begin
            if LSuccess then
            begin
              AResponsePayload := LReceivedNatsMsg.Payload;
              Result := True;
            end else Result := False;
            Break;
          end;
        wrTimeout:
          begin
            if MilliSecondsBetween(Now, LStartTime) > ATimeoutMS then
            begin
              FNatsConn.Unsubscribe(LSubscriptionId, 0);
              raise ENatsException.CreateFmt('JetStream API request to %s timed out after %dms.', [ASubject, ATimeoutMS]);
            end;
          end;
        wrError:
          raise ENatsException.Create('Error waiting for JetStream API response event.');
        else Break; // Should not happen
      end;
    end;
  finally
    LResponseEvent.Free;
  end;
end;

function TJetStreamContext.ToJSReceivedMessage(const ANatsMsg: TNatsArgsMSG): TJSReceivedMessage;
begin
    Result.Subject := ANatsMsg.Subject;
    Result.ReplyTo := ANatsMsg.ReplyTo;
    Result.Data := TEncoding.UTF8.GetBytes(ANatsMsg.Payload);

    if Assigned(ANatsMsg.Headers) then
    begin
      Result.Headers.AddStrings(ANatsMsg.Headers); // ANatsMsg.Headers is TStringList
      Result.Stream := Result.GetHeader('Nats-Stream');
      Result.Sequence := StrToIntDef(Result.GetHeader('Nats-Sequence'), 0);
      Result.ConsumerSequence := StrToIntDef(Result.GetHeader('Nats-Consumer-Seq'), StrToIntDef(Result.GetHeader('Nats-Consumer-Sequence'),0));
      Result.Timestamp := StrToInt64Def(Result.GetHeader('Nats-Time'), 0);
      Result.NumPending := StrToIntDef(Result.GetHeader('Nats-Pending-Messages'), StrToIntDef(Result.GetHeader('Nats-Pending'), -1));
      Result.Domain := Result.GetHeader('Nats-Domain');
    end;
end;

{ Account Info }
function TJetStreamContext.GetAccountInfo(var AAccountInfo: TJSAccountInfoResponse): Boolean;
const SubjectAccountInfo = JS_API_PREFIX + 'INFO';
begin
  // Caller must create AAccountInfo instance and free it.
  // Example: LAccInfo := TJSAccountInfoResponse.Create; try ... finally LAccInfo.Free;
  Result := DoApiRequest<TJSAccountInfoResponse>(SubjectAccountInfo, AAccountInfo, FDefaultTimeoutMS);
  if Result and AAccountInfo.HasError then Result := False;
end;

{ Stream Management }
function TJetStreamContext.CreateStream(const AConfig: TJSStreamConfig; var AResponse: TJSStreamCreateResponse): Boolean;
var LSubject: string;
begin
  LSubject := Format(JS_API_PREFIX + 'STREAM.CREATE.%s', [AConfig.name]);
  Result := DoApiRequest(LSubject, AConfig.ToJsonString, AResponse, FDefaultTimeoutMS);
  if Result and AResponse.HasError then Result := False;
end;

function TJetStreamContext.UpdateStream(const AConfig: TJSStreamConfig; var AResponse: TJSStreamCreateResponse): Boolean;
var LSubject: string;
begin
  LSubject := Format(JS_API_PREFIX + 'STREAM.UPDATE.%s', [AConfig.name]);
  Result := DoApiRequest<TJSStreamCreateResponse>(LSubject, AConfig.ToJsonString, AResponse, FDefaultTimeoutMS);
  if Result and AResponse.HasError then Result := False;
end;

function TJetStreamContext.GetStreamInfo(const AStreamName: string; var AResponse: TJSStreamInfoResponse): Boolean;
var LSubject: string;
begin
  LSubject := Format(JS_API_PREFIX + 'STREAM.INFO.%s', [AStreamName]);
  Result := DoApiRequest<TJSStreamInfoResponse>(LSubject, AResponse, FDefaultTimeoutMS);
  if Result and AResponse.HasError then Result := False;
end;

function TJetStreamContext.DeleteStream(const AStreamName: string; var AResponse: TJSStreamDeleteResponse): Boolean;
var LSubject: string;
begin
  LSubject := Format(JS_API_PREFIX + 'STREAM.DELETE.%s', [AStreamName]);
  Result := DoApiRequest<TJSStreamDeleteResponse>(LSubject, AResponse, FDefaultTimeoutMS);
  if Result and AResponse.HasError then Result := False;
end;

function TJetStreamContext.ListStreams(var AResponse: TJSStreamNamesResponse; const ADomain: string = ''): Boolean;
var LSubject: string;
begin
  LSubject := JS_API_PREFIX + 'STREAM.NAMES';
  if ADomain <> '' then
    LSubject := Format('$JS.%s.API.STREAM.NAMES', [ADomain]);
  Result := DoApiRequest<TJSStreamNamesResponse>(LSubject, AResponse, FDefaultTimeoutMS);
  if Result and AResponse.HasError then Result := False;
end;

{ Publishing }
function TJetStreamContext.Publish(const ASubject: string; const AMessage: string;
  var APubAck: TJSPubAck; AOptions: TJetStreamPublishOptions): Boolean;
var
  LReplySubject: string;
  LSubscriptionId: Integer;
  LResponseEvent: TEvent;
  LReceivedNatsMsg: TNatsArgsMSG;
  LSuccess: Boolean;
  LHandler: TNatsMsgHandler;
  LStartTime: TDateTime;
  LFinalHeaders: TStringList;
begin
  Result := False;
  if not FNatsConn.Connected then
    raise ENatsException.Create('NATS not connected for JetStream publish.');
  if not Assigned(APubAck) then APubAck := TJSPubAck.Create; // Ensure instance

  LReplySubject := FNatsConn.GetNewInbox;
  LResponseEvent := TEvent.Create(nil, True, False, '', False);
  LSuccess := False;
  LFinalHeaders := TStringList.Create;
  LFinalHeaders.CaseSensitive := False;

  try
    if Assigned(AOptions.CustomHeaders) then
      LFinalHeaders.AddStrings(AOptions.CustomHeaders);
    if AOptions.MsgID <> '' then LFinalHeaders.Values['Nats-Msg-Id'] := AOptions.MsgID;
    if AOptions.ExpectedStream <> '' then LFinalHeaders.Values['Nats-Expected-Stream'] := AOptions.ExpectedStream;
    if AOptions.ExpectedLastSeq > 0 then LFinalHeaders.Values['Nats-Expected-Last-Sequence'] := AOptions.ExpectedLastSeq.ToString;
    if AOptions.ExpectedLastMsgID <> '' then LFinalHeaders.Values['Nats-Expected-Last-Msg-ID'] := AOptions.ExpectedLastMsgID;

    LHandler := procedure(const AMsg: TNatsArgsMSG)
    begin
      LReceivedNatsMsg := AMsg;
      LSuccess := True;
      LResponseEvent.SetEvent;
    end;

    LSubscriptionId := FNatsConn.Subscribe(LReplySubject, LHandler);
    FNatsConn.Unsubscribe(LSubscriptionId, 1);

    FNatsConn.Publish(ASubject, AMessage, LReplySubject, LFinalHeaders);

    LStartTime := Now;
    while True do
    begin
      case LResponseEvent.WaitFor(FDefaultTimeoutMS) of
        wrSignaled:
          begin
            if LSuccess then
            begin
              try
                // Populate existing APubAck
                if Assigned(APubAck) then APubAck.FromJson(LReceivedNatsMsg.Payload)
                else
                  APubAck:=TJSPubAck.FromJsonString(LReceivedNatsMsg.Payload);
                Result := not APubAck.HasError;
                if Result and Assigned(LReceivedNatsMsg.Headers) and (LReceivedNatsMsg.Headers.Values['Status'] = '503') then
                begin
                    if not APubAck.HasError then APubAck.error.code := 503;
                    if APubAck.error.description = '' then APubAck.error.description := LReceivedNatsMsg.Headers.Values['Description'];
                    Result := False;
                end;
              except
                on E: Exception do
                  raise ENatsException.CreateFmt('Failed to parse JetStream PubAck from %s: %s. JSON: %s', [ASubject, E.Message, LReceivedNatsMsg.Payload]);
              end;
            end else Result := False;
            Break;
          end;
        wrTimeout:
          begin
            FNatsConn.Unsubscribe(LSubscriptionId, 0);
            raise ENatsException.CreateFmt('JetStream publish to %s timed out waiting for Ack.', [ASubject]);
          end;
        else
          raise ENatsException.Create('Error waiting for JetStream publish Ack event.');
      end;
    end;
  finally
    LResponseEvent.Free;
    LFinalHeaders.Free;
  end;
end;

function TJetStreamContext.Publish(const ASubject: string; const AMessage: string;
  var APubAck: TJSPubAck): Boolean;
var
  LOptions: TJetStreamPublishOptions;
begin
  Result := Publish(ASubject, AMessage, APubAck, LOptions);
end;

function TJetStreamContext.PublishBytes(const ASubject: string; const AData: TBytes;
  var APubAck: TJSPubAck; AOptions: TJetStreamPublishOptions): Boolean;
var
  LReplySubject: string;
  LSubscriptionId: Integer;
  LResponseEvent: TEvent;
  LReceivedNatsMsg: TNatsArgsMSG;
  LSuccess: Boolean;
  LHandler: TNatsMsgHandler;
  LStartTime: TDateTime;
  LFinalHeaders: TStringList;
begin
  Result := False;
  if not FNatsConn.Connected then
    raise ENatsException.Create('NATS not connected for JetStream publish (bytes).');
  if not Assigned(APubAck) then APubAck := TJSPubAck.Create;

  LReplySubject := FNatsConn.GetNewInbox;
  LResponseEvent := TEvent.Create(nil, True, False, '', False);
  LSuccess := False;
  LFinalHeaders := TStringList.Create;
  LFinalHeaders.CaseSensitive := False;
  try
    if Assigned(AOptions.CustomHeaders) then LFinalHeaders.AddStrings(AOptions.CustomHeaders);
    if AOptions.MsgID <> '' then LFinalHeaders.Values['Nats-Msg-Id'] := AOptions.MsgID;
    if AOptions.ExpectedStream <> '' then LFinalHeaders.Values['Nats-Expected-Stream'] := AOptions.ExpectedStream;
    // ... other headers
    LHandler := procedure(const AMsg: TNatsArgsMSG)
    begin
      LReceivedNatsMsg := AMsg;
      LSuccess := True;
      LResponseEvent.SetEvent;
    end;

    LSubscriptionId := FNatsConn.Subscribe(LReplySubject, LHandler);
    FNatsConn.Unsubscribe(LSubscriptionId, 1);

    FNatsConn.PublishBytes(ASubject, AData, LReplySubject, LFinalHeaders);

    LStartTime := Now;
    while True do
    begin
      case LResponseEvent.WaitFor(FDefaultTimeoutMS) of
        wrSignaled:
          begin
            if LSuccess then
            begin
              try
                // Populate existing APubAck
                if Assigned(APubAck) then APubAck.FromJson(LReceivedNatsMsg.Payload)
                else
                  APubAck:=TJSPubAck.FromJsonString(LReceivedNatsMsg.Payload);
                Result := not APubAck.HasError;
                if Result and Assigned(LReceivedNatsMsg.Headers) and (LReceivedNatsMsg.Headers.Values['Status'] = '503') then
                begin
                    if not APubAck.HasError then APubAck.error.code := 503;
                    if APubAck.error.description = '' then APubAck.error.description := LReceivedNatsMsg.Headers.Values['Description'];
                    Result := False;
                end;
              except
                on E: Exception do
                  raise ENatsException.CreateFmt('Failed to parse JetStream PubAck (bytes) from %s: %s. JSON: %s', [ASubject, E.Message, LReceivedNatsMsg.Payload]);
              end;
            end else Result := False;
            Break;
          end;
        wrTimeout:
          begin
            FNatsConn.Unsubscribe(LSubscriptionId, 0);
            raise ENatsException.CreateFmt('JetStream publish (bytes) to %s timed out waiting for Ack.', [ASubject]);
          end;
        else
          raise ENatsException.Create('Error waiting for JetStream publish (bytes) Ack event.');
      end;
    end;
  finally
    LResponseEvent.Free;
    LFinalHeaders.Free;
  end;
end;

function TJetStreamContext.PublishBytes(const ASubject: string; const AData: TBytes;
  var APubAck: TJSPubAck): Boolean;
var
  LOptions: TJetStreamPublishOptions;
begin
  Result := PublishBytes(ASubject, AData, APubAck, LOptions);
end;

{ Consumer Management }
function TJetStreamContext.CreateConsumer(const AStreamName: string; const AConfig: TJSConsumerConfig; var AResponse: TJSConsumerCreateResponse): Boolean;
var LSubject, LConsumerPart: string;
begin
  LConsumerPart := AConfig.durable_name;
  if LConsumerPart = '' then LConsumerPart := AConfig.name;

  if LConsumerPart <> '' then
    LSubject := Format(JS_API_PREFIX + 'CONSUMER.CREATE.%s.%s', [AStreamName, LConsumerPart])
  else
    LSubject := Format(JS_API_PREFIX + 'CONSUMER.CREATE.%s', [AStreamName]);

  Result := DoApiRequest<TJSConsumerCreateResponse>(LSubject, AConfig.ToJsonString, AResponse, FDefaultTimeoutMS);
  if Result and AResponse.HasError then Result := False;
end;

function TJetStreamContext.GetConsumerInfo(const AStreamName, AConsumerName: string; var AResponse: TJSConsumerInfoResponse): Boolean;
var LSubject: string;
begin
  LSubject := Format(JS_API_PREFIX + 'CONSUMER.INFO.%s.%s', [AStreamName, AConsumerName]);
  Result := DoApiRequest<TJSConsumerInfoResponse>(LSubject, AResponse, FDefaultTimeoutMS);
  if Result and AResponse.HasError then Result := False;
end;

function TJetStreamContext.DeleteConsumer(const AStreamName, AConsumerName: string; var AResponse: TJSConsumerDeleteResponse): Boolean;
var LSubject: string;
begin
  LSubject := Format(JS_API_PREFIX + 'CONSUMER.DELETE.%s.%s', [AStreamName, AConsumerName]);
  Result := DoApiRequest<TJSConsumerDeleteResponse>(LSubject, AResponse, FDefaultTimeoutMS);
  if Result and AResponse.HasError then Result := False;
end;

function TJetStreamContext.ListConsumers(const AStreamName: string; var AResponse: TJSConsumerNamesResponse): Boolean;
var LSubject: string;
begin
  LSubject := Format(JS_API_PREFIX + 'CONSUMER.NAMES.%s', [AStreamName]);
  Result := DoApiRequest<TJSConsumerNamesResponse>(LSubject, AResponse, FDefaultTimeoutMS);
  if Result and AResponse.HasError then Result := False;
end;

{ Message Consumption }
function TJetStreamContext.FetchNextMessagesInternal(const AStreamName, AConsumerName: string;
  const ARequestData: TJSMessageGetRequest;
  var AMessages: TList<TJSReceivedMessage>;
  ATimeoutMS: Cardinal): Boolean;
var
  LSubject, LRequestJson, LReplySubject: string;
  LSubscriptionId: Integer;
  LResponseEvent: TEvent;
  LReceivedNatsMsg: TNatsArgsMSG;
  LSuccess: Boolean;
  LHandler: TNatsMsgHandler;
  LStartTime: TDateTime;
  LMessagesReceivedCount: Integer;
  LCriticalSection: TCriticalSection;
  LIsControlMessage: Boolean;
  LStatusHeader: string;
begin
  Result := False;
  if not FNatsConn.Connected then
    raise ENatsException.Create('NATS not connected for JetStream FetchMessages.');

  LSubject := Format(JS_API_PREFIX + 'CONSUMER.MSG.NEXT.%s.%s', [AStreamName, AConsumerName]);
  LRequestJson := ARequestData.ToJsonString;
  if ARequestData.no_wait and ((LRequestJson = '{}') or ((ARequestData.batch = 0) and (ARequestData.expires = 0) and (ARequestData.max_bytes = 0))) then
    LRequestJson := '';

  LReplySubject := FNatsConn.GetNewInbox;
  LResponseEvent := TEvent.Create(nil, True, False, '', False);
  LSuccess := False;
  LMessagesReceivedCount := 0;
  LCriticalSection := TCriticalSection.Create;

  //D11 : [dcc32 Error] Nats.JetStream.Client.pas(632): E2555 Cannot capture symbol 'AMessages'
  var LMessages:=AMessages;
  LHandler := procedure(const AMsg: TNatsArgsMSG)
  var
    LJSMessage: TJSReceivedMessage;
  begin
    LCriticalSection.Enter;
    try
      LJSMessage := Self.ToJSReceivedMessage(AMsg);
      LStatusHeader := LJSMessage.GetHeader('Status');

      LIsControlMessage := (LStatusHeader = '100') or
                           (LStatusHeader = '404') or
                           (LStatusHeader = '408') or
                           (LStatusHeader = '409') or
                           ((Length(LJSMessage.Data) = 0) and (LStatusHeader<>''));

      if not LIsControlMessage then
      begin
        LMessages.Add(LJSMessage);
        Inc(LMessagesReceivedCount);
      end;

      LSuccess := True;
    finally
      LCriticalSection.Leave;
    end;

    if LIsControlMessage or ( (ARequestData.batch > 0) and (LMessagesReceivedCount >= ARequestData.batch) ) then
      LResponseEvent.SetEvent
    else if (ARequestData.batch <= 1) and LSuccess then
      LResponseEvent.SetEvent;
  end;

  try
    LSubscriptionId := FNatsConn.Subscribe(LReplySubject, LHandler);
    var LUnsubMax: Integer;
    if ARequestData.batch > 0 then LUnsubMax := ARequestData.batch + 5
    else LUnsubMax := 5;
    FNatsConn.Unsubscribe(LSubscriptionId, LUnsubMax);

    FNatsConn.Publish(LSubject, LRequestJson, LReplySubject);

    LStartTime := Now;
    case LResponseEvent.WaitFor(ATimeoutMS) of
      wrSignaled: Result := LSuccess;
      wrTimeout:
        begin
          FNatsConn.Unsubscribe(LSubscriptionId, 0);
          raise ENatsException.CreateFmt('JetStream FetchMessages for %s.%s timed out after %dms.', [AStreamName, AConsumerName, ATimeoutMS]);
        end;
      else
        raise ENatsException.Create('Error waiting for JetStream FetchMessages event.');
    end;
  finally
    LResponseEvent.Free;
    LCriticalSection.Free;
  end;
end;

function TJetStreamContext.FetchMessages(const AStreamName, AConsumerName: string; ABatchSize: Integer;
  var AMessages: TList<TJSReceivedMessage>; AExpiresNS: Int64; AMaxBytes: Integer; ANoWait: Boolean): Boolean;
var
  LRequest: TJSMessageGetRequest;
  LTimeoutMS: Cardinal;
begin
  if not Assigned(AMessages) then
    raise EArgumentNilException.Create('AMessages list cannot be nil for FetchMessages');
  // Caller is responsible for clearing AMessages if it's reused, and freeing its contents.
  // AMessages.Clear; // Or, if AMessages should accumulate, don't clear.
  // For now, assume caller manages clearing/freeing of TJSReceivedMessage instances in the list.

  LRequest.batch := ABatchSize;
  if (not ANoWait) and (LRequest.batch <= 0) then LRequest.batch := 1;

  LRequest.expires := AExpiresNS;
  LRequest.max_bytes := AMaxBytes;
  LRequest.no_wait := ANoWait;
  LRequest.heartbeat := 0;

  if AExpiresNS > 0 then
    LTimeoutMS := (AExpiresNS div 1000000) + 5000
  else if ANoWait then
    LTimeoutMS := Max(1000, FDefaultTimeoutMS)
  else
    LTimeoutMS := FDefaultTimeoutMS * Max(1, LRequest.batch div 5 + 1) + 5000;

  Result := FetchNextMessagesInternal(AStreamName, AConsumerName, LRequest, AMessages, LTimeoutMS);
end;

{ Acking }
procedure TJetStreamContext.Ack(const AMessage: TJSReceivedMessage; const APayload: string);
begin
  // AMessage is a record, so its Headers TStringList needs careful lifetime management.
  // Assuming if AMessage is passed, its ReplyTo is valid.
  if (AMessage.ReplyTo <> '') and FNatsConn.Connected then
  begin
    FNatsConn.Publish(AMessage.ReplyTo, APayload, '');
  end;
end;

procedure TJetStreamContext.Ack(const AReplyToSubject: string; const APayload: string);
begin
  if (AReplyToSubject <> '') and FNatsConn.Connected then
  begin
    FNatsConn.Publish(AReplyToSubject, APayload, '');
  end;
end;

{ TJetStreamPublishOptions }

class operator TJetStreamPublishOptions.Finalize(
  var Dest: TJetStreamPublishOptions);
begin
  Dest.CustomHeaders.free;
end;

class operator TJetStreamPublishOptions.Initialize(
  out Dest: TJetStreamPublishOptions);
begin
  with Dest do
    begin
      MsgID := '';
      ExpectedStream := '';
      ExpectedLastSeq := 0;
      ExpectedLastMsgID := '';
      CustomHeaders := TStringList.Create;
      CustomHeaders.CaseSensitive := False;
    end;
end;

end.

