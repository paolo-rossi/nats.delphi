{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.JetStream.Client;

interface

uses
  System.SysUtils,

  Nats.Consts,
  Nats.Classes,
  Nats.Connection,
  Nats.Exceptions,
  Nats.JetStream.Consts,
  Nats.JetStream.Entities;

type
  /// <summary>
  ///   The server answered, and the answer was an error rather than a result
  /// </summary>
  /// <remarks>
  ///   Raised rather than returned because a JetStream error carries no result
  ///   at all: there is nothing to hand back. Branch on ErrCode, which
  ///   identifies the specific failure - Code is only the broad HTTP-like class
  ///   (404 not found, 400 bad request, 503 unavailable).
  /// </remarks>
  EJetStreamApiError = class(ENatsException)
  private
    FError: TJetStreamApiError;
  public
    constructor Create(const AError: TJetStreamApiError); reintroduce;

    /// The whole error object, as the server sent it
    property Error: TJetStreamApiError read FError;
    function Code: Integer;
    function ErrCode: Integer;
    /// <summary>
    ///   True for the "it isn't there" family. Worth its own test because
    ///   "create it if missing" is the single most common reason to catch this
    /// </summary>
    function IsNotFound: Boolean;
  end;

  /// <summary>
  ///   The JetStream API did not answer at all, as opposed to answering with an
  ///   error. Usually means JetStream is not enabled on the server
  /// </summary>
  EJetStreamTimeout = class(ENatsException);

  /// <summary>
  ///   The JetStream management API, over an existing connection
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     Every method here BLOCKS. There is no sensible asynchronous form of
  ///     "create a stream", and management calls are made from setup code, not
  ///     from a message handler. Consumption (Phase 4) is handler-based
  ///     instead, matching the rest of the library.
  ///   </para>
  ///   <para>
  ///     **Never call these from a message, connect or disconnect handler.**
  ///     Those run on the connection's consumer thread, which is the thread
  ///     that has to deliver the API's reply, so the call would block until it
  ///     times out. This is RequestSync's constraint and it is inherited whole.
  ///   </para>
  ///   <para>
  ///     Does NOT own the connection - it is constructed over one and expects
  ///     it to outlive the context.
  ///   </para>
  /// </remarks>
  TJetStreamContext = class
  private
    FConnection: TNatsConnection;
    FDomain: string;
    FTimeout: Cardinal;

    /// $JS.API. or $JS.<domain>.API.
    function Prefix: string;
    function ApiSubject(const ATemplate: string; const AArgs: array of const): string;
    /// <summary>
    ///   The choke point: one place that builds the subject, sends the request,
    ///   and decides whether the answer is a result or an error
    /// </summary>
    function ApiRequestRaw(const ASubject, ARequestJson: string): string;
    /// <summary>
    ///   Raises if AName cannot go into an API subject unescaped. A name with a
    ///   dot in it would address a different endpoint, not fail
    /// </summary>
    procedure CheckName(const AKind, AName: string);
  public
    constructor Create(AConnection: TNatsConnection; const ADomain: string = '');

    { the choke point, exposed for calls this class does not wrap yet }

    function ApiRequest<TResp: record>(const ASubject: string): TResp; overload;
    function ApiRequest<TReq, TResp: record>(const ASubject: string;
      const ARequest: TReq): TResp; overload;

    { account }

    function AccountInfo: TJetStreamAccountInfo;

    { streams }

    function AddStream(const AConfig: TJetStreamStreamConfig): TJetStreamStreamInfo;
    /// <summary>
    ///   Replaces the WHOLE configuration - read StreamInfo, change what you
    ///   want and send it all back, or the fields you leave out revert
    /// </summary>
    function UpdateStream(const AConfig: TJetStreamStreamConfig): TJetStreamStreamInfo;
    function StreamInfo(const AStream: string): TJetStreamStreamInfo;
    function DeleteStream(const AStream: string): Boolean;
    /// Removes every message but keeps the stream. Returns how many went
    function PurgeStream(const AStream: string): UInt64;
    function ListStreams(AOffset: Integer = 0): TJetStreamStreamListResponse;
    function StreamNames(AOffset: Integer = 0): TArray<string>;

    { consumers }

    /// <summary>
    ///   Creates a durable consumer when the config names one, an ephemeral
    ///   consumer when it does not - the two use different API subjects
    /// </summary>
    function AddConsumer(const AStream: string;
      const AConfig: TJetStreamConsumerConfig): TJetStreamConsumerInfo;
    function ConsumerInfo(const AStream, AConsumer: string): TJetStreamConsumerInfo;
    function DeleteConsumer(const AStream, AConsumer: string): Boolean;
    function ListConsumers(const AStream: string; AOffset: Integer = 0): TJetStreamConsumerListResponse;
    function ConsumerNames(const AStream: string; AOffset: Integer = 0): TArray<string>;

    /// Empty unless this context was built for a JetStream domain
    property Domain: string read FDomain;
    /// How long to wait for an API reply. Defaults to the connection's own
    property Timeout: Cardinal read FTimeout write FTimeout;
  end;

implementation

{ EJetStreamApiError }

constructor EJetStreamApiError.Create(const AError: TJetStreamApiError);
begin
  { The description is the server's own wording - repeating the codes in the
    message means a log line identifies the failure without any extra work }
  inherited CreateFmt('JetStream API error %d (code %d): %s',
    [AError.ErrCode, AError.Code, AError.Description]);
  FError := AError;
end;

function EJetStreamApiError.Code: Integer;
begin
  Result := FError.Code;
end;

function EJetStreamApiError.ErrCode: Integer;
begin
  Result := FError.ErrCode;
end;

function EJetStreamApiError.IsNotFound: Boolean;
begin
  Result := FError.Code = 404;
end;

{ TJetStreamContext }

constructor TJetStreamContext.Create(AConnection: TNatsConnection; const ADomain: string);
begin
  inherited Create;

  if not Assigned(AConnection) then
    raise ENatsException.Create('A JetStream context needs a connection');

  FConnection := AConnection;
  FDomain := ADomain;
  FTimeout := NatsConstants.DEFAULT_REQUEST_TIMEOUT;
end;

function TJetStreamContext.Prefix: string;
begin
  if FDomain.IsEmpty then
    Result := JetStreamConstants.Api.PREFIX
  else
    Result := Format(JetStreamConstants.Api.PREFIX_DOMAIN, [FDomain]);
end;

function TJetStreamContext.ApiSubject(const ATemplate: string; const AArgs: array of const): string;
begin
  Result := Prefix + Format(ATemplate, AArgs);
end;

procedure TJetStreamContext.CheckName(const AKind, AName: string);
var
  LChar: Char;
begin
  if AName.IsEmpty then
    raise ENatsException.CreateFmt('The %s name cannot be empty', [AKind]);

  for LChar in JetStreamConstants.Naming.INVALID_CHARS do
    if AName.Contains(LChar) then
      raise ENatsException.CreateFmt(
        'The %s name [%s] cannot contain %s - it goes into the API subject as ' +
        'it stands, so it would address a different endpoint rather than fail',
        [AKind, AName, QuotedStr(LChar)]);
end;

function TJetStreamContext.ApiRequestRaw(const ASubject, ARequestJson: string): string;
var
  LReply: TNatsArgsMSG;
  LResponse: TJetStreamApiResponse;
begin
  if not FConnection.RequestSync(ASubject, ARequestJson, LReply, FTimeout) then
    raise EJetStreamTimeout.CreateFmt(
      'No reply from the JetStream API on [%s] within %d ms. The usual cause ' +
      'is JetStream not being enabled on this server', [ASubject, FTimeout]);

  Result := LReply.Payload;

  { The error object is checked BEFORE the caller deserializes the real type,
    and this is the only place it happens. An error response carries no result,
    so deserializing it as the expected record yields an empty one - which is
    indistinguishable from a successful call that returned all defaults }
  LResponse := TJetStreamJSON.FromJSON<TJetStreamApiResponse>(Result);
  if LResponse.Error.HasError then
    raise EJetStreamApiError.Create(LResponse.Error);
end;

function TJetStreamContext.ApiRequest<TResp>(const ASubject: string): TResp;
begin
  { No request body at all. Several API calls take none, and sending an empty
    JSON object where the server expects nothing is needless }
  Result := TJetStreamJSON.FromJSON<TResp>(ApiRequestRaw(ASubject, ''));
end;

function TJetStreamContext.ApiRequest<TReq, TResp>(const ASubject: string;
  const ARequest: TReq): TResp;
begin
  Result := TJetStreamJSON.FromJSON<TResp>(
    ApiRequestRaw(ASubject, TJetStreamJSON.ToJSON<TReq>(ARequest)));
end;

{ account }

function TJetStreamContext.AccountInfo: TJetStreamAccountInfo;
begin
  Result := ApiRequest<TJetStreamAccountInfo>(ApiSubject(JetStreamConstants.Api.INFO, []));
end;

{ streams }

function TJetStreamContext.AddStream(const AConfig: TJetStreamStreamConfig): TJetStreamStreamInfo;
begin
  CheckName('stream', AConfig.Name);

  Result := ApiRequest<TJetStreamStreamConfig, TJetStreamStreamInfo>(
    ApiSubject(JetStreamConstants.Api.STREAM_CREATE, [AConfig.Name]), AConfig);
end;

function TJetStreamContext.UpdateStream(const AConfig: TJetStreamStreamConfig): TJetStreamStreamInfo;
begin
  CheckName('stream', AConfig.Name);

  Result := ApiRequest<TJetStreamStreamConfig, TJetStreamStreamInfo>(
    ApiSubject(JetStreamConstants.Api.STREAM_UPDATE, [AConfig.Name]), AConfig);
end;

function TJetStreamContext.StreamInfo(const AStream: string): TJetStreamStreamInfo;
begin
  CheckName('stream', AStream);

  Result := ApiRequest<TJetStreamStreamInfo>(
    ApiSubject(JetStreamConstants.Api.STREAM_INFO, [AStream]));
end;

function TJetStreamContext.DeleteStream(const AStream: string): Boolean;
begin
  CheckName('stream', AStream);

  Result := ApiRequest<TJetStreamSuccessResponse>(
    ApiSubject(JetStreamConstants.Api.STREAM_DELETE, [AStream])).Success;
end;

function TJetStreamContext.PurgeStream(const AStream: string): UInt64;
begin
  CheckName('stream', AStream);

  Result := ApiRequest<TJetStreamSuccessResponse>(
    ApiSubject(JetStreamConstants.Api.STREAM_PURGE, [AStream])).Purged;
end;

function TJetStreamContext.ListStreams(AOffset: Integer): TJetStreamStreamListResponse;
var
  LRequest: TJetStreamListRequest;
begin
  LRequest.Offset := AOffset;

  Result := ApiRequest<TJetStreamListRequest, TJetStreamStreamListResponse>(
    ApiSubject(JetStreamConstants.Api.STREAM_LIST, []), LRequest);
end;

function TJetStreamContext.StreamNames(AOffset: Integer): TArray<string>;
var
  LRequest: TJetStreamListRequest;
begin
  LRequest.Offset := AOffset;

  Result := ApiRequest<TJetStreamListRequest, TJetStreamNamesResponse>(
    ApiSubject(JetStreamConstants.Api.STREAM_NAMES, []), LRequest).Streams;
end;

{ consumers }

function TJetStreamContext.AddConsumer(const AStream: string;
  const AConfig: TJetStreamConsumerConfig): TJetStreamConsumerInfo;
var
  LRequest: TJetStreamConsumerCreateRequest;
  LName, LSubject: string;
begin
  CheckName('stream', AStream);

  { DurableName is the durable one; Name covers the 2.9+ named-ephemeral case.
    With neither, the server names the consumer itself and the subject carries
    no name at all - a different endpoint, not the same one with a blank token }
  LName := AConfig.DurableName;
  if LName.IsEmpty then
    LName := AConfig.Name;

  if LName.IsEmpty then
    LSubject := ApiSubject(JetStreamConstants.Api.CONSUMER_CREATE, [AStream])
  else
  begin
    CheckName('consumer', LName);
    LSubject := ApiSubject(JetStreamConstants.Api.CONSUMER_CREATE_NAMED, [AStream, LName]);
  end;

  LRequest.StreamName := AStream;
  LRequest.Config := AConfig;

  Result := ApiRequest<TJetStreamConsumerCreateRequest, TJetStreamConsumerInfo>(
    LSubject, LRequest);
end;

function TJetStreamContext.ConsumerInfo(const AStream, AConsumer: string): TJetStreamConsumerInfo;
begin
  CheckName('stream', AStream);
  CheckName('consumer', AConsumer);

  Result := ApiRequest<TJetStreamConsumerInfo>(
    ApiSubject(JetStreamConstants.Api.CONSUMER_INFO, [AStream, AConsumer]));
end;

function TJetStreamContext.DeleteConsumer(const AStream, AConsumer: string): Boolean;
begin
  CheckName('stream', AStream);
  CheckName('consumer', AConsumer);

  Result := ApiRequest<TJetStreamSuccessResponse>(
    ApiSubject(JetStreamConstants.Api.CONSUMER_DELETE, [AStream, AConsumer])).Success;
end;

function TJetStreamContext.ListConsumers(const AStream: string;
  AOffset: Integer): TJetStreamConsumerListResponse;
var
  LRequest: TJetStreamListRequest;
begin
  CheckName('stream', AStream);
  LRequest.Offset := AOffset;

  Result := ApiRequest<TJetStreamListRequest, TJetStreamConsumerListResponse>(
    ApiSubject(JetStreamConstants.Api.CONSUMER_LIST, [AStream]), LRequest);
end;

function TJetStreamContext.ConsumerNames(const AStream: string;
  AOffset: Integer): TArray<string>;
var
  LRequest: TJetStreamListRequest;
begin
  CheckName('stream', AStream);
  LRequest.Offset := AOffset;

  Result := ApiRequest<TJetStreamListRequest, TJetStreamNamesResponse>(
    ApiSubject(JetStreamConstants.Api.CONSUMER_NAMES, [AStream]), LRequest).Consumers;
end;

end.
