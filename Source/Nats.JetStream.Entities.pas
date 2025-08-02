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
unit Nats.JetStream.Entities;

interface

uses
  System.SysUtils, System.Classes, System.Generics.Collections, System.Rtti,
  System.JSON, REST.Json, System.JSON.Serializers,

  Nats.Classes,
  Nats.Entities,
  Nats.JetStream.Enums;

const
  // JetStream API Prefixes
  JS_API_PREFIX = '$JS.API.';
  JS_METRIC_PREFIX = '$JS.EVENT.METRIC.';
  JS_ADVISORY_PREFIX = '$JS.EVENT.ADVISORY.';

  // Common response type strings
  JS_TYPE_STREAM_CREATE_RESPONSE =
    'io.nats.jetstream.api.v1.stream_create_response';
  JS_TYPE_STREAM_INFO_RESPONSE =
    'io.nats.jetstream.api.v1.stream_info_response';
  JS_TYPE_STREAM_DELETE_RESPONSE =
    'io.nats.jetstream.api.v1.stream_delete_response';
  JS_TYPE_CONSUMER_CREATE_RESPONSE =
    'io.nats.jetstream.api.v1.consumer_create_response';
  JS_TYPE_CONSUMER_INFO_RESPONSE =
    'io.nats.jetstream.api.v1.consumer_info_response';
  JS_TYPE_CONSUMER_DELETE_RESPONSE =
    'io.nats.jetstream.api.v1.consumer_delete_response';
  JS_TYPE_MSG_GET_RESPONSE =
    'io.nats.jetstream.api.v1.consumer_msg_next_response';
  JS_TYPE_PUB_ACK_RESPONSE =
    'io.nats.jetstream.api.v1.pub_ack_response';

type
  // --- General JetStream Structures ---
  TJetStreamError = record
    code: Integer;
    err_code: Cardinal;
    description: string;
  end;

  TJetStreamBaseResponse = class
  public
    [JsonName('type')]
    response_type: string;
    error: TJetStreamError;
    request_id: string;      // Optional

    constructor Create; virtual;
    function HasError: Boolean;
    class function FromJsonString<T: TJetStreamBaseResponse, constructor>
      (const AJson: string): T; static;
    function ToJsonString: string; virtual;
  end;

  // --- Stream Configuration and State  ---
  TJSStreamSource = record
    name: string;
    opt_start_seq: UInt64;
    opt_start_time: string;  // RFC3339
    filter_subject: string;
  end;

  TJSStreamState = record
    messages: UInt64;
    bytes: UInt64;
    first_seq: UInt64;
    first_ts: string;        // RFC3339
    last_seq: UInt64;
    last_ts: string;         // RFC3339
    consumer_count: Integer;
  end;

  // This is a configuration structure, can remain a record
  TJSStreamConfig = record
    name: string;
    description: string;
    subjects: TArray<string>;
    retention: TRetentionPolicy;
    [JsonName('max_consumers')]
    max_consumers: Integer;
    [JsonName('max_msgs')]
    max_msgs: Int64;
    [JsonName('max_bytes')]
    max_bytes: Int64;
    [JsonName('max_age')]
    max_age: Int64; // Nanoseconds
    [JsonName('max_msgs_per_subject')]
    max_msgs_per_subject: Int64;
    [JsonName('max_msg_size')]
    max_msg_size: Integer;
    storage: TStorageType;
    [JsonName('num_replicas')]
    num_replicas: Integer;
    [JsonName('no_ack')]
    no_ack: Boolean;
    [JsonName('template_owner')]
    template_owner: string;
    [JsonName('duplicate_window')]
    duplicate_window: Int64; // Nanoseconds
    mirror: TJSStreamSource;
    sources: TArray<TJSStreamSource>;
    sealed: Boolean;
    [JsonName('deny_delete')]
    deny_delete: Boolean;
    [JsonName('deny_purge')]
    deny_purge: Boolean;
    [JsonName('allow_rollup_hdrs')]
    allow_rollup_hdrs: Boolean;
    discard: TDiscardPolicy;
    [JsonName('allow_direct')]
    allow_direct: Boolean;
    [JsonName('mirror_direct')]
    mirror_direct: Boolean;

    function ToJsonString: string; // Instance method for record
    class function FromJsonString(const AJson: string): TJSStreamConfig; static;
    // Class method for record
  end;

  // --- Stream API Responses  ---
  TJSStreamCreateResponse = class(TJetStreamBaseResponse)
  public
    config: TJSStreamConfig; // This is a record field
    created: string;         // RFC3339
    state: TJSStreamState;   // This is a record field
    // did_create: Boolean;    // Optional
  end;

  TJSStreamInfoResponse = class(TJetStreamBaseResponse)
  public
    config: TJSStreamConfig;
    created: string;         // RFC3339
    state: TJSStreamState;
  end;

  TJSStreamDeleteResponse = class(TJetStreamBaseResponse)
  public
    success: Boolean;
  end;

  TJSStreamNamesResponse = class(TJetStreamBaseResponse)
  public
    streams: TArray<string>;
    total: Integer;
    limit: Integer;
    offset: Integer;
  end;

  TJSPubAck = class
  // PubAck is also a response, make it a class for consistency
  public
    stream: string;
    seq: UInt64;
    duplicate: Boolean;
    domain: string;
    error: TJetStreamError; // PubAck can also contain an error
    // type: string; // Optional, if PubAck has a 'type' field in its JSON
    constructor Create;
    function HasError: Boolean;
    function ToJsonString: string;
    procedure FromJson(const AValue: string);
    class function FromJsonString(const AValue: string): TJSPubAck; static;
  end;

  // --- Consumer Configuration and State (ConsumerConfig remains a record) ---
  // This is a configuration structure, can remain a record
  TJSConsumerConfig = record
    name: string;
    [JsonName('durable_name')]
    durable_name: string;
    description: string;
    [JsonName('deliver_policy')]
    deliver_policy: TDeliverPolicy;
    [JsonName('opt_start_seq')]
    opt_start_seq: UInt64;
    [JsonName('opt_start_time')]
    opt_start_time: string; // RFC3339
    [JsonName('ack_policy')]
    ack_policy: TAckPolicy;
    [JsonName('ack_wait')]
    ack_wait: Int64;           // Nanoseconds
    [JsonName('max_deliver')]
    max_deliver: Integer;
    [JsonName('filter_subject')]
    filter_subject: string;
    [JsonName('filter_subjects')]
    filter_subjects: TArray<string>;
    [JsonName('replay_policy')]
    replay_policy: TReplayPolicy;
    [JsonName('sample_freq')]
    sample_freq: string;
    [JsonName('rate_limit_bps')]
    rate_limit_bps: UInt64;
    [JsonName('max_waiting')]
    max_waiting: Integer;
    [JsonName('max_ack_pending')]
    max_ack_pending: Integer;
    [JsonName('flow_control')]
    flow_control: Boolean;
    [JsonName('heartbeat')]
    heartbeat_interval: Int64; // Nanoseconds
    [JsonName('headers_only')]
    headers_only: Boolean;
    [JsonName('max_batch')]
    max_request_batch: Integer;
    [JsonName('max_expires')]
    max_request_expires: Int64;// Nanoseconds
    [JsonName('max_bytes')]
    max_request_max_bytes: Integer;
    [JsonName('inactive_threshold')]
    inactive_threshold: Int64; // Nanoseconds
    [JsonName('num_replicas')]
    num_replicas: Integer;
    [JsonName('mem_storage')]
    mem_storage: Boolean;

    function ToJsonString: string;
    class function FromJsonString(const AValue: string): TJSConsumerConfig; static;
  end;

  TJSSequencePair = record
    consumer_seq: UInt64;
    stream_seq: UInt64;
    last_active: string; // Optional
  end;

  // --- Consumer API Responses (Now Classes) ---
  TJSConsumerCreateResponse = class(TJetStreamBaseResponse)
  public
    stream_name: string;
    name: string;
    config: TJSConsumerConfig;  // Record field
    created: string;
    delivered: TJSSequencePair; // Record field
    ack_floor: TJSSequencePair; // Record field
    num_ack_pending: Integer;
    num_redelivered: Integer;
    num_waiting: Integer;
    num_pending: UInt64;
  end;

  TJSConsumerInfoResponse = class(TJetStreamBaseResponse)
  public
    stream_name: string;
    name: string;
    config: TJSConsumerConfig;
    created: string;
    delivered: TJSSequencePair;
    ack_floor: TJSSequencePair;
    num_ack_pending: Integer;
    num_redelivered: Integer;
    num_waiting: Integer;
    num_pending: UInt64;
  end;

  TJSConsumerDeleteResponse = class(TJetStreamBaseResponse)
  public
    success: Boolean;
  end;

  TJSConsumerNamesResponse = class(TJetStreamBaseResponse)
  public
    consumers: TArray<string>;
    total: Integer;
    limit: Integer;
    offset: Integer;
  end;

  // --- Message Get & Received Message (These remain records as they are data holders) ---
  TJSMessageGetRequest = record
    batch: Integer;
    max_bytes: Integer;
    expires: Int64;   // Nanoseconds
    no_wait: Boolean;
    heartbeat: Int64; // Nanoseconds
    function ToJsonString: string;
  end;

  TJSReceivedMessage = record
    Subject: string;
    ReplyTo: string;
    Data: TBytes;
    Headers: TNatsHeaders;
    Stream: string;
    Sequence: UInt64;
    ConsumerSequence: UInt64;
    Timestamp: Int64; // Nanoseconds
    NumPending: Integer;
    Domain: string;
  end;

  // --- Account Info Response ---
  TJSAccountInfoResponse = class(TJetStreamBaseResponse)
  public
    memory: UInt64;
    storage: UInt64;
    streams: Integer;
    consumers: Integer;
    // limits: TJetStreamAccountLimits; // Define if needed as record or class
    // api: TJSApiStats; // Define if needed as record or class
    domain: string;
  end;

implementation

{ TJetStreamBaseResponse }

constructor TJetStreamBaseResponse.Create;
begin
  inherited Create;

  request_id := '';
  response_type := '';
  // error fields will be default (0, '', etc.)
end;

function TJetStreamBaseResponse.HasError: Boolean;
begin
  Result := (error.code <> 0) or (error.err_code <> 0) or
    (error.description <> '');
end;

class function TJetStreamBaseResponse.FromJsonString<T>(const AJson: string): T;
begin
  // TJson.JsonToObject works for classes too.
  // The generic constraint T: TJetStreamBaseResponse, constructor ensures T is a descendant and has a constructor.
  Result := TJson.JsonToObject<T>(AJson);
end;

function TJetStreamBaseResponse.ToJsonString: string;
begin
  Result := TJson.ObjectToJsonString(Self);
end;

{ TJSStreamConfig - Record methods }

function TJSStreamConfig.ToJsonString: string;
var
  LSer: TJsonSerializer;
begin
  LSer := TJsonSerializer.Create;
  try
    Result := LSer.Serialize<TJSStreamConfig>(Self);
  finally
    LSer.Free;
  end;
end;

class function TJSStreamConfig.FromJsonString(const AJson: string): TJSStreamConfig;
var
  LSer: TJsonSerializer;
begin
  LSer := TJsonSerializer.Create;
  try
    Result := LSer.Deserialize<TJSStreamConfig>(AJson);
  finally
    LSer.Free;
  end;
end;

{ TJSPubAck - Class methods }

constructor TJSPubAck.Create;
begin
  inherited Create;
  stream := '';
  domain := '';
end;


function TJSPubAck.HasError: Boolean;
begin
  Result := (error.code <> 0) or (error.err_code <> 0) or
    (error.description <> '');
end;

function TJSPubAck.ToJsonString: string;
begin
  Result := TJson.ObjectToJsonString(Self);
end;

procedure TJSPubAck.FromJson(const AValue: string);
var
  LObj: TJSONObject;
begin
  LObj := TJSONValue.ParseJSONValue(AValue) as TJSONObject;
  TJson.JsonToObject(self,LObj);
end;

class function TJSPubAck.FromJsonString(const AValue: string): TJSPubAck;
begin
  Result := TJson.JsonToObject<TJSPubAck>(AValue);
end;

{ TJSConsumerConfig - Record methods }

function TJSConsumerConfig.ToJsonString: string;
var
  LSer: TJsonSerializer;
begin
  LSer := TJsonSerializer.Create;
  try
    Result := LSer.Serialize<TJSConsumerConfig>(Self);
  finally
    LSer.Free;
  end;
end;

class function TJSConsumerConfig.FromJsonString(const AValue: string): TJSConsumerConfig;
var
  LSer: TJsonSerializer;
begin
  LSer := TJsonSerializer.Create;
  try
    Result := LSer.Deserialize<TJSConsumerConfig>(AValue);
  finally
    LSer.Free;
  end;
end;

{ TJSMessageGetRequest - Record method }

function TJSMessageGetRequest.ToJsonString: string;
var
  LSer: TJsonSerializer;
begin
  LSer := TJsonSerializer.Create;
  try
    Result := LSer.Serialize<TJSMessageGetRequest>(Self);
  finally
    LSer.Free;
  end;
end;


end.
