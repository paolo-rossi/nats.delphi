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
unit Nats.Json.Utils;

interface

uses
  System.JSON.Serializers,rest.Json,system.json;

type
  TNatsJsonUtils = class
  public
    class function RecordToJson<R: record>(const AValue: R): string; static;
    class function JsonToRecord<R: record>(const AJson: string): R; static;
    class procedure JsonToObject(const AJson: string;AObject: TObject); static;
  end;

implementation

{ TJsonRecordUtils }

class function TNatsJsonUtils.RecordToJson<R>(const AValue: R): string;
var
  Serializer: TJsonSerializer;
begin
  Serializer := TJsonSerializer.Create;
  try
    Result := Serializer.Serialize(AValue);  // Safe for Delphi 11
  finally
    Serializer.Free;
  end;
end;

class procedure TNatsJsonUtils.JsonToObject(const AJson: string;
  AObject: TObject);
begin
  var LObj:=TJSONValue.ParseJSONValue(AJson) as TJSONObject;
  TJson.JsonToObject(AObject,LObj);
end;

class function TNatsJsonUtils.JsonToRecord<R>(const AJson: string): R;
var
  Serializer: TJsonSerializer;
begin
  Serializer := TJsonSerializer.Create;
  try
    Result := Serializer.Deserialize<R>(AJson);
  finally
    Serializer.Free;
  end;
end;

end.

