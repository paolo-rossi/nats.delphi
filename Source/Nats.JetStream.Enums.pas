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
unit Nats.JetStream.Enums;

interface
 uses
  System.SysUtils, System.Classes, System.Generics.Collections,
  System.Rtti, System.TypInfo,
  System.JSON.Converters,
  System.JSON.Serializers,
  System.JSON.Readers,
  System.JSON.Writers;

type

  // Enum Converters
  TRetentionPolicyConverter = class(TJsonConverter)
  public
    function CanConvert(AType: PTypeInfo): Boolean; override;
    function ReadJson(const AReader: TJsonReader; AType: PTypeInfo;
      const AExistingValue: TValue; const ASerializer: TJsonSerializer)
      : TValue; override;
    procedure WriteJson(const AWriter: TJsonWriter; const AValue: TValue;
      const ASerializer: TJsonSerializer); override;
  end;

  TStorageTypeConverter = class(TJsonConverter)
  public
    function CanConvert(AType: PTypeInfo): Boolean; override;
    function ReadJson(const AReader: TJsonReader; AType: PTypeInfo;
      const AExistingValue: TValue; const ASerializer: TJsonSerializer): TValue;
      override;
    procedure WriteJson(const AWriter: TJsonWriter; const AValue: TValue;
      const ASerializer: TJsonSerializer); override;
  end;

  TDiscardPolicyConverter = class(TJsonConverter)
  public
    function CanConvert(AType: PTypeInfo): Boolean; override;
    function ReadJson(const AReader: TJsonReader; AType: PTypeInfo;
      const AExistingValue: TValue; const ASerializer: TJsonSerializer): TValue;
      override;
    procedure WriteJson(const AWriter: TJsonWriter; const AValue: TValue;
      const ASerializer: TJsonSerializer); override;
  end;

  TAckPolicyConverter = class(TJsonConverter)
  public
    function CanConvert(AType: PTypeInfo): Boolean; override;
    function ReadJson(const AReader: TJsonReader; AType: PTypeInfo;
      const AExistingValue: TValue; const ASerializer: TJsonSerializer): TValue;
      override;
    procedure WriteJson(const AWriter: TJsonWriter; const AValue: TValue;
      const ASerializer: TJsonSerializer); override;
  end;

  TReplayPolicyConverter = class(TJsonConverter)
  public
    function CanConvert(AType: PTypeInfo): Boolean; override;
    function ReadJson(const AReader: TJsonReader; AType: PTypeInfo;
      const AExistingValue: TValue; const ASerializer: TJsonSerializer): TValue;
      override;
    procedure WriteJson(const AWriter: TJsonWriter; const AValue: TValue;
      const ASerializer: TJsonSerializer); override;
  end;

  TDeliverPolicyConverter = class(TJsonConverter)
  public
    function CanConvert(AType: PTypeInfo): Boolean; override;
    function ReadJson(const AReader: TJsonReader; AType: PTypeInfo;
      const AExistingValue: TValue; const ASerializer: TJsonSerializer): TValue;
      override;
    procedure WriteJson(const AWriter: TJsonWriter; const AValue: TValue;
      const ASerializer: TJsonSerializer); override;
  end;

  // --- Enums for JetStream (must match JSON string values) ---
  [JsonConverter(TRetentionPolicyConverter)]
  TRetentionPolicy = (rpLimits, rpInterest, rpWorkQueue);
  [JsonConverter(TStorageTypeConverter)]
  TStorageType = (stFile, stMemory);
  [JsonConverter(TDiscardPolicyConverter)]
  TDiscardPolicy = (disOld, disNew);
  [JsonConverter(TAckPolicyConverter)]
  TAckPolicy = (apNone, apAll, apExplicit);
  [JsonConverter(TReplayPolicyConverter)]
  TReplayPolicy = (rpInstant, rpOriginal);
  [JsonConverter(TDeliverPolicyConverter)]
  TDeliverPolicy = (dpAll, dpLast, dpNew, dpByStartSequence, dpByStartTime,
    dpLastPerSubject);

  const
  RetentionPolicyStrings: array [TRetentionPolicy] of string =
    ('limits', 'interest', 'work_queue');
  StorageTypeStrings: array [TStorageType] of string =
    ('file', 'memory');
  DiscardPolicyStrings: array [TDiscardPolicy] of string =
    ('old', 'new');
  AckPolicyStrings: array [TAckPolicy] of string =
    ('none', 'all', 'explicit');
  ReplayPolicyStrings: array [TReplayPolicy] of string =
    ('instant', 'original');
  DeliverPolicyStrings: array [TDeliverPolicy] of string =
    ('all', 'last', 'new', 'by_start_sequence', 'by_start_time', 'last_per_subject');



implementation


{ Enum Converter Implementations - These remain unchanged as they operate on enum types }
function TRetentionPolicyConverter.CanConvert(AType: PTypeInfo): Boolean;
begin
  Result := AType^.Kind = tkEnumeration;
  Result := Result and SameText(AType^.Name, 'TRetentionPolicy');
end;

function TRetentionPolicyConverter.ReadJson(const AReader: TJsonReader;
  AType: PTypeInfo; const AExistingValue: TValue;
  const ASerializer: TJsonSerializer): TValue;
var
  LStr: string;
  I: TRetentionPolicy;
begin
  LStr := AReader.Value.AsString;
  for I := Low(TRetentionPolicy) to High(TRetentionPolicy) do
    if SameText(LStr, RetentionPolicyStrings[I]) then
      Exit(TValue.From<TRetentionPolicy>(I));
end;

procedure TRetentionPolicyConverter.WriteJson(const AWriter: TJsonWriter;
  const AValue: TValue; const ASerializer: TJsonSerializer);
begin
  AWriter.WriteValue(RetentionPolicyStrings[AValue.AsType<TRetentionPolicy>]);
end;

function TStorageTypeConverter.CanConvert(AType: PTypeInfo): Boolean;
begin
  Result := AType^.Kind = tkEnumeration;
  Result := Result and SameText(AType^.Name, 'TStorageType');
end;

function TStorageTypeConverter.ReadJson(const AReader: TJsonReader;
  AType: PTypeInfo; const AExistingValue: TValue;
  const ASerializer: TJsonSerializer): TValue;
var
  LStr: string;
  I: TStorageType;
begin
  LStr := AReader.Value.AsString;
  for I := Low(TStorageType) to High(TStorageType) do
    if SameText(LStr, StorageTypeStrings[I]) then
      Exit(TValue.From<TStorageType>(I));
end;

procedure TStorageTypeConverter.WriteJson(const AWriter: TJsonWriter;
  const AValue: TValue; const ASerializer: TJsonSerializer);
begin
  AWriter.WriteValue(StorageTypeStrings[AValue.AsType<TStorageType>]);
end;

function TDiscardPolicyConverter.CanConvert(AType: PTypeInfo): Boolean;
begin
  Result := AType^.Kind = tkEnumeration;
  Result := Result and SameText(AType^.Name, 'TDiscardPolicy');
end;

function TDiscardPolicyConverter.ReadJson(const AReader: TJsonReader;
  AType: PTypeInfo; const AExistingValue: TValue;
  const ASerializer: TJsonSerializer): TValue;
var
  LStr: string;
  I: TDiscardPolicy;
begin
  LStr := AReader.Value.AsString;
  for I := Low(TDiscardPolicy) to High(TDiscardPolicy) do
    if SameText(LStr, DiscardPolicyStrings[I]) then
      Exit(TValue.From<TDiscardPolicy>(I));
end;

procedure TDiscardPolicyConverter.WriteJson(const AWriter: TJsonWriter;
  const AValue: TValue; const ASerializer: TJsonSerializer);
begin
  AWriter.WriteValue(DiscardPolicyStrings[AValue.AsType<TDiscardPolicy>]);
end;

function TAckPolicyConverter.CanConvert(AType: PTypeInfo): Boolean;
begin
  Result := AType^.Kind = tkEnumeration;
  Result := Result and SameText(AType^.Name, 'TAckPolicy');
end;

function TAckPolicyConverter.ReadJson(const AReader: TJsonReader;
  AType: PTypeInfo; const AExistingValue: TValue;
  const ASerializer: TJsonSerializer): TValue;
var
  LStr: string;
  I: TAckPolicy;
begin
  LStr := AReader.Value.AsString;
  for I := Low(TAckPolicy) to High(TAckPolicy) do
    if SameText(LStr, AckPolicyStrings[I]) then
      Exit(TValue.From<TAckPolicy>(I));
end;

procedure TAckPolicyConverter.WriteJson(const AWriter: TJsonWriter;
  const AValue: TValue; const ASerializer: TJsonSerializer);
begin
  AWriter.WriteValue(AckPolicyStrings[AValue.AsType<TAckPolicy>]);
end;

function TReplayPolicyConverter.CanConvert(AType: PTypeInfo): Boolean;
begin
  Result := AType^.Kind = tkEnumeration;
  Result := Result and SameText(AType^.Name, 'TReplayPolicy');
end;

function TReplayPolicyConverter.ReadJson(const AReader: TJsonReader;
  AType: PTypeInfo; const AExistingValue: TValue;
  const ASerializer: TJsonSerializer): TValue;
var
  LStr: string;
  I: TReplayPolicy;
begin
  LStr := AReader.Value.AsString;
  for I := Low(TReplayPolicy) to High(TReplayPolicy) do
    if SameText(LStr, ReplayPolicyStrings[I]) then
      Exit(TValue.From<TReplayPolicy>(I));
end;

procedure TReplayPolicyConverter.WriteJson(const AWriter: TJsonWriter;
  const AValue: TValue; const ASerializer: TJsonSerializer);
begin
  AWriter.WriteValue(ReplayPolicyStrings[AValue.AsType<TReplayPolicy>]);
end;

function TDeliverPolicyConverter.CanConvert(AType: PTypeInfo): Boolean;
begin
  Result := AType^.Kind = tkEnumeration;
  Result := Result and SameText(AType^.Name, 'TDeliverPolicy');
end;

function TDeliverPolicyConverter.ReadJson(const AReader: TJsonReader;
  AType: PTypeInfo; const AExistingValue: TValue;
  const ASerializer: TJsonSerializer): TValue;
var
  LStr: string;
  I: TDeliverPolicy;
begin
  LStr := AReader.Value.AsString;
  for I := Low(TDeliverPolicy) to High(TDeliverPolicy) do
    if SameText(LStr, DeliverPolicyStrings[I]) then
      Exit(TValue.From<TDeliverPolicy>(I));
end;

procedure TDeliverPolicyConverter.WriteJson(const AWriter: TJsonWriter;
  const AValue: TValue; const ASerializer: TJsonSerializer);
begin
  AWriter.WriteValue(DeliverPolicyStrings[AValue.AsType<TDeliverPolicy>]);
end;

end.
