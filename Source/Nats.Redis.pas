{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{******************************************************************************}
unit Nats.Redis;

{ A Redis-shaped client over NATS. Strings, hashes, sets and per-key TTL live
  in a JetStream Key/Value bucket ("redis_<db>"); PUBLISH and SUBSCRIBE use
  plain core NATS subjects. The command names and return values follow Redis:

    Redis                  This unit
    SET key val [EX n]     SetValue(key, val [, expirySeconds])
    GET key                GetValue(key)
    DEL key...             Del([key...]) -> count
    EXISTS key             Exists(key) -> 0/1
    EXPIRE key s           Expire(key, s) -> Boolean
    TTL key                Ttl(key) -> seconds, -1 none, -2 missing
    PERSIST key            Persist(key) -> Boolean
    SETNX key val          SetNx(key, val) -> Boolean
    GETSET key val         GetSet(key, val) -> old value
    INCR / DECR key        Incr(key) / Decr(key) -> Int64
    MGET key...            MGet([key...])
    KEYS pattern           Keys(pattern)
    FLUSHDB                FlushDb
    HSET/HGET/HDEL/...     HSet/HGet/HDel/HExists/HGetAll/HLen
    SADD/SREM/...          SAdd/SRem/SMembers/SIsMember/SCard
    PUBLISH/SUBSCRIBE      Publish/Subscribe/Unsubscribe (core NATS)

  How it maps onto NATS:
  - every Redis key is base64url-encoded (so "user:42" and "a b" are fine) and
    stored under a typed subject: "k.<enc>" is the value, "t.<enc>" the TTL,
    "h.<enc>.<encField>" a hash field, "s.<enc>.<encMember>" a set member.
  - the per-key TTL is checked lazily on read: an expired key reads as missing,
    and its bytes linger until DEL or FLUSHDB (like Redis before active expiry)
  - KEYS/EXISTS on hash and set keys scan the bucket (documented cost).

  What is deliberately NOT emulated: MULTI/transactions, lists, sorted sets,
  Lua, and per-key type collisions (SET and HSET may share a name; the two are
  stored in separate subject spaces). PUBLISH reports the LOCAL subscriber
  count, since NATS has no server-side idea of it. }
{******************************************************************************}
interface

{$SCOPEDENUMS ON}

uses
  System.SysUtils, System.Classes, System.DateUtils, System.NetEncoding,
  System.Generics.Collections,

  Nats.Classes,
  Nats.Connection,
  Nats.Exceptions,
  Nats.JetStream.Client,
  Nats.JetStream.KV;

type
  /// <summary>
  ///   A Redis-shaped client over NATS. Every command BLOCKS, like the rest of
  ///   the JetStream surface, so none of it may be called from a message,
  ///   connect or disconnect handler - subscribe handlers run on the consumer
  ///   thread and may only touch thread-safe state.
  /// </summary>
  TNatsRedisClient = class
  private
    FConnection: TNatsConnection;
    FContext: TJetStreamContext;
    FKV: TJetStreamKV;
    FStream: string;
    FBucket: string;
    FDatabase: Integer;
    { pub/sub bookkeeping, so Publish can report the local subscriber count and
      Destroy can unsubscribe what is still open }
    FActiveSubs: TList<Integer>;
    FSubChannels: TDictionary<Integer, string>;
    FChannelCounts: TDictionary<string, Integer>;

    procedure CheckKey(const AKey: string);
    function ValueSubject(const AKey: string): string;
    function TtlSubject(const AKey: string): string;
    function HashPrefix(const AKey: string): string;
    function HashSubject(const AKey, AField: string): string;
    function SetPrefix(const AKey: string): string;
    function SetSubject(const AKey, AMember: string): string;
    function ExpiryMillis: Int64;
    /// True when the key has a TTL that has already elapsed
    function IsExpired(const AKey: string): Boolean;
    /// INCR and DECR share this: read, check it is an integer, add, write back
    function AddToKey(const AKey: string; ADelta: Int64): Int64;
  public
    constructor Create(AConnection: TNatsConnection;
      AContext: TJetStreamContext; ADatabase: Integer = 0);
    destructor Destroy; override;

    /// The Redis key encoding: unpadded base64url of the UTF-8 bytes. Public so
    /// the subjects a key lives on can be worked out when debugging a bucket
    class function Encode(const AValue: string): string; static;
    class function Decode(const AValue: string): string; static;
    /// A glob matcher for KEYS patterns: * any run, ? one character
    class function MatchesPattern(const APattern, AValue: string): Boolean; static;

    { strings }

    /// SET. AExpirySeconds > 0 attaches a TTL; 0 clears any previous one
    function SetValue(const AKey, AValue: string; AExpirySeconds: Int64 = 0): Boolean;
    /// GET. Empty string when missing (or expired, or stored empty)
    function GetValue(const AKey: string): string;
    /// SETNX: True only if the key did not exist
    function SetNx(const AKey, AValue: string): Boolean;
    /// GETSET: the old value, then the new one
    function GetSet(const AKey, AValue: string): string;
    /// DEL: how many of AKeys were actually there
    function Del(const AKeys: TArray<string>): Integer;
    /// EXISTS: 1 when the key is there and not expired, 0 otherwise
    function Exists(const AKey: string): Integer;
    /// EXPIRE: False when the key does not exist
    function Expire(const AKey: string; ASeconds: Int64): Boolean;
    /// TTL: seconds remaining, -1 when no TTL, -2 when the key is missing
    function Ttl(const AKey: string): Integer;
    /// PERSIST: True when a TTL was removed
    function Persist(const AKey: string): Boolean;
    /// INCR / DECR: read-modify-write on the integer value (starts at 0)
    function Incr(const AKey: string): Int64;
    function Decr(const AKey: string): Int64;
    /// MGET: one result per key, in order
    function MGet(const AKeys: TArray<string>): TArray<string>;

    { keys }

    /// KEYS with a glob pattern (default *). Expired keys are left out
    function Keys(const APattern: string = '*') : TArray<string>;
    /// FLUSHDB: empties this database (the whole bucket)
    function FlushDb: Boolean;

    { hashes }

    /// HSET: 1 when the field is new, 0 when it replaced an existing one
    function HSet(const AKey, AField, AValue: string): Integer;
    function HGet(const AKey, AField: string): string;
    /// HDEL: True when the field was there
    function HDel(const AKey, AField: string): Boolean;
    function HExists(const AKey, AField: string): Boolean;
    function HGetAll(const AKey: string): TArray<TPair<string, string>>;
    function HLen(const AKey: string): Integer;

    { sets }

    /// SADD: how many members were newly added
    function SAdd(const AKey: string; const AMembers: TArray<string>): Integer;
    /// SREM: how many members were removed
    function SRem(const AKey: string; const AMembers: TArray<string>): Integer;
    function SMembers(const AKey: string): TArray<string>;
    function SIsMember(const AKey, AMember: string): Boolean;
    function SCard(const AKey: string): Integer;

    { pub/sub - plain core NATS subjects, not JetStream }

    /// PUBLISH: returns the number of LOCAL subscriptions on the channel
    function Publish(const AChannel, AMessage: string): Integer;
    /// SUBSCRIBE: the handler runs on the connection's consumer thread
    function Subscribe(const AChannel: string;
      AHandler: TProc<string, string>): Integer;
    procedure Unsubscribe(AId: Integer);

    /// PING: True once the PING is on the wire (a dead connection raises)
    function Ping: Boolean;

    /// The Redis database index (0..15) and the bucket behind it
    property Database: Integer read FDatabase;
    property Bucket: string read FBucket;
    property StreamName: string read FStream;
  end;

implementation

uses
  Nats.JetStream.Entities;

{ TNatsRedisClient }

constructor TNatsRedisClient.Create(AConnection: TNatsConnection;
  AContext: TJetStreamContext; ADatabase: Integer);
var
  LConfig: TJetStreamKVConfig;
begin
  inherited Create;

  if not Assigned(AConnection) then
    raise ENatsException.Create('A Redis client needs a connection');
  if not Assigned(AContext) then
    raise ENatsException.Create('A Redis client needs a JetStream context');
  if (ADatabase < 0) or (ADatabase > 15) then
    raise ENatsException.CreateFmt(
      'Redis databases are 0..15, got %d', [ADatabase]);

  FConnection := AConnection;
  FContext := AContext;
  FDatabase := ADatabase;
  FBucket := 'redis_' + ADatabase.ToString;

  LConfig := Default(TJetStreamKVConfig);
  LConfig.Bucket := FBucket;
  LConfig.History := 1;   { Redis keeps one value per key }
  FKV := TJetStreamKV.CreateBucket(AContext, LConfig);
  FStream := FKV.StreamName;

  FActiveSubs := TList<Integer>.Create;
  FSubChannels := TDictionary<Integer, string>.Create;
  FChannelCounts := TDictionary<string, Integer>.Create;
end;

destructor TNatsRedisClient.Destroy;
begin
  { leave no subscriptions behind }
  while FActiveSubs.Count > 0 do
  begin
    try
      FConnection.Unsubscribe(FActiveSubs[0]);
    except
      on E: Exception do ;   // the connection is probably gone
    end;
    FActiveSubs.Delete(0);
  end;

  FChannelCounts.Free;
  FSubChannels.Free;
  FActiveSubs.Free;
  FKV.Free;
  inherited;
end;

class function TNatsRedisClient.Encode(const AValue: string): string;
begin
  { unpadded base64url, built from the plain encoder the way the object store
    does: strip the 76-column line breaks, swap +/ for -_, drop the padding }
  Result := TNetEncoding.Base64.EncodeBytesToString(TEncoding.UTF8.GetBytes(AValue));
  Result := Result.Replace(#13, '', [rfReplaceAll]).Replace(#10, '', [rfReplaceAll]);
  Result := Result.Replace('+', '-', [rfReplaceAll]).Replace('/', '_', [rfReplaceAll]);
  Result := Result.TrimRight(['=']);
end;

class function TNatsRedisClient.Decode(const AValue: string): string;
var
  LPadded: string;
begin
  { the reverse of Encode: swap back, restore the padding the standard decoder
    expects, then decode }
  LPadded := AValue.Replace('-', '+', [rfReplaceAll]).Replace('_', '/', [rfReplaceAll]);
  while Length(LPadded) mod 4 <> 0 do
    LPadded := LPadded + '=';

  Result := TEncoding.UTF8.GetString(
    TNetEncoding.Base64.DecodeStringToBytes(LPadded));
end;

class function TNatsRedisClient.MatchesPattern(const APattern, AValue: string): Boolean;
var
  LPi, LVi: Integer;
  LStar, LStarVi: Integer;
begin
  { classic glob with * (any run) and ? (one character), with backtracking so
    a late match can recover from a greedy * }
  LPi := 1;
  LVi := 1;
  LStar := 0;
  LStarVi := 0;

  while LVi <= Length(AValue) do
  begin
    if (LPi <= Length(APattern)) and
       ((APattern[LPi] = AValue[LVi]) or (APattern[LPi] = '?')) then
    begin
      Inc(LPi);
      Inc(LVi);
    end
    else if (LPi <= Length(APattern)) and (APattern[LPi] = '*') then
    begin
      LStar := LPi;
      LStarVi := LVi;
      Inc(LPi);
    end
    else if LStar > 0 then
    begin
      Inc(LStarVi);
      LPi := LStar + 1;
      LVi := LStarVi;
    end
    else
      Exit(False);
  end;

  while (LPi <= Length(APattern)) and (APattern[LPi] = '*') do
    Inc(LPi);

  Result := LPi > Length(APattern);
end;

procedure TNatsRedisClient.CheckKey(const AKey: string);
begin
  { NOT the KV CheckKey: the key never reaches a subject raw - it is encoded - 
    so Redis keys with ':', spaces and so on are all fine. Only emptiness is 
    refused, because an empty key would produce an empty subject token }
  if AKey.IsEmpty then
    raise ENatsException.Create('A Redis key cannot be empty');
end;

function TNatsRedisClient.ValueSubject(const AKey: string): string;
begin
  Result := 'k.' + Encode(AKey);
end;

function TNatsRedisClient.TtlSubject(const AKey: string): string;
begin
  Result := 't.' + Encode(AKey);
end;

function TNatsRedisClient.HashPrefix(const AKey: string): string;
begin
  Result := 'h.' + Encode(AKey) + '.';
end;

function TNatsRedisClient.HashSubject(const AKey, AField: string): string;
begin
  Result := HashPrefix(AKey) + Encode(AField);
end;

function TNatsRedisClient.SetPrefix(const AKey: string): string;
begin
  Result := 's.' + Encode(AKey) + '.';
end;

function TNatsRedisClient.SetSubject(const AKey, AMember: string): string;
begin
  Result := SetPrefix(AKey) + Encode(AMember);
end;

function TNatsRedisClient.ExpiryMillis: Int64;
begin
  Result := DateTimeToUnix(Now, True) * 1000 + MillisecondOf(Now);
end;

function TNatsRedisClient.IsExpired(const AKey: string): Boolean;
var
  LEntry: TKVEntry;
  LExpiry: Int64;
begin
  Result := False;
  if FKV.Get(TtlSubject(AKey), LEntry) then
    if TryStrToInt64(LEntry.ValueString, LExpiry) and (LExpiry <= ExpiryMillis) then
      Result := True;
end;

{ strings }

function TNatsRedisClient.SetValue(const AKey, AValue: string; AExpirySeconds: Int64): Boolean;
begin
  CheckKey(AKey);
  FKV.Put(ValueSubject(AKey), AValue);

  if AExpirySeconds > 0 then
    FKV.Put(TtlSubject(AKey), IntToStr(ExpiryMillis + AExpirySeconds * 1000))
  else
    FKV.Delete(TtlSubject(AKey));   { SET without EX clears a previous TTL }

  Result := True;
end;

function TNatsRedisClient.GetValue(const AKey: string): string;
var
  LEntry: TKVEntry;
begin
  CheckKey(AKey);
  Result := '';

  if IsExpired(AKey) then
    Exit;

  if FKV.Get(ValueSubject(AKey), LEntry) then
    Result := LEntry.ValueString;
end;

function TNatsRedisClient.SetNx(const AKey, AValue: string): Boolean;
begin
  CheckKey(AKey);
  try
    FKV.PutIfAbsent(ValueSubject(AKey), AValue);
    Result := True;
  except
    on E: EJetStreamKVError do
      Result := False;
  end;
end;

function TNatsRedisClient.GetSet(const AKey, AValue: string): string;
begin
  Result := GetValue(AKey);
  SetValue(AKey, AValue);
end;

function TNatsRedisClient.Del(const AKeys: TArray<string>): Integer;
var
  LKey: string;
  LRequest: TJetStreamPurgeRequest;
begin
  Result := 0;
  for LKey in AKeys do
  begin
    CheckKey(LKey);
    if Exists(LKey) = 0 then
      Continue;

    { remove everything that belongs to the key - the value, its TTL, every
      hash field and every set member - in four filtered purges, so no 
      tombstones are left behind. The filters are the SUBJECTS the KV layer
      actually stores on ($KV.<bucket>.k.<enc> ...), not the bare k./t./h./s.
      tokens - a filter that matches nothing purges nothing }
    LRequest := Default(TJetStreamPurgeRequest);
    LRequest.Filter := FKV.KeySubject(ValueSubject(LKey));
    FContext.PurgeStream(FStream, LRequest);
    LRequest.Filter := FKV.KeySubject(TtlSubject(LKey));
    FContext.PurgeStream(FStream, LRequest);
    LRequest.Filter := FKV.KeySubject(HashPrefix(LKey) + '>');
    FContext.PurgeStream(FStream, LRequest);
    LRequest.Filter := FKV.KeySubject(SetPrefix(LKey) + '>');
    FContext.PurgeStream(FStream, LRequest);

    Inc(Result);
  end;
end;

function TNatsRedisClient.Exists(const AKey: string): Integer;
var
  LEntry: TKVEntry;
  LSubject: string;
begin
  CheckKey(AKey);

  if IsExpired(AKey) then
    Exit(0);

  if FKV.Get(ValueSubject(AKey), LEntry) then
    Exit(1);

  { a key used only as a hash or a set has no value subject - look for its 
    fields. O(bucket), documented }
  for LSubject in FKV.Keys do
    if LSubject.StartsWith(HashPrefix(AKey)) or LSubject.StartsWith(SetPrefix(AKey)) then
      Exit(1);

  Result := 0;
end;

function TNatsRedisClient.Expire(const AKey: string; ASeconds: Int64): Boolean;
begin
  CheckKey(AKey);
  if Exists(AKey) = 0 then
    Exit(False);

  FKV.Put(TtlSubject(AKey), IntToStr(ExpiryMillis + ASeconds * 1000));
  Result := True;
end;

function TNatsRedisClient.Ttl(const AKey: string): Integer;
var
  LEntry: TKVEntry;
  LExpiry, LRemaining: Int64;
begin
  CheckKey(AKey);
  if Exists(AKey) = 0 then
    Exit(-2);

  if not FKV.Get(TtlSubject(AKey), LEntry) then
    Exit(-1);

  if not TryStrToInt64(LEntry.ValueString, LExpiry) then
    Exit(-1);

  LRemaining := (LExpiry - ExpiryMillis) div 1000;
  if LRemaining < 0 then
    LRemaining := 0;
  Result := Integer(LRemaining);
end;

function TNatsRedisClient.Persist(const AKey: string): Boolean;
var
  LEntry: TKVEntry;
begin
  CheckKey(AKey);
  if Exists(AKey) = 0 then
    Exit(False);

  if not FKV.Get(TtlSubject(AKey), LEntry) then
    Exit(False);

  FKV.Delete(TtlSubject(AKey));
  Result := True;
end;

function TNatsRedisClient.AddToKey(const AKey: string; ADelta: Int64): Int64;
var
  LEntry: TKVEntry;
  LCurrent: Int64;
begin
  CheckKey(AKey);

  LCurrent := 0;
  if (not IsExpired(AKey)) and FKV.Get(ValueSubject(AKey), LEntry) then
    if not TryStrToInt64(LEntry.ValueString, LCurrent) then
      raise ENatsException.CreateFmt(
        'The value of [%s] is not an integer', [AKey]);

  Result := LCurrent + ADelta;
  FKV.Put(ValueSubject(AKey), IntToStr(Result));
end;

function TNatsRedisClient.Incr(const AKey: string): Int64;
begin
  Result := AddToKey(AKey, 1);
end;

function TNatsRedisClient.Decr(const AKey: string): Int64;
begin
  Result := AddToKey(AKey, -1);
end;

function TNatsRedisClient.MGet(const AKeys: TArray<string>): TArray<string>;
var
  LIndex: Integer;
begin
  SetLength(Result, Length(AKeys));
  for LIndex := 0 to High(AKeys) do
    Result[LIndex] := GetValue(AKeys[LIndex]);
end;

{ keys }

function TNatsRedisClient.Keys(const APattern: string): TArray<string>;
var
  LSubjects: TArray<string>;
  LSubject, LKey: string;
  LResult: TList<string>;
  LIndex, LPos: Integer;
  LFound: Boolean;
begin
  LResult := TList<string>.Create;
  try
    LSubjects := FKV.Keys;
    for LSubject in LSubjects do
    begin
      { every subject is "<type>.<b64key>[.<b64field>]" - the key is the 
        first encoded token after the type prefix }
      if Length(LSubject) < 3 then
        Continue;

      LPos := Pos('.', LSubject, 4);
      if LPos = 0 then
        LKey := LSubject.Substring(2)
      else
        LKey := LSubject.Substring(2, LPos - 3);

      try
        LKey := Decode(LKey);
      except
        Continue;   // not one of ours
      end;

      if IsExpired(LKey) then
        Continue;
      if not MatchesPattern(APattern, LKey) then
        Continue;

      LFound := False;
      for LIndex := 0 to LResult.Count - 1 do
        if LResult[LIndex] = LKey then
        begin
          LFound := True;
          Break;
        end;
      if not LFound then
        LResult.Add(LKey);
    end;

    Result := LResult.ToArray;
  finally
    LResult.Free;
  end;
end;

function TNatsRedisClient.FlushDb: Boolean;
begin
  { one purge empties the whole bucket - values, TTLs, fields, members }
  FContext.PurgeStream(FStream);
  Result := True;
end;

{ hashes }

function TNatsRedisClient.HSet(const AKey, AField, AValue: string): Integer;
var
  LEntry: TKVEntry;
begin
  CheckKey(AKey);

  if FKV.Get(HashSubject(AKey, AField), LEntry) then
    Result := 0
  else
    Result := 1;

  FKV.Put(HashSubject(AKey, AField), AValue);
end;

function TNatsRedisClient.HGet(const AKey, AField: string): string;
var
  LEntry: TKVEntry;
begin
  CheckKey(AKey);
  Result := '';
  if FKV.Get(HashSubject(AKey, AField), LEntry) then
    Result := LEntry.ValueString;
end;

function TNatsRedisClient.HDel(const AKey, AField: string): Boolean;
var
  LEntry: TKVEntry;
begin
  CheckKey(AKey);
  Result := FKV.Get(HashSubject(AKey, AField), LEntry);
  if Result then
    FKV.Delete(HashSubject(AKey, AField));
end;

function TNatsRedisClient.HExists(const AKey, AField: string): Boolean;
var
  LEntry: TKVEntry;
begin
  CheckKey(AKey);
  Result := FKV.Get(HashSubject(AKey, AField), LEntry);
end;

function TNatsRedisClient.HGetAll(const AKey: string): TArray<TPair<string, string>>;
var
  LSubjects: TArray<string>;
  LSubject, LPrefix, LField: string;
  LEntry: TKVEntry;
  LResult: TList<TPair<string, string>>;
begin
  CheckKey(AKey);

  LPrefix := HashPrefix(AKey);
  LResult := TList<TPair<string, string>>.Create;
  try
    LSubjects := FKV.Keys;
    for LSubject in LSubjects do
      if LSubject.StartsWith(LPrefix) then
      begin
        try
          LField := Decode(LSubject.Substring(Length(LPrefix)));
        except
          Continue;
        end;

        if FKV.Get(LSubject, LEntry) then
          LResult.Add(TPair<string, string>.Create(LField, LEntry.ValueString));
      end;

    Result := LResult.ToArray;
  finally
    LResult.Free;
  end;
end;

function TNatsRedisClient.HLen(const AKey: string): Integer;
var
  LSubject: string;
begin
  CheckKey(AKey);
  Result := 0;
  for LSubject in FKV.Keys do
    if LSubject.StartsWith(HashPrefix(AKey)) then
      Inc(Result);
end;

{ sets }

function TNatsRedisClient.SAdd(const AKey: string; const AMembers: TArray<string>): Integer;
var
  LMember: string;
  LEntry: TKVEntry;
begin
  CheckKey(AKey);
  Result := 0;
  for LMember in AMembers do
    if not FKV.Get(SetSubject(AKey, LMember), LEntry) then
    begin
      FKV.Put(SetSubject(AKey, LMember), '1');
      Inc(Result);
    end;
end;

function TNatsRedisClient.SRem(const AKey: string; const AMembers: TArray<string>): Integer;
var
  LMember: string;
  LEntry: TKVEntry;
begin
  CheckKey(AKey);
  Result := 0;
  for LMember in AMembers do
    if FKV.Get(SetSubject(AKey, LMember), LEntry) then
    begin
      FKV.Delete(SetSubject(AKey, LMember));
      Inc(Result);
    end;
end;

function TNatsRedisClient.SMembers(const AKey: string): TArray<string>;
var
  LSubject, LPrefix, LMember: string;
  LResult: TList<string>;
begin
  CheckKey(AKey);

  LPrefix := SetPrefix(AKey);
  LResult := TList<string>.Create;
  try
    for LSubject in FKV.Keys do
      if LSubject.StartsWith(LPrefix) then
      begin
        try
          LMember := Decode(LSubject.Substring(Length(LPrefix)));
          LResult.Add(LMember);
        except
          Continue;
        end;
      end;

    Result := LResult.ToArray;
  finally
    LResult.Free;
  end;
end;

function TNatsRedisClient.SIsMember(const AKey, AMember: string): Boolean;
var
  LEntry: TKVEntry;
begin
  CheckKey(AKey);
  Result := FKV.Get(SetSubject(AKey, AMember), LEntry);
end;

function TNatsRedisClient.SCard(const AKey: string): Integer;
var
  LSubject: string;
begin
  CheckKey(AKey);
  Result := 0;
  for LSubject in FKV.Keys do
    if LSubject.StartsWith(SetPrefix(AKey)) then
      Inc(Result);
end;

{ pub/sub }

function TNatsRedisClient.Publish(const AChannel, AMessage: string): Integer;
begin
  FConnection.Publish(AChannel, AMessage);

  { Redis reports the server-wide receiver count; NATS has no such notion, so 
    this reports how many subscriptions THIS client holds on the channel }
  if not FChannelCounts.TryGetValue(AChannel, Result) then
    Result := 0;
end;

function TNatsRedisClient.Subscribe(const AChannel: string;
  AHandler: TProc<string, string>): Integer;
begin
  Result := FConnection.Subscribe(AChannel,
    procedure (const AMsg: TNatsArgsMSG)
    begin
      { runs on the connection's consumer thread - the handler must be 
        thread safe, and must not call back into this client }
      AHandler(AChannel, AMsg.Payload);
    end);

  FActiveSubs.Add(Result);
  FSubChannels.AddOrSetValue(Result, AChannel);
  if FChannelCounts.ContainsKey(AChannel) then
    FChannelCounts[AChannel] := FChannelCounts[AChannel] + 1
  else
    FChannelCounts.Add(AChannel, 1);
end;

procedure TNatsRedisClient.Unsubscribe(AId: Integer);
var
  LChannel: string;
  LIndex: Integer;
begin
  FConnection.Unsubscribe(AId);

  if FSubChannels.TryGetValue(AId, LChannel) then
  begin
    FSubChannels.Remove(AId);
    if FChannelCounts.ContainsKey(LChannel) then
    begin
      FChannelCounts[LChannel] := FChannelCounts[LChannel] - 1;
      if FChannelCounts[LChannel] <= 0 then
        FChannelCounts.Remove(LChannel);
    end;
  end;

  for LIndex := 0 to FActiveSubs.Count - 1 do
    if FActiveSubs[LIndex] = AId then
    begin
      FActiveSubs.Delete(LIndex);
      Break;
    end;
end;

function TNatsRedisClient.Ping: Boolean;
begin
  { the connection answers the PONG itself; a dead connection raises on the 
    write, which is the failure signal }
  FConnection.Ping;
  Result := True;
end;

end.