{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{******************************************************************************}
unit Nats.Tests.Redis;

{ The Redis-shaped client (Nats.Redis.pas). The encoding and pattern matcher
  are pure and offline; the command matrix is proved against a real server.
  Every live test uses the fixed database 0 (bucket redis_0): Setup deletes a
  bucket a crashed run may have left behind, TearDown deletes it again. }

interface

uses
  System.SysUtils, System.Generics.Collections,

  DUnitX.TestFramework,

  Nats.Consts,
  Nats.Connection,
  Nats.Exceptions,
  Nats.Socket,
  Nats.Socket.Indy,
  Nats.JetStream.Client,
  Nats.JetStream.KV,
  Nats.Redis,

  Nats.Tests.Mocks;

type
  [TestFixture]
  TRedisEncodingTests = class
public
  { the key encoding must round-trip any key, including characters that could 
    never live in a subject token }
  [Test]
  procedure Encode_RoundTrips_KeysWithRedisCharacters;
  [Test]
  procedure Encode_IsUrlSafeAndUnpadded;

  { the glob matcher behind KEYS }
  [Test]
  procedure MatchesPattern_StarAndQuestion;
  [Test]
  procedure MatchesPattern_ExactMatch;
end;

[TestFixture]
[Category('Live')]
TRedisLiveTests = class
private
  FConn: TNatsConnection;
  FJs: TJetStreamContext;
  FRedis: TNatsRedisClient;
  procedure Connect;
public
  [Setup]
  procedure Setup;
  [TearDown]
  procedure TearDown;

  { strings }
  [Test]
  procedure Set_Get_Exists_Del;
  [Test]
  procedure KeysWithColonsAndSpaces_AreStored;
  [Test]
  procedure SetNx_SecondCall_ReturnsFalse;
  [Test]
  procedure GetSet_ReturnsTheOldValue;
  [Test]
  procedure Expire_Ttl_Persist;
  [Test]
  procedure ShortTtl_ExpiresTheKey;
  [Test]
  procedure Incr_StartsAtOne_AndDecr;
  [Test]
  procedure Incr_OnANonInteger_Raises;
  [Test]
  procedure MGet_ReturnsInOrder;

  { keys }
  [Test]
  procedure Keys_FiltersByPattern;
  [Test]
  procedure FlushDb_EmptiesEverything;

  { hashes and sets }
  [Test]
  procedure HSet_HGet_HGetAll_HDel;
  [Test]
  procedure SAdd_SMembers_SIsMember_SCard_SRem;

  { pub/sub }
  [Test]
  procedure PublishSubscribe_RoundTrip;
end;

implementation

const
  HOST = '127.0.0.1';
  CONNECT_TIMEOUT = 5000;
  WAIT_MS = 5000;

var
  GIndySwitchCount: Integer = 0;

procedure UseIndySocket;
begin
  { the registry default is process-wide, so register under a throwaway name -
    see UseMockSocket for the same trick }
  Inc(GIndySwitchCount);
  TNatsSocketRegistry.Register<TNatsSocketIndy>(
    Format('IndyRedis#%d', [GIndySwitchCount]), True);
end;

{ TRedisEncodingTests }

procedure TRedisEncodingTests.Encode_RoundTrips_KeysWithRedisCharacters;
var
  LKeys: TArray<string>;
  LKey, LEncoded, LDecoded: string;
begin
  { the point of the encoding: Redis keys routinely carry characters that are
    not legal in a NATS subject token }
  LKeys := ['user:42', 'a b', 'sue.color', 'reports/2024 Q1.pdf',
    'tab' + #9 + 'key', 'a', ''] ;

  for LKey in LKeys do
  begin
    LEncoded := TNatsRedisClient.Encode(LKey);
    LDecoded := TNatsRedisClient.Decode(LEncoded);
    Assert.AreEqual(LKey, LDecoded,
      Format('round trip of [%s] via [%s]', [LKey, LEncoded]));
  end;
end;

procedure TRedisEncodingTests.Encode_IsUrlSafeAndUnpadded;
var
  LEncoded: string;
begin
  LEncoded := TNatsRedisClient.Encode('hello');
  Assert.IsFalse(LEncoded.Contains('='), 'no padding');
  Assert.IsFalse(LEncoded.Contains('+'), 'no + in a subject');
  Assert.IsFalse(LEncoded.Contains('/'), 'no / in a subject');
  Assert.IsFalse(LEncoded.Contains(#10) or LEncoded.Contains(#13),
    'no line breaks from the base64 encoder');

  { the same value as the object store encoder, since both are unpadded base64url }
  Assert.AreEqual('aGVsbG8', LEncoded);
end;

procedure TRedisEncodingTests.MatchesPattern_ExactMatch;
begin
  Assert.IsTrue(TNatsRedisClient.MatchesPattern('user:42', 'user:42'));
  Assert.IsFalse(TNatsRedisClient.MatchesPattern('user:42', 'user:43'));
  Assert.IsFalse(TNatsRedisClient.MatchesPattern('user:4', 'user:42'));
end;

procedure TRedisEncodingTests.MatchesPattern_StarAndQuestion;
begin
  Assert.IsTrue(TNatsRedisClient.MatchesPattern('*', 'anything'));
  Assert.IsTrue(TNatsRedisClient.MatchesPattern('user:*', 'user:42'));
  Assert.IsTrue(TNatsRedisClient.MatchesPattern('*:42', 'user:42'));
  Assert.IsTrue(TNatsRedisClient.MatchesPattern('a*c', 'abc'));
  Assert.IsTrue(TNatsRedisClient.MatchesPattern('a*c', 'ac'));
  Assert.IsTrue(TNatsRedisClient.MatchesPattern('a?c', 'abc'));
  Assert.IsFalse(TNatsRedisClient.MatchesPattern('a?c', 'ab'));
  Assert.IsFalse(TNatsRedisClient.MatchesPattern('a?c', 'abbc'));
  { as in Redis, a * crosses dots: KEYS sue.* finds sue.color.deep too }
  Assert.IsTrue(TNatsRedisClient.MatchesPattern('sue.*', 'sue.color'));
  Assert.IsTrue(TNatsRedisClient.MatchesPattern('sue.*', 'sue.color.deep'));
  Assert.IsFalse(TNatsRedisClient.MatchesPattern('sue.color', 'sue.color.deep'));
end;

{ TRedisLiveTests }

procedure TRedisLiveTests.Setup;
begin
  UseIndySocket;

  FConn := TNatsConnection.Create;
  FConn.Name := 'RedisLiveConn';
  FConn.SetChannel(HOST, NatsConstants.DEFAULT_PORT, CONNECT_TIMEOUT);

  FJs := TJetStreamContext.Create(FConn);
  FRedis := nil;
end;

procedure TRedisLiveTests.TearDown;
begin
  if Assigned(FConn) and FConn.Connected then
    try
      if Assigned(FRedis) then
        TJetStreamKV.DeleteBucket(FJs, FRedis.Bucket);
    except
      on E: Exception do ;
    end;

  FRedis.Free;
  FJs.Free;
  FConn.Free;
  UseMockSocket;
end;

procedure TRedisLiveTests.Connect;
begin
  FConn.Open(nil);
  Assert.IsTrue(FConn.WaitForReady(WAIT_MS),
    Format('handshake with nats-server at %s:%d did not complete: %s',
      [HOST, NatsConstants.DEFAULT_PORT, FConn.LastError]));

  { a crashed earlier run may have left database 0 behind - the client 
    creates the bucket, so it must not already exist }
  try
    TJetStreamKV.DeleteBucket(FJs, 'redis_0');
  except
    on E: EJetStreamApiError do ;
  end;

  FRedis := TNatsRedisClient.Create(FConn, FJs, 0);
end;

procedure TRedisLiveTests.Set_Get_Exists_Del;
begin
  Connect;

  Assert.IsTrue(FRedis.SetValue('greeting', 'hello'));
  Assert.AreEqual('hello', FRedis.GetValue('greeting'));
  Assert.AreEqual(1, FRedis.Exists('greeting'));

  Assert.AreEqual(1, FRedis.Del(['greeting']));
  Assert.AreEqual('', FRedis.GetValue('greeting'));
  Assert.AreEqual(0, FRedis.Exists('greeting'));
  Assert.AreEqual(0, FRedis.Del(['greeting']), 'del of a missing key counts 0');
end;

procedure TRedisLiveTests.KeysWithColonsAndSpaces_AreStored;
begin
  Connect;

  { the encoding is the whole point: these keys could never be subjects }
  FRedis.SetValue('user:42', 'alice');
  FRedis.SetValue('a b', 'spaced');

  Assert.AreEqual('alice', FRedis.GetValue('user:42'));
  Assert.AreEqual('spaced', FRedis.GetValue('a b'));
  Assert.AreEqual(2, Length(FRedis.Keys('*')));
end;

procedure TRedisLiveTests.SetNx_SecondCall_ReturnsFalse;
begin
  Connect;

  Assert.IsTrue(FRedis.SetNx('lock', 'me'));
  Assert.IsFalse(FRedis.SetNx('lock', 'me'));
  Assert.AreEqual('me', FRedis.GetValue('lock'));
end;

procedure TRedisLiveTests.GetSet_ReturnsTheOldValue;
begin
  Connect;

  FRedis.SetValue('counter', '1');
  Assert.AreEqual('1', FRedis.GetSet('counter', '2'));
  Assert.AreEqual('2', FRedis.GetValue('counter'));
  Assert.AreEqual('', FRedis.GetSet('fresh', 'x'), 'missing key has no old value');
end;

procedure TRedisLiveTests.Expire_Ttl_Persist;
begin
  Connect;

  FRedis.SetValue('shortlived', 'v', 30);
  Assert.IsTrue(FRedis.Ttl('shortlived') > 0, 'a TTL is set');
  Assert.IsTrue(FRedis.Ttl('shortlived') <= 30);

  Assert.IsTrue(FRedis.Persist('shortlived'), 'the TTL was removed');
  Assert.AreEqual(-1, FRedis.Ttl('shortlived'), 'no TTL any more');

  Assert.IsFalse(FRedis.Expire('missing', 10), 'expire of a missing key fails');
  Assert.AreEqual(-2, FRedis.Ttl('missing'), 'missing key answers -2');
end;

procedure TRedisLiveTests.ShortTtl_ExpiresTheKey;
begin
  Connect;

  FRedis.SetValue('temp', 'v', 1);
  Assert.AreEqual('v', FRedis.GetValue('temp'));

  Sleep(1500);

  { the lazy expiry: the key reads as missing once its TTL has elapsed }
  Assert.AreEqual('', FRedis.GetValue('temp'));
  Assert.AreEqual(0, FRedis.Exists('temp'));
  Assert.AreEqual(-2, FRedis.Ttl('temp'));
end;

procedure TRedisLiveTests.Incr_StartsAtOne_AndDecr;
begin
  Connect;

  Assert.AreEqual(Int64(1), FRedis.Incr('visits'));
  Assert.AreEqual(Int64(2), FRedis.Incr('visits'));
  Assert.AreEqual(Int64(3), FRedis.Incr('visits'));
  Assert.AreEqual(Int64(2), FRedis.Decr('visits'));
  Assert.AreEqual('2', FRedis.GetValue('visits'));
end;

procedure TRedisLiveTests.Incr_OnANonInteger_Raises;
begin
  Connect;

  FRedis.SetValue('word', 'hello');
  Assert.WillRaise(
    procedure
    begin
      FRedis.Incr('word');
    end,
    ENatsException);
end;

procedure TRedisLiveTests.MGet_ReturnsInOrder;
var
  LValues: TArray<string>;
begin
  Connect;

  FRedis.SetValue('a', '1');
  FRedis.SetValue('b', '2');

  LValues := FRedis.MGet(['a', 'missing', 'b']);
  Assert.AreEqual(3, Length(LValues));
  Assert.AreEqual('1', LValues[0]);
  Assert.AreEqual('', LValues[1], 'missing key -> empty string');
  Assert.AreEqual('2', LValues[2]);
end;

procedure TRedisLiveTests.Keys_FiltersByPattern;
var
  LKeys: TArray<string>;
begin
  Connect;

  FRedis.SetValue('a.1', 'x');
  FRedis.SetValue('a.2', 'x');
  FRedis.SetValue('b.1', 'x');

  LKeys := FRedis.Keys('*');
  Assert.AreEqual(3, Length(LKeys));

  LKeys := FRedis.Keys('a.*');
  Assert.AreEqual(2, Length(LKeys), 'the glob is applied to the decoded key');
end;

procedure TRedisLiveTests.FlushDb_EmptiesEverything;
begin
  Connect;

  FRedis.SetValue('a', '1');
  FRedis.HSet('h', 'f', 'v');

  Assert.IsTrue(FRedis.FlushDb);
  Assert.AreEqual(0, Length(FRedis.Keys('*')));
  Assert.AreEqual('', FRedis.GetValue('a'));
  Assert.AreEqual('', FRedis.HGet('h', 'f'));
end;

procedure TRedisLiveTests.HSet_HGet_HGetAll_HDel;
var
  LPairs: TArray<TPair<string, string>>;
begin
  Connect;

  Assert.AreEqual(1, FRedis.HSet('user:42', 'name', 'alice'), 'new field');
  Assert.AreEqual(0, FRedis.HSet('user:42', 'name', 'bob'), 'replaced field');
  FRedis.HSet('user:42', 'role', 'admin');

  Assert.AreEqual('bob', FRedis.HGet('user:42', 'name'));
  Assert.IsTrue(FRedis.HExists('user:42', 'role'));
  Assert.AreEqual(2, FRedis.HLen('user:42'));
  Assert.AreEqual(1, FRedis.Exists('user:42'), 'a hash key exists too');

  LPairs := FRedis.HGetAll('user:42');
  Assert.AreEqual(2, Length(LPairs));
  Assert.IsTrue((LPairs[0].Key = 'name') or (LPairs[1].Key = 'name'));

  Assert.IsTrue(FRedis.HDel('user:42', 'role'));
  Assert.IsFalse(FRedis.HExists('user:42', 'role'));
  Assert.AreEqual(1, FRedis.HLen('user:42'));
end;

procedure TRedisLiveTests.SAdd_SMembers_SIsMember_SCard_SRem;
var
  LMembers: TArray<string>;
begin
  Connect;

  Assert.AreEqual(2, FRedis.SAdd('tags', ['red', 'green']));
  Assert.AreEqual(0, FRedis.SAdd('tags', ['red']), 'already a member');
  Assert.AreEqual(2, FRedis.SCard('tags'));
  Assert.IsTrue(FRedis.SIsMember('tags', 'green'));
  Assert.IsFalse(FRedis.SIsMember('tags', 'blue'));

  LMembers := FRedis.SMembers('tags');
  Assert.AreEqual(2, Length(LMembers));

  Assert.AreEqual(1, FRedis.SRem('tags', ['red']));
  Assert.AreEqual(1, FRedis.SCard('tags'));
  Assert.IsFalse(FRedis.SIsMember('tags', 'red'));
end;

procedure TRedisLiveTests.PublishSubscribe_RoundTrip;
var
  LLog: TNatsTestLog;
  LSid: Integer;
begin
  Connect;

  LLog := TNatsTestLog.Create;
  try
    LSid := FRedis.Subscribe('demo.channel',
      procedure (AChannel, AMessage: string)
      begin
        { runs on the consumer thread: only thread-safe state here }
        LLog.AddFmt('%s|%s', [AChannel, AMessage]);
      end);

    Assert.AreEqual(1, FRedis.Publish('demo.channel', 'hello redis'));

    Assert.IsTrue(WaitForCondition(
      function: Boolean
      begin
        Result := LLog.Count > 0;
      end),
      'the subscriber never received the message');

    Assert.AreEqual('demo.channel|hello redis', LLog.Item(0));

    FRedis.Unsubscribe(LSid);
    Assert.AreEqual(0, FRedis.Publish('demo.channel', 'nobody home'),
      'PUBLISH reports the local subscriber count');
  finally
    LLog.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TRedisEncodingTests);
  TDUnitX.RegisterTestFixture(TRedisLiveTests);

end.