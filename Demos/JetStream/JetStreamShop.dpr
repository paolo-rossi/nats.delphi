{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{******************************************************************************}
{                                                                              }
{  The Redis-shaped client (Nats.Redis.pas) in a real-life shape: an           }
{  e-commerce checkout. One Redis database carries the whole shop, the way    }
{  it would with redis-server:                                                 }
{                                                                              }
{    catalog cache with TTL        SET/GET/EXPIRE/TTL                          }
{    shopping cart                 HSET/HGET/HGETALL/HDEL/HLEN                 }
{    stock counters                INCR/DECR (guarded)                         }
{    checkout lock                 SETNX + EXPIRE (lease) + DEL                }
{    order events                  PUBLISH/SUBSCRIBE                           }
{                                                                              }
{  Requires a nats-server with JetStream enabled, e.g.:                       }
{      nats-server -js                                                         }
{                                                                              }
{  Database 1 (bucket redis_1) is deleted at start and in cleanup.            }
{******************************************************************************}
program JetStreamShop;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.SyncObjs,
  System.Generics.Collections,

  Nats.Consts in '..\Source\Nats.Consts.pas',
  Nats.Socket in '..\Source\Nats.Socket.pas',
  Nats.Socket.Indy in '..\Source\Nats.Socket.Indy.pas',
  Nats.Connection in '..\Source\Nats.Connection.pas',
  Nats.JetStream.Client in '..\Source\Nats.JetStream.Client.pas',
  Nats.JetStream.KV in '..\Source\Nats.JetStream.KV.pas',
  Nats.Redis in '..\Source\Nats.Redis.pas';

type
  { the pub/sub handler runs on the connection's consumer thread, so received
    order events are buffered under a lock and read by the main thread }
  TOrderFeed = class
  private
    FLock: TCriticalSection;
    FMessages: TList<string>;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Add(const AMessage: string);
    function Count: Integer;
    function Item(AIndex: Integer): string;
  end;

var
  GConn: TNatsConnection;
  GJs: TJetStreamContext;
  GRedis: TNatsRedisClient;      { database 1 }
  GFeed: TOrderFeed;

{ TOrderFeed }

constructor TOrderFeed.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
  FMessages := TList<string>.Create;
end;

destructor TOrderFeed.Destroy;
begin
  FMessages.Free;
  FLock.Free;
  inherited;
end;

procedure TOrderFeed.Add(const AMessage: string);
begin
  FLock.Enter;
  try
    FMessages.Add(AMessage);
  finally
    FLock.Leave;
  end;
end;

function TOrderFeed.Count: Integer;
begin
  FLock.Enter;
  try
    Result := FMessages.Count;
  finally
    FLock.Leave;
  end;
end;

function TOrderFeed.Item(AIndex: Integer): string;
begin
  FLock.Enter;
  try
    Result := FMessages[AIndex];
  finally
    FLock.Leave;
  end;
end;

procedure Banner(const ATitle: string);
begin
  Writeln;
  Writeln('=== ', ATitle, ' ', StringOfChar('=', 52 - Length(ATitle)));
end;

procedure DemoConnect;
begin
  Banner('Connecting');
  GConn := TNatsConnection.Create
    .SetName('JetStreamShop')
    .SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, 5000);

  GConn.Open(nil);
  if not GConn.WaitForReady(5000) then
    raise Exception.Create('The handshake did not complete: ' + GConn.LastError);

  GJs := TJetStreamContext.Create(GConn);
  Writeln('  connected, server max_payload ', GConn.MaxPayload);

  try
    TJetStreamKV.DeleteBucket(GJs, 'redis_1');
  except
    on E: EJetStreamApiError do ;
  end;

  GRedis := TNatsRedisClient.Create(GConn, GJs, 1);
  Writeln('  redis db ', GRedis.Database, ' (bucket ', GRedis.Bucket, ') ready');
end;

procedure DemoCatalog;
begin
  Banner('Catalog cache (SET with EX / GET / TTL)');

  { the catalog is fetched from the database once and cached for a minute }
  GRedis.SetValue('catalog:sku-1001', '{"name":"Wireless Mouse","price":29.90}', 60);
  GRedis.SetValue('catalog:sku-1002', '{"name":"Mechanical Keyboard","price":89.00}', 60);

  Writeln('  SET catalog:sku-1001 with EX 60 -> cached');
  Writeln('  GET -> ', GRedis.GetValue('catalog:sku-1001'));
  Writeln('  TTL -> ', GRedis.Ttl('catalog:sku-1001'), ' s remaining');
end;

procedure DemoCart;
var
  LItems: TArray<TPair<string, string>>;
  LPair: TPair<string, string>;
begin
  Banner('Shopping cart (hash)');

  GRedis.HSet('cart:user42', 'sku-1001', '2');
  GRedis.HSet('cart:user42', 'sku-1002', '1');
  Writeln('  HSET cart:user42 sku-1001 2, sku-1002 1');
  Writeln('  HLEN -> ', GRedis.HLen('cart:user42'));
  Writeln('  HGET sku-1001 -> ', GRedis.HGet('cart:user42', 'sku-1001'));

  LItems := GRedis.HGetAll('cart:user42');
  Writeln('  HGETALL:');
  for LPair in LItems do
    Writeln('    ', LPair.Key, ' x', LPair.Value);

  { the user removes the keyboard from the cart }
  GRedis.HDel('cart:user42', 'sku-1002');
  Writeln('  HDEL sku-1002 -> HLEN now ', GRedis.HLen('cart:user42'));
  Writeln('  HDEL sku-1002 again -> ', GRedis.HDel('cart:user42', 'sku-1002'),
    ' (False = field was not there)');
end;

procedure DemoStock;
var
  LStock: Integer;
begin
  Banner('Stock counters (INCR / DECR, guarded)');

  GRedis.SetValue('stock:sku-1001', '5');
  Writeln('  SET stock:sku-1001 = 5');
  Writeln('  DECR -> ', GRedis.Decr('stock:sku-1001'), ' left');
  Writeln('  INCR -> ', GRedis.Incr('stock:sku-1001'), ' left (a return arrived)');

  { the guard: never sell below zero. Read-check-DECR; note this is not atomic
    - WATCH/MULTI is deliberately not part of the Redis surface }
  LStock := StrToInt(GRedis.GetValue('stock:sku-1001'));
  if LStock > 0 then
    Writeln('  guarded DECR -> ', GRedis.Decr('stock:sku-1001'), ' left')
  else
    Writeln('  guarded DECR refused: out of stock');
end;

procedure DemoCheckout;
begin
  Banner('Checkout lock (SETNX + EXPIRE lease + DEL)');

  { two requests race to check out; SETNX wins exactly one of them }
  if GRedis.SetNx('lock:checkout:user42', 'txn-1001') then
    Writeln('  SETNX lock:checkout:user42 -> acquired (request txn-1001)')
  else
    Writeln('  !! the lock should have been free');

  if GRedis.SetNx('lock:checkout:user42', 'txn-1002') then
    Writeln('  !! a second request got the lock')
  else
    Writeln('  SETNX again -> False (txn-1002 must wait)');

  { a lease so a crashed checkout cannot hold the lock forever }
  GRedis.Expire('lock:checkout:user42', 30);
  Writeln('  EXPIRE lock 30 -> TTL ', GRedis.Ttl('lock:checkout:user42'), ' s (lease)');

  { checkout finishes and releases the lock }
  GRedis.Del(['lock:checkout:user42']);
  Writeln('  DEL lock -> released (', GRedis.Exists('lock:checkout:user42'),
    ' = gone)');
end;

procedure DemoOrderFeed;
var
  LSid: Integer;
  LDeadline: TDateTime;
begin
  Banner('Order events (PUBLISH / SUBSCRIBE)');

  GFeed := TOrderFeed.Create;
  LSid := GRedis.Subscribe('orders.completed',
    procedure (AChannel, AMessage: string)
    begin
      { runs on the consumer thread: only thread-safe state here }
      GFeed.Add(AMessage);
    end);

  Writeln('  subscribed to orders.completed (sid ', LSid, ')');
  Writeln('  PUBLISH -> ', GRedis.Publish('orders.completed',
    '{"order":1042,"total":29.90,"sku":"sku-1001"}'), ' local subscriber(s)');

  { deliveries arrive asynchronously }
  LDeadline := Now + 5 / SecsPerDay;
  while (GFeed.Count = 0) and (Now < LDeadline) do
    Sleep(50);

  if GFeed.Count > 0 then
    Writeln('  subscriber received: ', GFeed.Item(0))
  else
    Writeln('  !! no order event arrived');

  GRedis.Unsubscribe(LSid);
  Writeln('  UNSUBSCRIBE -> PUBLISH now reports ',
    GRedis.Publish('orders.completed', '{"order":1043}'), ' subscriber(s)');
end;

procedure Cleanup;
begin
  Banner('Cleanup');

  if Assigned(GConn) and GConn.Connected then
    try
      if Assigned(GRedis) then
      begin
        TJetStreamKV.DeleteBucket(GJs, GRedis.Bucket);
        Writeln('  removed bucket ', GRedis.Bucket);
      end;
    except
      on E: Exception do
        Writeln('  (cleanup failed: ', E.ClassName, ': ', E.Message, ')');
    end;

  GFeed.Free;
  GRedis.Free;
  GJs.Free;
  GConn.Free;
end;

begin
  Writeln('nats.delphi - Redis layer: an e-commerce checkout');
  Writeln(StringOfChar('=', 56));
  try
    try
      DemoConnect;
      DemoCatalog;
      DemoCart;
      DemoStock;
      DemoCheckout;
      DemoOrderFeed;

      Banner('Done');
      Writeln('  everything worked. The shop runs on one Redis database over JetStream.');
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
