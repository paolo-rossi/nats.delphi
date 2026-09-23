{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{******************************************************************************}
{                                                                              }
{  JetStream as a session store, straight on the NATS JetStream API. Every     }
{  HTTP session is a key in a Key/Value bucket, and the bucket's TTL is the    }
{  session lifetime:                                                            }
{                                                                              }
{    create only if new        PutIfAbsent (revision on success)               }
{    create or overwrite       Put                                                }
{    read                     Get                                                  }
{    sliding TTL               Touch (re-Put: fresh bucket TTL)                }
{    optimistic concurrency    Update on a revision                            }
{    remove                    Delete (a tombstone, not a removal)             }
{    list                      Keys                                            }
{                                                                              }
{  The TTL belongs to the BUCKET, not to one key - so sessions with different  }
{  lifetimes need separate buckets (the expiry demo below uses a 5-second      }
{  bucket on purpose).                                                          }
{                                                                              }
{  Requires a nats-server with JetStream enabled, e.g.:                        }
{      nats-server -js                                                         }
{                                                                              }
{  Every bucket name is NUID-suffixed so consecutive runs never collide, and   }
{  everything is deleted again in the cleanup section.                         }
{******************************************************************************}
program JetStreamSessions;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.JSON,
  System.Generics.Collections,

  Nats.Consts in '..\Source\Nats.Consts.pas',
  Nats.Socket in '..\Source\Nats.Socket.pas',
  Nats.Socket.Indy in '..\Source\Nats.Socket.Indy.pas',
  Nats.Connection in '..\Source\Nats.Connection.pas',
  Nats.Exceptions in '..\Source\Nats.Exceptions.pas',
  Nats.Nuid in '..\Source\Nats.Nuid.pas',
  Nats.JetStream.Client in '..\Source\Nats.JetStream.Client.pas',
  Nats.JetStream.Entities in '..\Source\Nats.JetStream.Entities.pas',
  Nats.JetStream.KV in '..\Source\Nats.JetStream.KV.pas';

type
  { A session store over JetStream Key/Value. Keys are stored as "session.<id>"
    - dots are fine in a KV key. Callers pass the bare id; the store owns the
    prefix and strips it again when listing. }
  TSessionStore = class
  private
    FKV: TJetStreamKV;
    FPrefix: string;
  public
    constructor Create(AContext: TJetStreamContext; const ABucket: string;
      const ATTL: TJetStreamDuration);
    destructor Destroy; override;

    { creates the session only if the id is not taken yet }
    function CreateSession(const ASessionId: string; const AData: string): UInt64;
    { creates or overwrites }
    function PutSession(const ASessionId: string; const AData: string): UInt64;
    { False when the session does not exist or has expired }
    function GetSession(const ASessionId: string; out AData: string): Boolean;
    { sliding window: re-Put resets the TTL and bumps the revision }
    function TouchSession(const ASessionId: string): UInt64;
    { optimistic concurrency on the revision }
    function UpdateSession(const ASessionId: string; const AData: string;
      ARevision: UInt64): UInt64;
    { a tombstone, so the history survives }
    procedure DeleteSession(const ASessionId: string);
    { every session still held }
    function ListSessions: TArray<string>;

    property Bucket: TJetStreamKV read FKV;
  end;

var
  GConn: TNatsConnection;
  GJs: TJetStreamContext;
  GSessions: TSessionStore;      { the real store: 30 minute lifetime }
  GShortTTL: TSessionStore;     { the expiry demo: 5 second lifetime }

procedure Banner(const ATitle: string);
begin
  Writeln;
  Writeln('=== ', ATitle, ' ', StringOfChar('=', 52 - Length(ATitle)));
end;

{ TSessionStore }

constructor TSessionStore.Create(AContext: TJetStreamContext; const ABucket: string;
  const ATTL: TJetStreamDuration);
var
  LConfig: TJetStreamKVConfig;
begin
  inherited Create;

  FPrefix := 'session.';

  LConfig := Default(TJetStreamKVConfig);
  LConfig.Bucket := ABucket;
  LConfig.TTL := ATTL;            { the session lifetime }
  LConfig.History := 5;           { keep a few revisions per session }
  FKV := TJetStreamKV.CreateBucket(AContext, LConfig);
end;

destructor TSessionStore.Destroy;
begin
  FKV.Free;
  inherited;
end;

function TSessionStore.CreateSession(const ASessionId: string; const AData: string): UInt64;
begin
  Result := FKV.PutIfAbsent(FPrefix + ASessionId, AData);
end;

function TSessionStore.PutSession(const ASessionId: string; const AData: string): UInt64;
begin
  Result := FKV.Put(FPrefix + ASessionId, AData);
end;

function TSessionStore.GetSession(const ASessionId: string; out AData: string): Boolean;
var
  LEntry: TKVEntry;
begin
  Result := FKV.Get(FPrefix + ASessionId, LEntry);
  if Result then
    AData := LEntry.ValueString;
end;

function TSessionStore.TouchSession(const ASessionId: string): UInt64;
var
  LData: string;
begin
  { The sliding window: every access buys the session another full lifetime.
    Touching a session that is not there is an error, not a way to create one }
  if not GetSession(ASessionId, LData) then
    raise ENatsException.CreateFmt(
      'Cannot touch session [%s]: it does not exist (or has expired)', [ASessionId]);

  Result := FKV.Put(FPrefix + ASessionId, LData);
end;

function TSessionStore.UpdateSession(const ASessionId: string; const AData: string;
  ARevision: UInt64): UInt64;
begin
  Result := FKV.Update(FPrefix + ASessionId, AData, ARevision);
end;

procedure TSessionStore.DeleteSession(const ASessionId: string);
begin
  FKV.Delete(FPrefix + ASessionId);
end;

function TSessionStore.ListSessions: TArray<string>;
var
  LKey: string;
  LResult: TList<string>;
begin
  LResult := TList<string>.Create;
  try
    for LKey in FKV.Keys do
      if LKey.StartsWith(FPrefix) then
        LResult.Add(LKey.Substring(Length(FPrefix)));
    Result := LResult.ToArray;
  finally
    LResult.Free;
  end;
end;

procedure DemoConnect;
begin
  Banner('Connecting');
  GConn := TNatsConnection.Create;
  GConn
    .SetName('JetStreamSessions')
    .SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, 5000)
    .Open(nil);

  if not GConn.WaitForReady(5000) then
    raise Exception.Create('The handshake did not complete: ' + GConn.LastError);

  GJs := TJetStreamContext.Create(GConn);
  Writeln('  connected, server max_payload ', GConn.MaxPayload);
end;

procedure DemoCreateAndRead;
var
  LId: string;
  LData, LBack: string;
  LJson: TJSONObject;
  LRev: UInt64;
begin
  Banner('Sessions: create / read / touch / update');

  { A new HTTP client logs in: create the session only if the id is free }
  LId := 'a1b2c3';
  LData := '{"user":42,"role":"admin","logged_in_at":"2026-08-13T10:00:00Z"}';
  LRev := GSessions.CreateSession(LId, LData);
  Writeln('  create session.', LId, ' -> revision ', LRev);

  { A second login with the same id is a collision - exactly what PutIfAbsent
    is for }
  try
    GSessions.CreateSession(LId, LData);
    Writeln('  !! a duplicate session id was accepted');
  except
    on E: EJetStreamKVError do
      Writeln('  create on an existing session correctly failed');
  end;

  { Every request reads the session }
  if GSessions.GetSession(LId, LBack) then
  begin
    LJson := TJSONObject.ParseJSONValue(LBack) as TJSONObject;
    try
      Writeln('  GET session.', LId, ' -> user ', LJson.GetValue<Integer>('user'),
        ', role "', LJson.GetValue<string>('role'), '"');
    finally
      LJson.Free;
    end;
  end
  else
    Writeln('  !! GET failed: the session should be there');

  { Sliding expiration: touching resets the TTL and bumps the revision }
  LRev := GSessions.TouchSession(LId);
  Writeln('  touch session.', LId, ' -> revision ', LRev, ' (TTL restarted)');

  { Optimistic concurrency: nobody may have changed the session between read
    and write }
  LRev := GSessions.UpdateSession(LId, '{"user":42,"role":"banned"}', LRev);
  Writeln('  CAS update -> revision ', LRev);

  { A second session, so listing has something to show }
  GSessions.CreateSession('x9y8z7', '{"user":7,"role":"guest"}');

  Writeln('  list -> ', Length(GSessions.ListSessions), ' active');
  for LId in GSessions.ListSessions do
    Writeln('    ', LId);
end;

procedure DemoLogout;
var
  LData: string;
begin
  Banner('Logout (delete)');

  GSessions.DeleteSession('a1b2c3');

  { Delete is a tombstone: the reader no longer sees the session, but its
    history is still there }
  if GSessions.GetSession('a1b2c3', LData) then
    Writeln('  !! the deleted session is still readable')
  else
    Writeln('  GET after delete -> session not found (tombstone)');

  Writeln('  list -> ', Length(GSessions.ListSessions), ' active');
end;

procedure DemoExpiry;
var
  LData: string;
begin
  Banner('Session expiry (bucket TTL)');

  GShortTTL.CreateSession('temp', '{"user":99}');
  Writeln('  created session.temp with a 5 second TTL');

  if GShortTTL.GetSession('temp', LData) then
    Writeln('  GET immediately -> found');

  Writeln('  waiting 6.5 s for the TTL to elapse...');
  Sleep(6500);

  { The server dropped the message when MaxAge elapsed }
  if GShortTTL.GetSession('temp', LData) then
    Writeln('  !! the expired session is still readable')
  else
    Writeln('  GET after 6.5 s -> gone, the TTL elapsed');
end;

procedure Cleanup;
begin
  Banner('Cleanup');

  if Assigned(GConn) and GConn.Connected then
  begin
    try
      if Assigned(GSessions) then
        TJetStreamKV.DeleteBucket(GJs, GSessions.Bucket.Bucket);
      if Assigned(GShortTTL) then
        TJetStreamKV.DeleteBucket(GJs, GShortTTL.Bucket.Bucket);
      Writeln('  removed both session buckets');
    except
      on E: Exception do
        Writeln('  (cleanup failed: ', E.ClassName, ': ', E.Message, ')');
    end;
  end;

  GSessions.Free;
  GShortTTL.Free;
  GJs.Free;
  GConn.Free;
end;

begin
  Writeln('nats.delphi - JetStream as a session store');
  Writeln(StringOfChar('=', 56));
  try
    try
      DemoConnect;

      GSessions := TSessionStore.Create(GJs, 'sessions_' + TNUID.NextNuid,
        TJetStreamDuration.FromMinutes(30));
      Writeln('  session bucket TTL: 30 min');
      DemoCreateAndRead;
      DemoLogout;

      GShortTTL := TSessionStore.Create(GJs, 'sessions_short_' + TNUID.NextNuid,
        TJetStreamDuration.FromSeconds(5));
      DemoExpiry;

      Banner('Done');
      Writeln('  everything worked. Sessions live in a JetStream Key/Value bucket.');
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
