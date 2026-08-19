{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.JetStream.ObjectStore;

interface

uses
  System.SysUtils, System.Classes, System.Hash, System.NetEncoding, System.DateUtils,
  System.Generics.Collections,

  Neon.Core.Types,
  Neon.Core.Attributes,

  Nats.Consts,
  Nats.Classes,
  Nats.Nuid,
  Nats.Exceptions,
  Nats.JetStream.Consts,
  Nats.JetStream.Client,
  Nats.JetStream.Entities,
  Nats.JetStream.Message;

type
  /// <summary>
  ///   Per-object settings, carried inside the metadata so a reader knows how
  ///   the object was written
  /// </summary>
  TJetStreamObjectOptions = record
    [NeonInclude(IncludeIf.NotDefault)]
    MaxChunkSize: Integer;
  end;

  /// <summary>
  ///   Everything about one stored object except its bytes
  /// </summary>
  /// <remarks>
  ///   This record IS the message stored on the object's metadata subject, so
  ///   its field names are protocol - the same rule as Nats.Entities.pas. It
  ///   lives here rather than in Nats.JetStream.Entities.pas because it is an
  ///   object-store payload, not a JetStream API type.
  /// </remarks>
  TJetStreamObjectInfo = record
    Name: string;
    [NeonInclude(IncludeIf.NotDefault)]
    Description: string;
    Bucket: string;
    /// <summary>
    ///   Identifies the object's CHUNK subject, and is regenerated on every
    ///   write. That is what lets a new version be uploaded while readers are
    ///   still streaming the old one
    /// </summary>
    Nuid: string;
    /// Total bytes, which is not the sum of anything the reader can check first
    Size: UInt64;
    Chunks: Integer;
    /// <summary>
    ///   'SHA-256=&lt;base64url&gt;' over the WHOLE object. Verified on read -
    ///   an object spread over many messages is exactly the kind of thing that
    ///   can come back short without anything having reported an error
    /// </summary>
    Digest: string;
    /// RFC3339, as text - see TJetStreamStoredMsg for why
    Mtime: string;
    /// <summary>
    ///   A tombstone rather than an absence: the metadata stays, so a name that
    ///   was deleted is distinguishable from one that never existed
    /// </summary>
    Deleted: Boolean;
    [NeonInclude(IncludeIf.NotDefault)]
    Options: TJetStreamObjectOptions;
  end;

  TJetStreamObjectStoreConfig = record
    Bucket: string;
    Description: string;
    /// How long an object lives before the server drops it. 0 means forever
    TTL: TJetStreamDuration;
    MaxBytes: Int64;
    Storage: TJetStreamStorage;
    Replicas: Integer;
    /// 0 uses JetStreamConstants.Obj.DEFAULT_CHUNK_SIZE
    ChunkSize: Integer;
  end;

  TJetStreamObjectStoreStatus = record
    Bucket: string;
    StreamName: string;
    /// Chunks AND metadata records - not the number of objects
    Messages: UInt64;
    Bytes: UInt64;
    TTL: TJetStreamDuration;
  end;

  /// <summary>
  ///   Raised for an unusable bucket or object name, and when a retrieved
  ///   object fails its digest check
  /// </summary>
  EJetStreamObjectError = class(ENatsException);

  /// <summary>
  ///   A handle on one object store bucket
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     Like Key/Value this is a convention over a stream, but a thicker one.
  ///     A bucket is the stream OBJ_&lt;bucket&gt;, capturing two subject
  ///     spaces: '$O.&lt;bucket&gt;.C.&gt;' for chunks and
  ///     '$O.&lt;bucket&gt;.M.&gt;' for metadata. An object is split across as
  ///     many chunk messages as it needs, all on ONE subject keyed by a NUID,
  ///     and a single metadata message says how many there are and what the
  ///     whole thing should hash to.
  ///   </para>
  ///   <para>
  ///     Why chunks at all: max_payload is around 1 MB by default and is a hard
  ///     server limit, so anything larger cannot be a message. Chunking is the
  ///     only way to store a file at all.
  ///   </para>
  ///   <para>
  ///     Every method BLOCKS, and must never be called from a message, connect
  ///     or disconnect handler. Does NOT own the context.
  ///   </para>
  /// </remarks>
  TJetStreamObjectStore = class
  private
    FContext: TJetStreamContext;
    FBucket: string;
    FStream: string;
    FChunkSize: Integer;

    function MetaSubject(const AName: string): string;
    function ChunkSubject(const ANuid: string): string;
    /// Reads the metadata record for AName, deleted ones included
    function RawInfo(const AName: string; out AInfo: TJetStreamObjectInfo): Boolean;
    /// Writes a metadata record, rolling up whatever was on the subject before
    procedure PutInfo(const AInfo: TJetStreamObjectInfo);
    /// Removes every chunk belonging to ANuid
    procedure PurgeChunks(const ANuid: string);
    /// <summary>
    ///   Raises if AStream said EOF while still claiming data - a stream whose
    ///   Read ends early. Streams that cannot report a size are skipped; the
    ///   read loop in Put is the guarantee there
    /// </summary>
    procedure CheckStreamComplete(const AStream: TStream);
    /// <summary>
    ///   Best-effort rollback of ADest to where it was before a failed Get:
    ///   rewind to APosition and truncate what was written. A stream that
    ///   cannot seek or truncate is left alone - the exception the caller
    ///   already has is the real answer, and Get's docs say the destination may
    ///   hold partial bytes in that case
    /// </summary>
    procedure RollBackDestination(const ADest: TStream; APosition: Int64);
  public
    /// <summary>
    ///   Binds to an EXISTING bucket without a round trip. Use Status to
    ///   confirm it is there
    /// </summary>
    constructor Create(AContext: TJetStreamContext; const ABucket: string;
      AChunkSize: Integer = 0);

    { bucket management }

    class function CreateBucket(AContext: TJetStreamContext;
      const AConfig: TJetStreamObjectStoreConfig): TJetStreamObjectStore; overload; static;
    class function CreateBucket(AContext: TJetStreamContext;
      const ABucket: string): TJetStreamObjectStore; overload; static;
    class procedure DeleteBucket(AContext: TJetStreamContext;
      const ABucket: string); static;
    class function ListBuckets(AContext: TJetStreamContext): TArray<string>; static;

    class procedure CheckBucket(const ABucket: string); static;
    /// <summary>
    ///   Raises unless AName can be stored. An object name is arbitrary text -
    ///   it is base64url-encoded into the subject rather than used raw - so the
    ///   only thing rejected is an empty one
    /// </summary>
    class procedure CheckName(const AName: string); static;

    { writing }

    /// <summary>
    ///   Stores AStream under AName, replacing whatever was there. Reads from
    ///   the current position to the end
    /// </summary>
    function Put(const AName: string; AStream: TStream): TJetStreamObjectInfo; overload;
    function Put(const AName: string; const AData: TBytes): TJetStreamObjectInfo; overload;
    function PutString(const AName, AData: string): TJetStreamObjectInfo;
    function PutFile(const AName, AFileName: string): TJetStreamObjectInfo;

    { reading }

    /// <summary>
    ///   Writes the object to ADest. False means no such object - never stored,
    ///   or deleted. Raises EJetStreamObjectError if what came back does not
    ///   match the stored digest, and EJetStreamApiError if the bucket's stream
    ///   does not exist at all
    /// </summary>
    /// <remarks>
    ///   When a read fails part-way, ADest is rewound and truncated to where it
    ///   was before the call (best effort - a stream that cannot seek or
    ///   truncate is left holding the partial bytes). Either way, a raise means
    ///   "no object" and a return of True means "the whole object"
    /// </remarks>
    function Get(const AName: string; ADest: TStream): Boolean; overload;
    function Get(const AName: string; out AData: TBytes): Boolean; overload;
    function GetString(const AName: string; const ADefault: string = ''): string;
    function GetFile(const AName, AFileName: string): Boolean;

    /// <summary>
    ///   The metadata alone, without moving any bytes. False if there is none -
    ///   or if the object was deleted. A bucket whose stream does not exist
    ///   raises EJetStreamApiError instead
    /// </summary>
    function Info(const AName: string; out AInfo: TJetStreamObjectInfo): Boolean;
    /// Every object currently stored, deleted ones left out
    function List: TArray<TJetStreamObjectInfo>;
    function Status: TJetStreamObjectStoreStatus;

    /// <summary>
    ///   Removes the object's bytes and marks its metadata deleted. The
    ///   metadata record itself stays, so the name is remembered as gone
    /// </summary>
    procedure Delete(const AName: string);

    property Bucket: string read FBucket;
    /// OBJ_<bucket> - a bucket is a stream, and this is the stream
    property StreamName: string read FStream;
    /// How much of an object goes into one message
    property ChunkSize: Integer read FChunkSize;
  end;

  /// <summary>
  ///   base64url WITHOUT padding, which is what NATS uses for object names and
  ///   digests. Public because the subject an object lives on is derived from
  ///   it, and anyone debugging a bucket needs to be able to work it out
  /// </summary>
  TObjectStoreEncoding = class
  public
    class function Encode(const AData: TBytes): string; overload; static;
    class function Encode(const AText: string): string; overload; static;
  end;

implementation

/// <summary>
///   RFC3339 in UTC, matching the shape the server writes for every other
///   timestamp in a JetStream record
/// </summary>
function UtcNowIso: string;
begin
  Result := FormatDateTime('yyyy-mm-dd''T''hh:nn:ss''Z''',
    TTimeZone.Local.ToUniversalTime(Now));
end;

{ TObjectStoreEncoding }

class function TObjectStoreEncoding.Encode(const AData: TBytes): string;
begin
  Result := TNetEncoding.Base64.EncodeBytesToString(AData);

  { Built from the plain base64 encoder rather than a base64url one, because
    the differences are exactly these three and doing them by hand cannot be
    caught out by an RTL that pads when NATS does not:
      - TNetEncoding.Base64 wraps at 76 characters, and a subject token cannot
        contain a line break
      - '+' and '/' are not safe in a subject
      - NATS uses the RAW (unpadded) alphabet, so the '=' must go }
  Result := Result.Replace(#13, '', [rfReplaceAll]).Replace(#10, '', [rfReplaceAll]);
  Result := Result.Replace('+', '-', [rfReplaceAll]).Replace('/', '_', [rfReplaceAll]);
  Result := Result.TrimRight(['=']);
end;

class function TObjectStoreEncoding.Encode(const AText: string): string;
begin
  Result := Encode(TEncoding.UTF8.GetBytes(AText));
end;

{ TJetStreamObjectStore }

constructor TJetStreamObjectStore.Create(AContext: TJetStreamContext;
  const ABucket: string; AChunkSize: Integer);
begin
  inherited Create;

  if not Assigned(AContext) then
    raise EJetStreamObjectError.Create('An object store needs a JetStream context');

  CheckBucket(ABucket);

  FContext := AContext;
  FBucket := ABucket;
  FStream := JetStreamConstants.Obj.STREAM_PREFIX + ABucket;

  FChunkSize := AChunkSize;
  if FChunkSize <= 0 then
    FChunkSize := JetStreamConstants.Obj.DEFAULT_CHUNK_SIZE;
end;

class procedure TJetStreamObjectStore.CheckBucket(const ABucket: string);
var
  LChar: Char;
begin
  if ABucket.IsEmpty then
    raise EJetStreamObjectError.Create('A bucket name cannot be empty');

  for LChar in ABucket do
    if not CharInSet(LChar, JetStreamConstants.Obj.VALID_BUCKET_CHARS) then
      raise EJetStreamObjectError.CreateFmt(
        'Bucket name [%s] cannot contain %s - a bucket name becomes part of a ' +
        'stream name and of every subject in it', [ABucket, QuotedStr(LChar)]);
end;

class procedure TJetStreamObjectStore.CheckName(const AName: string);
begin
  { Deliberately permissive. An object name is base64url-encoded into the
    subject, never used raw, so spaces, dots and slashes are all fine - which
    is the point, since these are usually file names }
  if AName.IsEmpty then
    raise EJetStreamObjectError.Create('An object name cannot be empty');
end;

class function TJetStreamObjectStore.CreateBucket(AContext: TJetStreamContext;
  const AConfig: TJetStreamObjectStoreConfig): TJetStreamObjectStore;
var
  LStream: TJetStreamStreamConfig;
begin
  CheckBucket(AConfig.Bucket);

  LStream := Default(TJetStreamStreamConfig);
  LStream.Name := JetStreamConstants.Obj.STREAM_PREFIX + AConfig.Bucket;

  { Two subject spaces in one stream: the chunks and the metadata. They share a
    stream so that an object and its description cannot be separated by a
    retention policy applying to one and not the other }
  LStream.Subjects := [
    Format(JetStreamConstants.Obj.SUBJECT_CHUNKS_ALL, [AConfig.Bucket]),
    Format(JetStreamConstants.Obj.SUBJECT_META_ALL, [AConfig.Bucket])
  ];

  LStream.Description := AConfig.Description;
  LStream.MaxAge := AConfig.TTL;
  LStream.MaxBytes := AConfig.MaxBytes;
  LStream.Storage := AConfig.Storage;
  LStream.NumReplicas := AConfig.Replicas;

  { Discard New so a full bucket refuses a write rather than shedding somebody
    else's chunks, which would corrupt an unrelated object silently.
    AllowRollupHdrs is what lets a metadata record replace its predecessor.
    NOTE there is deliberately no MaxMsgsPerSubject here: every chunk of an
    object shares one subject, so a per-subject limit would delete the object }
  LStream.Discard := TJetStreamDiscard.New;
  LStream.AllowRollupHdrs := True;
  LStream.AllowDirect := True;

  AContext.AddStream(LStream);

  Result := TJetStreamObjectStore.Create(AContext, AConfig.Bucket, AConfig.ChunkSize);
end;

class function TJetStreamObjectStore.CreateBucket(AContext: TJetStreamContext;
  const ABucket: string): TJetStreamObjectStore;
var
  LConfig: TJetStreamObjectStoreConfig;
begin
  LConfig := Default(TJetStreamObjectStoreConfig);
  LConfig.Bucket := ABucket;

  Result := CreateBucket(AContext, LConfig);
end;

class procedure TJetStreamObjectStore.DeleteBucket(AContext: TJetStreamContext;
  const ABucket: string);
begin
  CheckBucket(ABucket);
  AContext.DeleteStream(JetStreamConstants.Obj.STREAM_PREFIX + ABucket);
end;

class function TJetStreamObjectStore.ListBuckets(AContext: TJetStreamContext): TArray<string>;
var
  LNames: TArray<string>;
  LName: string;
  LResult: TList<string>;
begin
  LNames := AContext.StreamNames;

  LResult := TList<string>.Create;
  try
    for LName in LNames do
      if LName.StartsWith(JetStreamConstants.Obj.STREAM_PREFIX) then
        LResult.Add(LName.Substring(Length(JetStreamConstants.Obj.STREAM_PREFIX)));

    Result := LResult.ToArray;
  finally
    LResult.Free;
  end;
end;

function TJetStreamObjectStore.MetaSubject(const AName: string): string;
begin
  { The name is ENCODED, never used raw: an object called 'reports/2024 Q1.pdf'
    would otherwise be several subject tokens with a space in one of them }
  Result := Format(JetStreamConstants.Obj.SUBJECT_META,
    [FBucket, TObjectStoreEncoding.Encode(AName)]);
end;

function TJetStreamObjectStore.ChunkSubject(const ANuid: string): string;
begin
  Result := Format(JetStreamConstants.Obj.SUBJECT_CHUNK, [FBucket, ANuid]);
end;

function TJetStreamObjectStore.RawInfo(const AName: string;
  out AInfo: TJetStreamObjectInfo): Boolean;
var
  LMsg: TJetStreamStoredMsg;
  LJson: string;
begin
  CheckName(AName);
  AInfo := Default(TJetStreamObjectInfo);

  if not FContext.GetLastMsg(FStream, MetaSubject(AName), LMsg) then
    Exit(False);

  LJson := TEncoding.UTF8.GetString(
    TNetEncoding.Base64.DecodeStringToBytes(LMsg.Data));

  AInfo := TJetStreamJSON.FromJSON<TJetStreamObjectInfo>(LJson);
  Result := True;
end;

procedure TJetStreamObjectStore.PutInfo(const AInfo: TJetStreamObjectInfo);
begin
  { Nats-Rollup: sub - each metadata write REPLACES the one before it on that
    subject. Without it every version of every object's metadata would pile up
    forever, and List would need to work out which was current }
  FContext.Publish(MetaSubject(AInfo.Name),
    TJetStreamJSON.ToJSON<TJetStreamObjectInfo>(AInfo),
    TJetStreamPubOptions.New.WithHeader(
      JetStreamConstants.Header.ROLLUP, JetStreamConstants.Header.ROLLUP_SUBJECT));
end;

procedure TJetStreamObjectStore.PurgeChunks(const ANuid: string);
var
  LRequest: TJetStreamPurgeRequest;
begin
  if ANuid.IsEmpty then
    Exit;

  { Every chunk of the object is on this one subject and nothing else is, so a
    filtered purge removes exactly the object and exactly nothing more }
  LRequest := Default(TJetStreamPurgeRequest);
  LRequest.Filter := ChunkSubject(ANuid);

  FContext.PurgeStream(FStream, LRequest);
end;

procedure TJetStreamObjectStore.CheckStreamComplete(const AStream: TStream);
var
  LSize, LPosition: Int64;
begin
  { The read loop is the guarantee against short reads; this is the net for a
    stream whose Read reports EOF while it still claims more data. Reading is
    the only way to get the bytes, so only the stream's own word about its size
    can catch that - and a stream which cannot give one (a wrapper reporting 0,
    or a non-seekable one whose Size access raises) is skipped, because for
    those the read loop is all there is }
  try
    LSize := AStream.Size;
    LPosition := AStream.Position;
  except
    on E: Exception do
      Exit;
  end;

  if (LSize > 0) and (LPosition <> LSize) then
    raise EJetStreamObjectError.CreateFmt(
      'The stream ended at %d of its declared %d bytes - refusing to store a truncated object',
      [LPosition, LSize]);
end;

function TJetStreamObjectStore.Put(const AName: string; AStream: TStream): TJetStreamObjectInfo;
var
  LPrevious: TJetStreamObjectInfo;
  LHadPrevious: Boolean;
  LBuffer: TBytes;
  LRead: Integer;
  LHash: THashSHA2;
  LSubject: string;
begin
  CheckName(AName);

  if not Assigned(AStream) then
    raise EJetStreamObjectError.Create('There is nothing to store');

  { Read BEFORE writing anything: the old chunks have to survive until the new
    metadata is in place, or a reader mid-download would lose them }
  LHadPrevious := RawInfo(AName, LPrevious);

  Result := Default(TJetStreamObjectInfo);
  Result.Name := AName;
  Result.Bucket := FBucket;

  { A fresh NUID, so the new chunks land on a subject of their own rather than
    interleaving with the ones still being read }
  Result.Nuid := TNUID.NextNuid;
  Result.Options.MaxChunkSize := FChunkSize;

  LSubject := ChunkSubject(Result.Nuid);
  LHash := THashSHA2.Create(SHA256);
  SetLength(LBuffer, FChunkSize);

  { Read until the stream says EOF, treating a short read as a smaller chunk
    rather than as the end: many TStream implementations return fewer bytes
    than asked for before the end (sockets, pipes, filtered streams). The old
    loop stopped on the FIRST short read, so those objects were silently
    truncated - and the digest was computed over the truncated bytes, so no
    read ever caught it }
  while True do
  begin
    LRead := AStream.Read(LBuffer, 0, FChunkSize);
    if LRead <= 0 then
      Break;

    { The hash is over the WHOLE object, not per chunk, so it is fed here as
      the bytes go past rather than by reading everything back afterwards }
    LHash.Update(LBuffer, LRead);

    { Every chunk is published with an ack, so a failure to store stops the
      upload instead of producing an object that is quietly short }
    FContext.PublishBytes(LSubject, Copy(LBuffer, 0, LRead));

    Inc(Result.Size, LRead);
    Inc(Result.Chunks);
  end;

  { The loop above guarantees short reads cannot truncate. One failure mode
    remains - a stream whose Read reports EOF while it still claims more data -
    and only the stream's own word about its size can catch that }
  CheckStreamComplete(AStream);

  Result.Digest := JetStreamConstants.Obj.DIGEST_PREFIX +
    TObjectStoreEncoding.Encode(LHash.HashAsBytes);
  Result.Mtime := UtcNowIso;

  PutInfo(Result);

  { Only now: the metadata points at the new chunks, so anything still reading
    the old ones has already got what it needs, and a crash before this point
    leaves orphaned chunks rather than a broken object }
  if LHadPrevious then
    PurgeChunks(LPrevious.Nuid);
end;

function TJetStreamObjectStore.Put(const AName: string; const AData: TBytes): TJetStreamObjectInfo;
var
  LStream: TBytesStream;
begin
  LStream := TBytesStream.Create(AData);
  try
    Result := Put(AName, LStream);
  finally
    LStream.Free;
  end;
end;

function TJetStreamObjectStore.PutString(const AName, AData: string): TJetStreamObjectInfo;
begin
  Result := Put(AName, TEncoding.UTF8.GetBytes(AData));
end;

function TJetStreamObjectStore.PutFile(const AName, AFileName: string): TJetStreamObjectInfo;
var
  LStream: TFileStream;
begin
  LStream := TFileStream.Create(AFileName, fmOpenRead or fmShareDenyWrite);
  try
    Result := Put(AName, LStream);
  finally
    LStream.Free;
  end;
end;

function TJetStreamObjectStore.Info(const AName: string;
  out AInfo: TJetStreamObjectInfo): Boolean;
begin
  Result := RawInfo(AName, AInfo);

  { A deleted object keeps its metadata record - that is how a name that was
    removed stays distinguishable from one that never existed - but it is not
    an object any more, so it must not be reported as one }
  if Result and AInfo.Deleted then
  begin
    AInfo := Default(TJetStreamObjectInfo);
    Result := False;
  end;
end;

function TJetStreamObjectStore.Get(const AName: string; ADest: TStream): Boolean;
var
  LInfo: TJetStreamObjectInfo;
  LHash: THashSHA2;
  LChunks: Integer;
  LDigest: string;
  LStartPos: Int64;
begin
  if not Assigned(ADest) then
    raise EJetStreamObjectError.Create('There is nowhere to put the object');

  if not Info(AName, LInfo) then
    Exit(False);

  { Where ADest stood before any bytes were written, so a failed read can be
    rolled back: a caller who catches the exception must not be left with a
    stream that looks like a successful partial download }
  LStartPos := ADest.Position;

  try
    LHash := THashSHA2.Create(SHA256);
    LChunks := 0;

    { Every chunk is on one subject, so stream order IS chunk order and there
      is no reassembly to get wrong - only the counting and the digest }
    FContext.ScanSubject(FStream, ChunkSubject(LInfo.Nuid), False, False,
      procedure (AMsg: IJetStreamMsg)
      var
        LData: TBytes;
      begin
        LData := AMsg.PayloadData;
        LHash.Update(LData, Length(LData));
        ADest.WriteBuffer(LData, Length(LData));
        Inc(LChunks);
      end);

    { Two independent checks, because they fail differently. A short read - the
      stream expired chunks under a MaxAge, say - shows up as a chunk count
      that does not match; a corrupted or interleaved one shows up in the
      digest. Neither would otherwise be reported: the caller would just get
      less than it asked for, with nothing having raised }
    if LChunks <> LInfo.Chunks then
      raise EJetStreamObjectError.CreateFmt(
        'Object [%s] in bucket [%s] came back in %d chunks but its metadata says ' +
        '%d - part of it is missing from the stream', [AName, FBucket, LChunks, LInfo.Chunks]);

    LDigest := JetStreamConstants.Obj.DIGEST_PREFIX +
      TObjectStoreEncoding.Encode(LHash.HashAsBytes);

    if not LInfo.Digest.IsEmpty and (LDigest <> LInfo.Digest) then
      raise EJetStreamObjectError.CreateFmt(
        'Object [%s] in bucket [%s] failed its digest check: stored %s, got %s',
        [AName, FBucket, LInfo.Digest, LDigest]);

    Result := True;
  except
    { Any failure - the checks above, or the connection dying mid-scan - leaves
      ADest holding partial bytes. Undo them, then hand the exception on }
    on E: Exception do
    begin
      RollBackDestination(ADest, LStartPos);
      raise;
    end;
  end;
end;

procedure TJetStreamObjectStore.RollBackDestination(const ADest: TStream; APosition: Int64);
begin
  { Only what this call wrote is removed: everything after where the caller's
    stream stood. A stream that cannot seek or truncate is left alone - the
    exception that triggered this is the real answer }
  try
    ADest.Seek(APosition, soBeginning);
    ADest.Size := APosition;
  except
    on E: Exception do
      ;   // cannot roll back: the caller still gets the exception above
  end;
end;

function TJetStreamObjectStore.Get(const AName: string; out AData: TBytes): Boolean;
var
  LStream: TBytesStream;
begin
  AData := nil;

  LStream := TBytesStream.Create;
  try
    Result := Get(AName, LStream);
    if Result then
      AData := Copy(LStream.Bytes, 0, LStream.Size);
  finally
    LStream.Free;
  end;
end;

function TJetStreamObjectStore.GetString(const AName, ADefault: string): string;
var
  LData: TBytes;
begin
  if Get(AName, LData) then
    Result := TEncoding.UTF8.GetString(LData)
  else
    Result := ADefault;
end;

function TJetStreamObjectStore.GetFile(const AName, AFileName: string): Boolean;
var
  LStream: TFileStream;
begin
  LStream := TFileStream.Create(AFileName, fmCreate);
  try
    Result := Get(AName, LStream);
  finally
    LStream.Free;
  end;

  { A failed get leaves an empty file behind, which would then look like a
    successfully retrieved empty object }
  if not Result then
    DeleteFile(AFileName);
end;

procedure TJetStreamObjectStore.Delete(const AName: string);
var
  LInfo: TJetStreamObjectInfo;
begin
  if not RawInfo(AName, LInfo) then
    Exit;   // never stored, so there is nothing to remove

  { The metadata is rewritten as a tombstone rather than removed. Size and
    Chunks go to zero with it, so a reader that already holds the old metadata
    cannot be told to expect bytes that are no longer there }
  LInfo.Deleted := True;
  LInfo.Size := 0;
  LInfo.Chunks := 0;
  LInfo.Digest := String.Empty;
  LInfo.Mtime := UtcNowIso;

  PutInfo(LInfo);

  { and only then the bytes, in that order for the same reason Put purges last:
    the metadata must never point at chunks that have already gone }
  PurgeChunks(LInfo.Nuid);
end;

function TJetStreamObjectStore.List: TArray<TJetStreamObjectInfo>;
var
  LResult: TList<TJetStreamObjectInfo>;
begin
  LResult := TList<TJetStreamObjectInfo>.Create;
  try
    { LastPerSubject over the METADATA space only - one record per object, and
      never a chunk }
    FContext.ScanSubject(FStream,
      Format(JetStreamConstants.Obj.SUBJECT_META_ALL, [FBucket]), True, False,
      procedure (AMsg: IJetStreamMsg)
      var
        LInfo: TJetStreamObjectInfo;
      begin
        LInfo := TJetStreamJSON.FromJSON<TJetStreamObjectInfo>(AMsg.Payload);

        { A deleted object keeps its metadata, so filtering on the tombstone is
          what tells a stored object from a remembered name }
        if not LInfo.Deleted then
          LResult.Add(LInfo);
      end);

    Result := LResult.ToArray;
  finally
    LResult.Free;
  end;
end;

function TJetStreamObjectStore.Status: TJetStreamObjectStoreStatus;
var
  LInfo: TJetStreamStreamInfo;
begin
  LInfo := FContext.StreamInfo(FStream);

  Result := Default(TJetStreamObjectStoreStatus);
  Result.Bucket := FBucket;
  Result.StreamName := FStream;

  { Messages counts CHUNKS and metadata records, not objects - an object is
    many messages, and saying otherwise here would be a lie the caller could
    not detect }
  Result.Messages := LInfo.State.Messages;
  Result.Bytes := LInfo.State.Bytes;
  Result.TTL := LInfo.Config.MaxAge;
end;

end.
