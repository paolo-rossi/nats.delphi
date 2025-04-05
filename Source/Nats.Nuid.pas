unit Nats.Nuid;

interface

uses
  System.SysUtils,
  NATS.Rand;

type
  /// <summary>
  ///   <para>
  ///     A unique identifier generator that is high performance, very fast,
  ///     and tries to be entropy pool friendly.
  ///   </para>
  ///   <para>
  ///     We will use 12 bytes of crypto generated data (entropy
  ///     draining), and 10 bytes of sequential that is started at a pseudo
  ///     random number and increments with a pseudo-random increment.
  ///     Total is 22 bytes of base 62 ascii text :)
  ///   </para>
  /// </summary>
  TNUID = class sealed
  private const
    DIGITS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
    BASE = 62;
    PRE_LEN = 12;
    SEQ_LEN = 10;
    MAX_SEQ: UInt64 = 839299365868340224; // base^seqLen == 62^10
    MIN_INC: UInt64 = 33;
    MAX_INC: UInt64 = 333;
    TOT_LEN: Integer = PRE_LEN + SEQ_LEN;
  private
    FRand: TSplittableRandom;
    FPrefix: TArray<Byte>;
    FSequence: Int64;
    FIncrement: Int64;

    /// <summary>
    ///   Resets the sequential portion of the NUID
    /// </summary>
    procedure ResetSequential();
  public

    /// <summary>
    ///   Generate the next NUID string
    /// </summary>
    function Next(): string; overload;

    /// <summary>
    ///   <para>
    ///     Generate a new prefix from nats.rand
    ///   </para>
    ///   <para>
    ///     This call *can* drain entropy and will be called automatically
    ///     when we exhaust the sequential range. Will raise an Exception if
    ///     it gets an error from rand.
    ///   </para>
    /// </summary>
    procedure RandomizePrefix();
  public
    /// <summary>
    ///   Create will generate a new NUID and properly initialize the prefix,
    ///   sequential start, and sequential increment
    /// </summary>
    constructor Create;

  private
    class var FGlobalNUID: TNUID;
  public
    class constructor Create;
    class destructor Destroy;

    class function NextNuid(): string; overload;
  end;


implementation

{ TNUID }

constructor TNUID.Create;
begin
  FRand := TSplittableRandom.New();
  FSequence := FRand.Random64(MAX_SEQ);
  FIncrement := MIN_INC + FRand.Random64(MAX_INC - MIN_INC);
  SetLength(FPrefix, PRE_LEN);
	RandomizePrefix();
end;

function TNUID.Next: string;
var
  b: TArray<Byte>;
  i, l: Int64;
begin
	// Increment and capture
	FSequence := FSequence + FIncrement;
	if FSequence >= MAX_SEQ then
  begin
		RandomizePrefix();
		ResetSequential();
	end;

	// Copy prefix
  SetLength(b, TOT_LEN);
  Move(FPrefix[0], b[0], Length(FPrefix));

  i := Length(b);
  l := FSequence;
  while i > PRE_LEN do
  begin
    i := i - 1;
    b[i] := Ord(DIGITS.Chars[l mod BASE]);
    l := l div BASE;
  end;
  Result := TEncoding.ANSI.GetString(b);
end;

class constructor TNUID.Create;
begin
  FGlobalNUID := TNUID.Create;
end;

class destructor TNUID.Destroy;
begin
  FGlobalNUID.Free;
end;

class function TNUID.NextNuid(): string;
begin
	TMonitor.Enter(FGlobalNUID);
  try
  	Result := FGlobalNUID.Next();
  finally
	  TMonitor.Exit(FGlobalNUID);
  end;
end;

procedure TNUID.RandomizePrefix;
var
  cb: TBytes;
  nb: Integer;
  i: Integer;
begin
  SetLength(cb, PRE_LEN);
  nb := FRand.Read(cb);

	if nb <> PRE_LEN then
    raise Exception.Create('Error generating random sequence');

	for i := 0 to PRE_LEN - 1 do
		FPrefix[i] := Ord(DIGITS.Chars[cb[i] mod BASE]);
end;

procedure TNUID.ResetSequential;
begin
  FSequence := FRand.Random64(MAX_SEQ);
  FIncrement := MIN_INC + FRand.Random64(MAX_INC - MIN_INC);
end;

{
initialization
  TNUID.FGlobalNUID := TNUID.Create;

finalization
  TNUID.FGlobalNUID.Free;
}

end.
