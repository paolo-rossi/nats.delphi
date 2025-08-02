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
unit Nats.Rand;

interface

{$R-}
{$Q-}

uses
  System.SysUtils;

type
  /// <summary>
  ///   This is a fixed-increment version of Java 8's SplittableRandom
  ///   generator. It is a very fast generator passing BigCrush, and it can be
  ///   useful if for some reason you absolutely want 64 bits of state
  ///
  ///   https://rosettacode.org/wiki/Pseudo-random_numbers/Splitmix64
  /// </summary>
  TSplittableRandom = record
  private
    FMax64: UInt64;
    FState: UInt64;
  public
    class function New(): TSplittableRandom; overload; static;
    class function New(ASeed: UInt64): TSplittableRandom; overload; static;
  public
    function Next(): UInt64; inline;
    function NextFloat(): Double; inline;
    function Random64(const ARange: UInt64): UInt64;
    function Read(var ABuffer: TBytes): Integer;
  end;

implementation

class function TSplittableRandom.New(ASeed: UInt64): TSplittableRandom;
begin
  Result.FState := ASeed;
  Int64Rec(Result.FMax64).Lo := Cardinal(-1);
  Int64Rec(Result.FMax64).Hi := Cardinal(-1);
end;

class function TSplittableRandom.New: TSplittableRandom;
begin
  Randomize;
  Result.FState := (UInt64(Random32Proc) shl 32) or UInt64(Random32Proc);
  Int64Rec(Result.FMax64).Lo := Cardinal(-1);
  Int64Rec(Result.FMax64).Hi := Cardinal(-1);
end;

function TSplittableRandom.Next(): UInt64;
begin
  FState := FState + UInt64($9e3779b97f4a7c15);
  Result := FState;
  Result := (Result xor (Result shr 30)) * UInt64($bf58476d1ce4e5b9);
  Result := (Result xor (Result shr 27)) * UInt64($94d049bb133111eb);
  Result :=  Result xor (Result shr 31);
end;

function TSplittableRandom.NextFloat: Double;
begin
  Result := Next() / FMax64;
end;

function TSplittableRandom.Random64(const ARange: UInt64): UInt64;
var
  LTest, LNext: UInt64;
begin
  LTest := ((UInt64($FFFFFFFFFFFFFFFF) - ARange) + 1) mod ARange;
  LNext := Next();
  while LNext < LTest do
    LNext := Next();
  Result := LNext mod ARange;
end;

function TSplittableRandom.Read(var ABuffer: TBytes): Integer;
var
  LIndex: Integer;
begin
  Result := 0;
  if Length(ABuffer) = 0 then
    raise Exception.Create('Cannot use a 0-lenght buffer');

  for LIndex := 0 to High(ABuffer) do
  begin
    ABuffer[LIndex] := Random64(255);
    Inc(Result);
  end;
end;

end.
