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
unit Nats.Monitor;

interface

uses
  System.SysUtils, System.SyncObjs, system.Classes, System.Generics.Collections,

  Nats.Consts,
  Nats.Classes;

type
  /// <summary>
  ///   Target resources need to implement INatsResource interface to be
  ///   monitored by TNatsMonitor
  /// </summary>
  INatsResource = interface
  ['{C49BC6AF-7852-4129-8639-5FA106813E4E}']
		procedure SendPing(AHandler: TNatsPingHandler);
		function GetResourceId: string;
		function IsConnected: Boolean;
  end;


  /// <summary>
  ///   Monitors a registered "Resource" by sending a PING message. It removes
  ///   the resource from the list when PING fails.
  /// </summary>
  TNatsMonitor = class(TNatsThread)
  private
    FResources: TDictionary<string, INatsResource>;
    FLock: TCriticalSection;
    function ResourceSnapshot: TArray<TPair<string, INatsResource>>;
  protected
    procedure Execute; override;
  public
    constructor Create;
    destructor Destroy; override;

    procedure AddResource(AId: string; AResource: INatsResource);
    procedure RemoveResource(AId: string);
  end;

implementation

{ TNatsMonitor }

constructor TNatsMonitor.Create;
begin
  { Delphi does not chain constructors: without this the TThread ancestor is
    never constructed, so no OS thread exists and FStopEvent stays nil }
  inherited Create;
  FLock := TCriticalSection.Create;
  FResources := TDictionary<string, INatsResource>.Create;
end;

destructor TNatsMonitor.Destroy;
begin
  FResources.Free;
  FLock.Free;
  inherited;
end;

procedure TNatsMonitor.AddResource(AId: string; AResource: INatsResource);
begin
  FLock.Enter;
  try
    FResources.AddOrSetValue(AId, AResource);
  finally
    FLock.Leave;
  end;
end;

procedure TNatsMonitor.RemoveResource(AId: string);
begin
  FLock.Enter;
  try
    FResources.Remove(AId);
  finally
    FLock.Leave;
  end;
end;

function TNatsMonitor.ResourceSnapshot: TArray<TPair<string, INatsResource>>;
begin
  FLock.Enter;
  try
    Result := FResources.ToArray;
  finally
    FLock.Leave;
  end;
end;

procedure TNatsMonitor.Execute;
var
  LPair: TPair<string, INatsResource>;
begin
  NameThreadForDebugging('Nats Monitor');

  while not Terminated do
  begin
    if FStopEvent.WaitFor(NatsConstants.DEFAULT_PING_INTERVAL) = TWaitResult.wrSignaled then
      Break;

    { Iterate a snapshot: AddResource/RemoveResource may run on another thread,
      and a resource that fails its ping is removed inside this very loop }
    for LPair in ResourceSnapshot do
    begin
      if Terminated then
        Break;

      try
        if LPair.Value.IsConnected then
          LPair.Value.SendPing(nil);
      except
        on E: Exception do
          { the ping failed: stop monitoring this resource rather than
            rediscovering the failure every interval }
          RemoveResource(LPair.Key);
      end;
    end;
  end;
end;

end.
