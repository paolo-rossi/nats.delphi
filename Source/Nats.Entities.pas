{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Entities;

interface

uses
  System.SysUtils,

  Nats.Consts;

type
  /// <summary>
  ///   The server's INFO. Fields are named in Delphi casing and mapped onto the
  ///   wire JSON (<c>server_id</c>, <c>max_payload</c>, ...) by the SnakeCase
  ///   rule in <c>NatsJSONConfig</c> - so a rename here silently renames a
  ///   protocol field. Anything the server sends that is not declared here is
  ///   ignored.
  /// </summary>
  TNatsServerInfo = record
    ServerId: string;
    ServerName: string;
    Version: string;
    Proto: Integer;
    GitCommit: string;
    Go: string;
    Host: string;
    Port: Integer;
    Headers: Boolean;
    AuthRequired: Boolean;
    TlsRequired: Boolean;
    TlsAvailable: Boolean;
    MaxPayload: Integer;
    Jetstream: Boolean;
    ClientId: Integer;
    ClientIp: string;
    Nonce: string;

    class function FromJSONString(const AValue: string): TNatsServerInfo; static;
  end;

  /// <summary>
  ///   The CONNECT payload. Same naming rule as <see cref="TNatsServerInfo" />:
  ///   the wire names (<c>auth_token</c>, <c>tls_required</c>, ...) are derived
  ///   from these by <c>NatsJSONConfig</c>.
  /// </summary>
  TNatsConnectOptions = record
  public
    Verbose: Boolean;
    Pedantic: Boolean;
    TlsRequired: Boolean;
    AuthToken: string;
    User: string;
    Pass: string;
    Name: string;
    Lang: string;
    Version: string;
    Protocol: Integer;
    Echo: Boolean;
    /// <summary>
    ///   Must be True to use message headers: a server will refuse HPUB from a
    ///   client that has not declared header support (it closes the connection)
    ///   and will strip headers from anything it delivers, sending MSG instead
    ///   of HMSG
    /// </summary>
    Headers: Boolean;
    Sig: string;
    Jwt: string;

    function ToJSONString: string;
    class function FromJSONString(const AValue: string): TNatsConnectOptions; static;
  end;

implementation

uses
  System.Rtti,

  Neon.Core.Types,
  Neon.Core.Persistence,
  Neon.Core.Persistence.JSON;

/// <summary>
///   The Neon configuration these records must be (de)serialized with - always
///   pass it explicitly, never use the parameterless TNeon overloads.
/// </summary>
/// <remarks>
///   Those overloads resolve to TNeonConfiguration.Default, which is PascalCase,
///   and the failure is silent rather than loud: the server ignores CONNECT
///   fields it does not recognize, so the client would simply stop declaring
///   header support and have the next HPUB drop the connection, while INFO would
///   parse as an empty record. SnakeCase is what turns MaxPayload into
///   max_payload, in both directions. Neon's default TNeonMembers.Standard
///   already maps a record to its fields, which is what these records need.
/// </remarks>
function NatsJSONConfig: INeonConfiguration;
begin
  Result := TNeonConfiguration.Create.SetMemberCase(TNeonCase.SnakeCase);
end;

{ TNatsServerInfo }

class function TNatsServerInfo.FromJSONString(const AValue: string): TNatsServerInfo;
begin
  Result := TNeon.JSONToValue<TNatsServerInfo>(AValue, NatsJSONConfig);
end;

{ TNatsConnectOptions }

class function TNatsConnectOptions.FromJSONString(const AValue: string): TNatsConnectOptions;
begin
  Result := TNeon.JSONToValue<TNatsConnectOptions>(AValue, NatsJSONConfig);
end;

function TNatsConnectOptions.ToJSONString: string;
begin
  Result := TNeon.ValueToJSONString(TValue.From<TNatsConnectOptions>(Self), NatsJSONConfig);
end;

end.
