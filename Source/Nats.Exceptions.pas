{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Exceptions;

interface

uses
  System.SysUtils;

type
  ENatsException = class(Exception);

  /// <summary>
  ///   A read exceeded the socket's read timeout. Distinct from every other
  ///   socket failure because it is not by itself fatal: an idle connection is
  ///   healthy, and the client answers it with a keep-alive PING rather than
  ///   tearing the connection down
  /// </summary>
  ENatsReadTimeout = class(ENatsException);

  /// <summary>
  ///   The peer sent something this client cannot make sense of. The stream can
  ///   no longer be trusted to be aligned, so the connection is torn down
  /// </summary>
  ENatsProtocolError = class(ENatsException);

  /// <summary>
  ///   A message was refused before it was sent because it is larger than the
  ///   max_payload the server declared in INFO
  /// </summary>
  /// <remarks>
  ///   Its own type because it is the one publish failure a caller can do
  ///   something about - chunk the message, or compress it - and because the
  ///   alternative is far worse than an exception: the server answers an
  ///   oversized message with -ERR 'Maximum Payload Violation' and closes the
  ///   connection, so one bad publish takes down every subscription on it.
  /// </remarks>
  ENatsMaxPayloadError = class(ENatsException);

implementation

end.
