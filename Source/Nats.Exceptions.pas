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
