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
program DemoNats;

uses
  Vcl.Forms,
  Demo.Form.Main in 'Demo.Form.Main.pas' {frmMain},
  Demo.Form.Connection in 'Demo.Form.Connection.pas' {frmConnection},
  Nats.Consts in '..\Source\Nats.Consts.pas',
  Nats.Socket in '..\Source\Nats.Socket.pas',
  Nats.Socket.Indy in '..\Source\Nats.Socket.Indy.pas',
  Nats.Connection in '..\Source\Nats.Connection.pas',
  Nats.Exceptions in '..\Source\Nats.Exceptions.pas',
  Nats.Entities in '..\Source\Nats.Entities.pas',
  Nats.Parser in '..\Source\Nats.Parser.pas',
  Nats.Monitor in '..\Source\Nats.Monitor.pas',
  Nats.Classes in '..\Source\Nats.Classes.pas',
  Nats.JetStream.Client in '..\Source\Nats.JetStream.Client.pas',
  Nats.JetStream.Entities in '..\Source\Nats.JetStream.Entities.pas',
  Nats.JetStream.Enums in '..\Source\Nats.JetStream.Enums.pas',
  Nats.Json.Utils in '..\Source\Nats.Json.Utils.pas',
  Nats.Nuid in '..\Source\Nats.Nuid.pas';

{$R *.res}

begin
  ReportMemoryLeaksOnShutdown := True;
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmMain, frmMain);
  Application.Run;
end.
