{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
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
  Nats.JetStream.Consts in '..\Source\Nats.JetStream.Consts.pas',
  Nats.JetStream.Entities in '..\Source\Nats.JetStream.Entities.pas',
  Nats.JetStream.Message in '..\Source\Nats.JetStream.Message.pas',
  Nats.JetStream.KV in '..\Source\Nats.JetStream.KV.pas',
  Nats.JetStream.ObjectStore in '..\Source\Nats.JetStream.ObjectStore.pas',
  Nats.Nuid in '..\Source\Nats.Nuid.pas';

{$R *.res}

begin
  ReportMemoryLeaksOnShutdown := True;
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmMain, frmMain);
  Application.Run;
end.
