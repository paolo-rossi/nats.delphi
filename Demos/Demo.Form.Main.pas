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
unit Demo.Form.Main;

interface

uses
  Winapi.Windows, System.SysUtils, System.Classes,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls, Vcl.ExtCtrls,
  Vcl.ComCtrls, Vcl.Graphics, Vcl.Imaging.pngimage,

  Demo.Form.Connection,
  Nats.Consts,
  Nats.Entities,
  Nats.Connection;

type
  TfrmMain = class(TForm)
    memoLog: TMemo;
    pnlNetwork: TPanel;
    lstNetwork: TListBox;
    btnNewConnection: TButton;
    pnlClient: TPanel;
    Splitter1: TSplitter;
    pgcConnections: TPageControl;
    tsAbout: TTabSheet;
    imgNatsDelphi: TImage;
    procedure FormCreate(Sender: TObject);
    procedure btnNewConnectionClick(Sender: TObject);
  private
    FDefaultIP: string;
    FDefaultPort: Integer;
  public
  end;

var
  frmMain: TfrmMain;

implementation

uses
  Nats.Classes,
  Nats.Parser;

{$R *.dfm}

procedure TfrmMain.FormCreate(Sender: TObject);
begin
  Color := RGB(Random(255), Random(255), Random(255));

  FDefaultIP := ParamStr(1);
  if ParamStr(2).IsEmpty then
    FDefaultPort := 0
  else
    FDefaultPort := ParamStr(2).ToInteger;
end;

procedure TfrmMain.btnNewConnectionClick(Sender: TObject);
var
  LFormConn: TfrmConnection;
  LTabSheet: TTabSheet;
begin
  LTabSheet := TTabSheet.Create(pgcConnections);
  LTabSheet.Caption := 'Connection ' + pgcConnections.PageCount.ToString;
  LTabSheet.PageControl := pgcConnections;

  LFormConn := TfrmConnection.CreateAndShow(LTabSheet.Caption, LTabSheet, memoLog.Lines);
  LFormConn.Configure(FDefaultIP, FDefaultPort);
  lstNetwork.AddItem(LTabSheet.Caption, LFormConn.Connection);
  pgcConnections.ActivePage := LTabSheet;
end;

initialization
  Randomize;

end.
