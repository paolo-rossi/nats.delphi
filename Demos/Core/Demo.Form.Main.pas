{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
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
