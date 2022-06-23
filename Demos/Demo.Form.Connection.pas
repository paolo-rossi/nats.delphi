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
unit Demo.Form.Connection;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs,

  Nats.Consts,
  Nats.Entities,
  Nats.Connection, Vcl.JumpList, System.Actions, Vcl.ActnList, Vcl.WinXCtrls,
  Vcl.Grids, Vcl.ValEdit, Vcl.StdCtrls;

type
  TfrmConnection = class(TForm)
    grpServer: TGroupBox;
    switchConnection: TToggleSwitch;
    lstServerInfo: TValueListEditor;
    lblServerInfo: TLabel;
    edtHost: TEdit;
    edtPort: TEdit;
    lblServerSettings: TLabel;
    grpCommands: TGroupBox;
    lstCommandList: TListBox;
    lblCommandList: TLabel;
    lstCommandParams: TValueListEditor;
    lblCommandParams: TLabel;
    btnSend: TButton;
    grpConnection: TGroupBox;
    lstSubscriptions: TListBox;
    lblSubscriptionList: TLabel;
    btnSimplePublish: TButton;
    btnSimpleSubscribe: TButton;
    btnSimpleRequest: TButton;
    btnSimpleUnsubscribe: TButton;
    procedure btnSimplePublishClick(Sender: TObject);
    procedure btnSendClick(Sender: TObject);
    procedure btnSimpleRequestClick(Sender: TObject);
    procedure btnSimpleSubscribeClick(Sender: TObject);
    procedure btnSimpleUnsubscribeClick(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure lstCommandListClick(Sender: TObject);
    procedure switchConnectionClick(Sender: TObject);
  private
    FLog: TStrings;
    FConnection: TNatsConnection;
    procedure Log(const AMessage: string);
    procedure LogFmt(const AMessage: string; const Args: array of const);
    procedure SetParams(AParams: TArray<string>);
  public
    class function CreateAndShow(const AName: string; AParent: TWinControl; ALog: TStrings): TfrmConnection;
    procedure RefreshLists;
    property Connection: TNatsConnection read FConnection write FConnection;
  end;

implementation

uses
  Nats.Classes;

{$R *.dfm}

procedure TfrmConnection.btnSimplePublishClick(Sender: TObject);
begin
  FConnection.Publish('mysubject', 'My Message');
end;

procedure TfrmConnection.btnSimpleRequestClick(Sender: TObject);
var
  LHandler: TNatsMsgHandler;
begin
  LHandler :=
    procedure (const AMsg: TNatsArgsMSG)
    begin
      // Code to handle the received response
      // **Remember, your code here must be thread safe!
    end;

  FConnection.Request('mysubject', LHandler)
end;

procedure TfrmConnection.btnSimpleSubscribeClick(Sender: TObject);
var
  LHandler: TNatsMsgHandler;
begin
  LHandler :=
    procedure (const AMsg: TNatsArgsMSG)
    begin
      // Code to handle received Msg with subject "mysubject"
      // **Remember, your code here must be thread safe!
    end;

  FConnection.Subscribe('mysubject', LHandler);
end;

procedure TfrmConnection.btnSimpleUnsubscribeClick(Sender: TObject);
begin
  FConnection.Unsubscribe('mysubject');
end;

procedure TfrmConnection.btnSendClick(Sender: TObject);
var
  LOptPar: string;
begin
  case lstCommandList.ItemIndex of
    0: // Publish
    begin
      FConnection.Publish(
        lstCommandParams.Values['Subject'],
        lstCommandParams.Values['Message']
      );
    end;
    1: // Request
    begin
      LOptPar := lstCommandParams.Values['Message*'];
      if LOptPar.IsEmpty then
        FConnection.Request(
          lstCommandParams.Values['Subject'],
          procedure (const AMsg: TNatsArgsMSG)
          begin
            TThread.Queue(TThread.Current,
              procedure
              begin
                LogFmt('RES <- (%s) %s: %s', [AMsg.ReplyTo, AMsg.Subject, AMsg.Payload]);
              end
            );
          end
        )
      else
        FConnection.Request(
          lstCommandParams.Values['Subject'],
          LOptPar,
          procedure (const AMsg: TNatsArgsMSG)
          begin
            TThread.Queue(TThread.Current,
              procedure
              begin
                LogFmt('RES <- (%s) %s: %s', [AMsg.ReplyTo, AMsg.Subject, AMsg.Payload]);
              end
            );
          end
        );
    end;
    2: // Subscribe
    begin
      LOptPar := lstCommandParams.Values['Queue*'];
      if LOptPar.IsEmpty then
        FConnection.Subscribe(
          lstCommandParams.Values['Subject'],
          procedure (const AMsg: TNatsArgsMSG)
          begin
            TThread.Queue(TThread.Current,
              procedure
              begin
                if not AMsg.ReplyTo.IsEmpty then
                  FConnection.Publish(AMsg.ReplyTo, 'Yes, I can help!');

                LogFmt('MSG <- %s: %s', [AMsg.Subject, AMsg.Payload]);
              end
            );
          end
        )
      else
        FConnection.Subscribe(
          lstCommandParams.Values['Subject'], LOptPar,
          procedure (const AMsg: TNatsArgsMSG)
          begin
            TThread.Queue(TThread.Current,
              procedure
              begin
                if not AMsg.ReplyTo.IsEmpty then
                  FConnection.Publish(AMsg.ReplyTo, 'Yes, I can help!');

                LogFmt('MSG <- %s: %s', [AMsg.Subject, AMsg.Payload]);
              end
            );
          end
        )
    end;
    3: // Unsubscribe
    begin
      LOptPar := lstCommandParams.Values['Max*'];
      if LOptPar.IsEmpty then
        FConnection.Unsubscribe(lstCommandParams.Values['Id'].ToInteger)
      else
        FConnection.Unsubscribe(lstCommandParams.Values['Id'].ToInteger, LOptPar.ToInteger);
    end;
  end;
  RefreshLists;
end;

procedure TfrmConnection.FormDestroy(Sender: TObject);
begin
  FConnection.Free;
end;

procedure TfrmConnection.Log(const AMessage: string);
begin
  FLog.Add(Format('%s [%s]: %s', [
    FormatDateTime('hh:nn:ss.zzz', Now),
    FConnection.Name,
    AMessage])
  );
end;

procedure TfrmConnection.LogFmt(const AMessage: string; const Args: array of const);
begin
  Log(Format(AMessage, Args));
end;

procedure TfrmConnection.FormCreate(Sender: TObject);
begin
  FConnection := TNatsConnection.Create;
end;

class function TfrmConnection.CreateAndShow(const AName: string; AParent: TWinControl; ALog: TStrings): TfrmConnection;
begin
  Result := TfrmConnection.Create(AParent);
  try
    Result.FConnection.Name := AName;
    Result.FLog := ALog;
    Result.Top := 0;
    Result.Left := 0;
    Result.BorderStyle := bsNone;
    Result.Parent := AParent;
    Result.Align := alClient;
    Result.Show;
  except
    Result.Free;
    raise;
  end;
end;

procedure TfrmConnection.lstCommandListClick(Sender: TObject);
begin
  case lstCommandList.ItemIndex of
     0: // Publish
     begin
       SetParams(['Subject', 'Message']);
     end;
     1: // Request
     begin
       SetParams(['Subject', 'Message*']);
     end;
     2: // Subscribe
     begin
       SetParams(['Subject', 'Queue*']);
     end;
     3: // Unsubscribe
     begin
       SetParams(['Id', 'Max*']);
     end;
  end;
end;

procedure TfrmConnection.RefreshLists;
var
  LPair: TNatsSubscriptionPair;
begin
  lstSubscriptions.Clear;
  for LPair in FConnection.GetSubscriptionList do
  begin
    lstSubscriptions.Items.Add(Format('%s (%d)', [LPair.Value.Subject, LPair.Key]));
  end;
end;

procedure TfrmConnection.SetParams(AParams: TArray<string>);
var
  LParam: string;
begin
  lstCommandParams.Strings.Clear;
  for LParam in AParams do
  begin
    lstCommandParams.Strings.AddPair(LParam, '');
  end;
end;

procedure TfrmConnection.switchConnectionClick(Sender: TObject);
begin
  if FConnection.Connected then
    FConnection.Close
  else
    FConnection.
      SetChannel(edtHost.Text, StrToInt(edtPort.Text), 1000).
      Open(
        procedure (AInfo: TNatsServerInfo)
        begin
          TThread.Queue(TThread.Current,
            procedure
            begin
              Log('Connected to server ' + AInfo.server_name);

              lstServerInfo.Strings.Values['Server ID'] := AInfo.server_id;
              lstServerInfo.Strings.Values['Server Name'] := AInfo.server_name;
              lstServerInfo.Strings.Values['Server Version'] := AInfo.version;
              lstServerInfo.Strings.Values['Protocol'] := AInfo.proto.ToString;
              lstServerInfo.Strings.Values['Host'] := AInfo.host;
              lstServerInfo.Strings.Values['Port'] := AInfo.port.ToString;
              lstServerInfo.Strings.Values['Client ID'] := AInfo.client_id.ToString;
              lstServerInfo.Strings.Values['Client IP'] := AInfo.client_ip;
            end
          );
        end
      );
end;

end.
