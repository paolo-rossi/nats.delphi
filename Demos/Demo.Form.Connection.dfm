object frmConnection: TfrmConnection
  Left = 0
  Top = 0
  Caption = 'frmConnection'
  ClientHeight = 370
  ClientWidth = 566
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = 'Segoe UI'
  Font.Style = []
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  TextHeight = 15
  object grpServer: TGroupBox
    Left = 0
    Top = 0
    Width = 209
    Height = 370
    Align = alLeft
    Caption = ' Server '
    TabOrder = 0
    ExplicitHeight = 357
    object lblServerInfo: TLabel
      Left = 16
      Top = 101
      Width = 56
      Height = 15
      Caption = 'Server Info'
    end
    object lblServerSettings: TLabel
      Left = 16
      Top = 56
      Width = 63
      Height = 15
      Caption = 'Host && Port'
    end
    object switchConnection: TToggleSwitch
      Left = 16
      Top = 25
      Width = 73
      Height = 20
      TabOrder = 0
      OnClick = switchConnectionClick
    end
    object lstServerInfo: TValueListEditor
      Left = 16
      Top = 122
      Width = 173
      Height = 225
      DefaultColWidth = 80
      Options = [goFixedVertLine, goFixedHorzLine, goVertLine, goHorzLine, goColSizing, goAlwaysShowEditor, goThumbTracking]
      Strings.Strings = (
        'Server ID='
        'Server Name='
        'Server Version='
        'Protocol='
        'Host='
        'Port='
        'Client ID='
        'Cllient IP=')
      TabOrder = 1
      TitleCaptions.Strings = (
        'Prop'
        'Value')
      ColWidths = (
        80
        87)
    end
    object edtHost: TEdit
      Left = 16
      Top = 72
      Width = 117
      Height = 23
      TabOrder = 2
      Text = '127.0.0.1'
    end
    object edtPort: TEdit
      Left = 139
      Top = 72
      Width = 50
      Height = 23
      TabOrder = 3
      Text = '4222'
    end
  end
  object grpCommands: TGroupBox
    Left = 209
    Top = 0
    Width = 200
    Height = 370
    Align = alLeft
    Caption = ' Commands '
    TabOrder = 1
    ExplicitHeight = 357
    object lblCommandList: TLabel
      Left = 6
      Top = 25
      Width = 78
      Height = 15
      Caption = 'Command List'
    end
    object lblCommandParams: TLabel
      Left = 6
      Top = 170
      Width = 119
      Height = 15
      Caption = 'Command Parameters'
    end
    object lstCommandList: TListBox
      Left = 6
      Top = 46
      Width = 107
      Height = 118
      ItemHeight = 15
      Items.Strings = (
        'Publish'
        'Request'
        'Subscribe'
        'Unsubscribe')
      TabOrder = 0
      OnClick = lstCommandListClick
    end
    object lstCommandParams: TValueListEditor
      Left = 6
      Top = 191
      Width = 179
      Height = 118
      DefaultColWidth = 80
      Strings.Strings = (
        'Topic='
        'Reply-to=')
      TabOrder = 1
      TitleCaptions.Strings = (
        'Prop'
        'Value')
      ColWidths = (
        80
        93)
    end
    object btnSend: TButton
      Left = 6
      Top = 322
      Width = 179
      Height = 25
      Caption = 'Send Command'
      TabOrder = 2
      OnClick = btnSendClick
    end
  end
  object grpConnection: TGroupBox
    Left = 409
    Top = 0
    Width = 157
    Height = 370
    Align = alClient
    Caption = ' Connection '
    TabOrder = 2
    ExplicitHeight = 357
    object lblSubscriptionList: TLabel
      Left = 6
      Top = 25
      Width = 87
      Height = 15
      Caption = 'Subscription List'
    end
    object lstSubscriptions: TListBox
      Left = 6
      Top = 46
      Width = 121
      Height = 118
      ItemHeight = 15
      TabOrder = 0
    end
  end
end
