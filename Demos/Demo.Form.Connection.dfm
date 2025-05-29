object frmConnection: TfrmConnection
  Left = 0
  Top = 0
  Margins.Left = 6
  Margins.Top = 6
  Margins.Right = 6
  Margins.Bottom = 6
  Caption = 'frmConnection'
  ClientHeight = 740
  ClientWidth = 1240
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -24
  Font.Name = 'Segoe UI'
  Font.Style = []
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  PixelsPerInch = 192
  TextHeight = 32
  object grpServer: TGroupBox
    Left = 0
    Top = 0
    Width = 418
    Height = 740
    Margins.Left = 6
    Margins.Top = 6
    Margins.Right = 6
    Margins.Bottom = 6
    Align = alLeft
    Caption = ' Server '
    TabOrder = 0
    object lblServerInfo: TLabel
      Left = 32
      Top = 202
      Width = 116
      Height = 32
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      Caption = 'Server Info'
    end
    object lblServerSettings: TLabel
      Left = 32
      Top = 112
      Width = 124
      Height = 32
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      Caption = 'Host && Port'
    end
    object switchConnection: TToggleSwitch
      Left = 32
      Top = 50
      Width = 146
      Height = 40
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      SwitchHeight = 40
      SwitchWidth = 100
      TabOrder = 0
      ThumbWidth = 30
      OnClick = switchConnectionClick
    end
    object lstServerInfo: TValueListEditor
      Left = 32
      Top = 244
      Width = 346
      Height = 450
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      DefaultColWidth = 160
      DefaultRowHeight = 36
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
        160
        174)
    end
    object edtHost: TEdit
      Left = 32
      Top = 144
      Width = 234
      Height = 40
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      TabOrder = 2
      Text = '127.0.0.1'
    end
    object edtPort: TEdit
      Left = 278
      Top = 144
      Width = 100
      Height = 40
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      TabOrder = 3
      Text = '4222'
    end
  end
  object grpCommands: TGroupBox
    Left = 418
    Top = 0
    Width = 500
    Height = 740
    Margins.Left = 6
    Margins.Top = 6
    Margins.Right = 6
    Margins.Bottom = 6
    Align = alLeft
    Caption = ' Commands '
    TabOrder = 1
    object lblCommandList: TLabel
      Left = 12
      Top = 50
      Width = 153
      Height = 32
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      Caption = 'Command List'
    end
    object lblCommandParams: TLabel
      Left = 12
      Top = 340
      Width = 235
      Height = 32
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      Caption = 'Command Parameters'
    end
    object lblSubscriptionList: TLabel
      Left = 252
      Top = 50
      Width = 174
      Height = 32
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      Caption = 'Subscription List'
    end
    object lstCommandList: TListBox
      Left = 12
      Top = 92
      Width = 214
      Height = 236
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      ItemHeight = 30
      Items.Strings = (
        'Publish'
        'Request'
        'Subscribe'
        'Unsubscribe')
      TabOrder = 0
      OnClick = lstCommandListClick
    end
    object lstCommandParams: TValueListEditor
      Left = 12
      Top = 382
      Width = 454
      Height = 236
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      DefaultColWidth = 160
      DefaultRowHeight = 36
      Strings.Strings = (
        'Topic='
        'Reply-to=')
      TabOrder = 1
      TitleCaptions.Strings = (
        'Prop'
        'Value')
      ColWidths = (
        160
        282)
    end
    object btnSend: TButton
      Left = 12
      Top = 644
      Width = 454
      Height = 50
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      Caption = 'Send Command'
      TabOrder = 2
      OnClick = btnSendClick
    end
    object lstSubscriptions: TListBox
      Left = 252
      Top = 92
      Width = 214
      Height = 236
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      ItemHeight = 30
      TabOrder = 3
    end
  end
  object grpConnection: TGroupBox
    Left = 918
    Top = 0
    Width = 322
    Height = 740
    Margins.Left = 6
    Margins.Top = 6
    Margins.Right = 6
    Margins.Bottom = 6
    Align = alClient
    Caption = ' Misc'
    TabOrder = 2
    object btnSimplePublish: TButton
      Left = 12
      Top = 92
      Width = 278
      Height = 50
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      Caption = 'Simple Publish Code'
      TabOrder = 0
      OnClick = btnSimplePublishClick
    end
    object btnSimpleSubscribe: TButton
      Left = 12
      Top = 154
      Width = 278
      Height = 50
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      Caption = 'Simple Subscribe Code'
      TabOrder = 1
      OnClick = btnSimpleSubscribeClick
    end
    object btnSimpleRequest: TButton
      Left = 12
      Top = 216
      Width = 278
      Height = 50
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      Caption = 'Simple Request Code'
      TabOrder = 2
      OnClick = btnSimpleRequestClick
    end
    object btnSimpleUnsubscribe: TButton
      Left = 12
      Top = 278
      Width = 278
      Height = 50
      Margins.Left = 6
      Margins.Top = 6
      Margins.Right = 6
      Margins.Bottom = 6
      Caption = 'Simple Unsubscribe Code'
      TabOrder = 3
      OnClick = btnSimpleUnsubscribeClick
    end
  end
end
