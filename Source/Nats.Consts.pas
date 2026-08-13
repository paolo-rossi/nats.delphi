{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.Consts;

interface

uses
  System.SysUtils;

type

  NatsConstants = class
  public const

    CLIENT_VERSION = '0.1.0';

    // Standard NATS header version
    CLIENT_HEADER_VERSION = 'NATS/1.0';

    DEFAULT_PORT = 4222;
    DEFAULT_URI_ = 'nats://localhost';

    // Parser status
    AWAITING_CONTROL = 0;
    AWAITING_MSG_PAYLOAD = 1;

    // Connection status
    OPEN = 0;
    CLOSE = 1;
    RECONNECT = 2;

    // TLS parameters
    DEFAULT_KEYSTORE = './keystore';
    DEFAULT_TRUSTSTORE = './truststore';
    DEFAULT_PASSWORD = 'password';
    TLS_REQUIRED = 'tls_required';
    TLS_VERSION = 'TLSv1.2';

    // Reconnect Parameters, 2 sec wait, 10 tries
    DEFAULT_RECONNECT_TIME_WAIT = 2*1000;
    DEFAULT_MAX_RECONNECT_ATTEMPTS = 3;
    DEFAULT_PING_INTERVAL = 4*1000;

    /// <summary>
    ///   How long to wait for the TCP connection to be established. Nothing to
    ///   do with how long a read may block - see DEFAULT_READ_TIMEOUT
    /// </summary>
    DEFAULT_CONNECT_TIMEOUT = 5*1000;

    /// <summary>
    ///   nats-server pings an idle client every 2 minutes by default and the
    ///   client answers PONG, so on a healthy connection something arrives at
    ///   least that often
    /// </summary>
    DEFAULT_SERVER_PING_INTERVAL = 2*60*1000;

    /// <summary>
    ///   How long a single read may block. It MUST be comfortably longer than
    ///   DEFAULT_SERVER_PING_INTERVAL, otherwise an idle but perfectly healthy
    ///   connection times out over and over
    /// </summary>
    DEFAULT_READ_TIMEOUT = 3*60*1000;

    /// <summary>
    ///   How long RequestSync waits for a reply. Unrelated to the read timeout:
    ///   this bounds one request/reply exchange, not one socket read, and it is
    ///   deliberately short because a request that gets no answer usually means
    ///   nobody is serving the subject
    /// </summary>
    DEFAULT_REQUEST_TIMEOUT = 5*1000;

    CR_LF = #13#10;
    TAB = #9;
    CR_LF_LEN = 2;
    EMPTY = '';
    SPC = ' ';
    COL = ':';
    SEP = '.';
    WC = '*';
    ARR = '>';

    // Standard prefix for NATS inboxes
    INBOX_PREFIX = '_INBOX.';

  public type
    Protocol = class
    const
      // Core Protocol Commands (Client -> Server)
      CONNECT = 'CONNECT';
      PUB     = 'PUB';
      HPUB    = 'HPUB'; // Publish with Headers JetStream
      SUB     = 'SUB';
      UNSUB   = 'UNSUB';
      PING    = 'PING'; // Client sends PING
      PONG    = 'PONG'; // Client sends PONG in response to server's PING

      // Core Protocol Commands (Server -> Client)
      INFO    = 'INFO';
      MSG     = 'MSG';
      HMSG    = 'HMSG'; // Message with Headers JetStream
      OK      = '+OK';
      ERR     = '-ERR';
      UNKNOWN = 'UNKNOWN'; // For parser if command is not recognized
    end;

    /// <summary>
    ///   Codes that can appear on the header block's status line, as in
    ///   "NATS/1.0 404 No Messages". A status message has an EMPTY body: it is
    ///   control flow, not data, and a consumer that treats it as a message
    ///   hands the application a phantom empty payload
    /// </summary>
    Status = class
    const
      /// A pull consumer's batch produced nothing before it gave up
      NO_MESSAGES = 404;
      /// The pull request's own expiry elapsed
      REQUEST_TIMEOUT = 408;
      /// Consumer deleted, or the request exceeded MaxWaiting
      CONFLICT = 409;
      /// Keep-alive on an idle push consumer or pull batch
      IDLE_HEARTBEAT = 100;
      /// <summary>
      ///   Nobody is subscribed to the requested subject. Only ever sent to a
      ///   client that asked for it in CONNECT, which this one does not do yet
      /// </summary>
      NO_RESPONDERS = 503;
    end;

    class function DEFAULT_URI: string; static;
  end;


implementation

{ NATSConsts }

class function NatsConstants.DEFAULT_URI: string;
begin
  Result := DEFAULT_URI_ + ':' + DEFAULT_PORT.ToString;
end;

end.
