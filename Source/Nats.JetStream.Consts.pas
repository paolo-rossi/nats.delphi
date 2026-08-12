{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
unit Nats.JetStream.Consts;

interface

type
  /// <summary>
  ///   JetStream protocol constants, in the nested-class idiom of
  ///   NatsConstants. Use these, never string literals
  /// </summary>
  JetStreamConstants = class
  public type
    /// <summary>
    ///   The reply-to subject of a message delivered by a JetStream consumer.
    ///   It is not an inbox: it carries the message's metadata in its tokens
    ///   AND is the subject an acknowledgement is published to
    /// </summary>
    Ack = class
    const
      /// <summary>
      ///   Every ack subject starts with this. A reply-to that does not is an
      ///   ordinary core NATS reply, not a JetStream one
      /// </summary>
      PREFIX = '$JS.ACK.';

      { The first two tokens, checked rather than assumed }
      TOKEN_JS = '$JS';
      TOKEN_ACK = 'ACK';

      /// <summary>
      ///   What a server with no JetStream domain configured puts in the domain
      ///   token - it sends a placeholder rather than omitting the token
      /// </summary>
      NO_DOMAIN = '_';

      /// <summary>
      ///   V1: $JS.ACK.&lt;stream&gt;.&lt;consumer&gt;.&lt;delivered&gt;.
      ///   &lt;stream seq&gt;.&lt;consumer seq&gt;.&lt;timestamp&gt;.&lt;pending&gt;
      /// </summary>
      V1_TOKEN_COUNT = 9;

      /// <summary>
      ///   V2 adds &lt;domain&gt;.&lt;account hash&gt; after ACK and a random
      ///   token at the end. This is a MINIMUM, not an equality: a later server
      ///   may append further tokens, and appending must not break parsing
      /// </summary>
      V2_TOKEN_COUNT = 12;

      { Token positions in the V2 layout. A V1 subject is normalised onto it by
        inserting the two tokens it does not have, so there is one set of
        indices rather than two }
      POS_JS            = 0;
      POS_ACK           = 1;
      POS_DOMAIN        = 2;
      POS_ACCOUNT_HASH  = 3;
      POS_STREAM        = 4;
      POS_CONSUMER      = 5;
      POS_NUM_DELIVERED = 6;
      POS_STREAM_SEQ    = 7;
      POS_CONSUMER_SEQ  = 8;
      POS_TIMESTAMP     = 9;
      POS_NUM_PENDING   = 10;
    end;
  end;

implementation

end.
