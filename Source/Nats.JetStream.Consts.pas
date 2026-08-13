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
    ///   The reserved subjects the JetStream API answers on. Every one is a
    ///   plain core NATS request/reply carrying JSON - JetStream adds no wire
    ///   protocol of its own
    /// </summary>
    Api = class
    const
      /// <summary>
      ///   Without a domain. A domain moves the whole API under
      ///   $JS.&lt;domain&gt;.API instead, which is how a leaf node reaches the
      ///   hub's JetStream rather than its own
      /// </summary>
      PREFIX = '$JS.API.';
      PREFIX_DOMAIN = '$JS.%s.API.';

      /// Account usage and limits - the cheapest call, so also a liveness check
      INFO = 'INFO';

      STREAM_CREATE = 'STREAM.CREATE.%s';
      STREAM_UPDATE = 'STREAM.UPDATE.%s';
      STREAM_DELETE = 'STREAM.DELETE.%s';
      STREAM_INFO   = 'STREAM.INFO.%s';
      STREAM_PURGE  = 'STREAM.PURGE.%s';
      STREAM_LIST   = 'STREAM.LIST';
      STREAM_NAMES  = 'STREAM.NAMES';

      /// <summary>
      ///   Ephemeral form - the server picks the name. A named or durable
      ///   consumer uses CONSUMER_CREATE_NAMED instead
      /// </summary>
      CONSUMER_CREATE       = 'CONSUMER.CREATE.%s';
      CONSUMER_CREATE_NAMED = 'CONSUMER.CREATE.%s.%s';
      CONSUMER_DELETE       = 'CONSUMER.DELETE.%s.%s';
      CONSUMER_INFO         = 'CONSUMER.INFO.%s.%s';
      CONSUMER_LIST         = 'CONSUMER.LIST.%s';
      CONSUMER_NAMES        = 'CONSUMER.NAMES.%s';
      /// Pull consumption (Phase 4), listed here because it is an API subject
      CONSUMER_MSG_NEXT     = 'CONSUMER.MSG.NEXT.%s.%s';
    end;

    /// <summary>
    ///   Headers a client puts on a JetStream publish. They are ordinary NATS
    ///   headers on an ordinary subject - the stream reads them as it captures
    ///   the message, so publishing with them needs HPUB and nothing else
    /// </summary>
    Header = class
    const
      /// <summary>
      ///   Deduplication id. Inside the stream's duplicate window a second
      ///   message carrying the same id is not stored, and the PubAck comes
      ///   back with "duplicate" set and "seq" pointing at the original
      /// </summary>
      MSG_ID = 'Nats-Msg-Id';

      { The optimistic-concurrency expectations. Each one makes the server
        REJECT the publish unless it holds, which is the only way a client gets
        compare-and-set semantics out of a stream }

      /// The subject must land in this stream and no other
      EXPECTED_STREAM = 'Nats-Expected-Stream';
      /// The stream's last sequence must be this. Zero asserts it is empty
      EXPECTED_LAST_SEQ = 'Nats-Expected-Last-Sequence';
      /// As EXPECTED_LAST_SEQ, but counting only the subject being published to
      EXPECTED_LAST_SUBJECT_SEQ = 'Nats-Expected-Last-Subject-Sequence';
      /// The last stored message must have carried this MSG_ID
      EXPECTED_LAST_MSG_ID = 'Nats-Expected-Last-Msg-Id';
    end;

    /// <summary>
    ///   Characters a stream or consumer name may not contain. A name goes into
    ///   the API subject verbatim, so a dot would silently add a token and
    ///   address a different endpoint entirely
    /// </summary>
    Naming = class
    const
      INVALID_CHARS = '.* >' + #9#13#10;
    end;

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

      { What gets PUBLISHED to that subject to acknowledge the message. The
        server also reads an empty body as a plain ack, but saying which one is
        meant costs four bytes and makes a packet capture readable }

      /// Done with it. In a work-queue stream this is what deletes the message
      PAYLOAD_ACK = '+ACK';
      /// <summary>
      ///   Could not handle it - redeliver. Unlike simply not acking, this does
      ///   not wait out AckWait first
      /// </summary>
      PAYLOAD_NAK = '-NAK';
      /// <summary>
      ///   Still working: resets AckWait without acknowledging anything. The
      ///   one ack that may be sent repeatedly for the same message
      /// </summary>
      PAYLOAD_PROGRESS = '+WPI';
      /// <summary>
      ///   Never redeliver, whatever MaxDeliver says. For a message that will
      ///   fail every time - a poison message
      /// </summary>
      PAYLOAD_TERM = '+TERM';

      /// <summary>
      ///   A NAK asking for redelivery after a specific delay rather than at
      ///   once. The delay is NANOSECONDS, as everywhere else in JetStream
      /// </summary>
      PAYLOAD_NAK_DELAY = '-NAK {"delay":%d}';

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
