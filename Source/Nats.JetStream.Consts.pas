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
      ///   Reads one stored message without a consumer - by sequence, or the
      ///   last one on a subject. This is how a key/value get is done: a KV
      ///   bucket is a stream, and "the current value" is "the last message on
      ///   that subject"
      /// </summary>
      STREAM_MSG_GET = 'STREAM.MSG.GET.%s';

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

      /// <summary>
      ///   Makes this message REPLACE everything before it - with ROLLUP_SUBJECT
      ///   only on its own subject. The stream must allow it (AllowRollupHdrs),
      ///   and it is how a KV purge erases a key's history in one publish
      /// </summary>
      ROLLUP = 'Nats-Rollup';
      ROLLUP_SUBJECT = 'sub';
      ROLLUP_ALL = 'all';

      /// <summary>
      ///   The server-to-client counterpart of the publish headers above: a
      ///   PUSH delivery marked as a flow-control request. Modern nats-server
      ///   sends those as status lines instead, but older versions used this
      ///   header on an ordinary message - either way the delivery carries a
      ///   reply subject that MUST be answered
      /// </summary>
      FLOW_CONTROL = 'Nats-Flow-Control';
    end;

    /// <summary>
    ///   Key/Value is a naming convention over a stream, not new protocol: a
    ///   bucket IS a stream, a key IS a subject, and the current value of a key
    ///   is the last message on that subject
    /// </summary>
    KV = class
    const
      /// A bucket named "config" is the stream "KV_config"
      STREAM_PREFIX = 'KV_';
      /// ...capturing '$KV.config.>', one subject per key
      SUBJECT_ALL = '$KV.%s.>';
      SUBJECT_KEY = '$KV.%s.%s';

      /// <summary>
      ///   What marks a message as a tombstone rather than a value. Absent for
      ///   an ordinary put, so a message with no KV-Operation IS the value
      /// </summary>
      HEADER_OPERATION = 'KV-Operation';
      /// Deletes the key but keeps its history
      OP_DELETE = 'DEL';
      /// Deletes the key AND its history, via a subject rollup
      OP_PURGE = 'PURGE';

      /// How many revisions of each key are kept when the config says nothing
      DEFAULT_HISTORY = 1;
      /// <summary>
      ///   The server's own ceiling on MaxMsgsPerSubject for a bucket. Asking
      ///   for more is rejected, so it is checked here where the message can
      ///   say what the limit is
      /// </summary>
      MAX_HISTORY = 64;

      /// <summary>
      ///   A bucket name becomes part of a stream name and of every subject, so
      ///   it is restricted to what is safe in both
      /// </summary>
      VALID_BUCKET_CHARS = ['A'..'Z', 'a'..'z', '0'..'9', '_', '-'];
      /// <summary>
      ///   A key becomes a subject TOKEN, so dots are allowed - they simply
      ///   make it several tokens - but wildcards are not, and it may not begin
      ///   or end with a dot because that would produce an empty token
      /// </summary>
      VALID_KEY_CHARS = ['A'..'Z', 'a'..'z', '0'..'9', '_', '-', '/', '=', '.'];
    end;

    /// <summary>
    ///   Object Store is a second convention over a stream, and a thicker one
    ///   than Key/Value: an object is SPLIT across many messages, so the bucket
    ///   holds two kinds of message - the chunks, and one metadata record per
    ///   object saying how to put them back together
    /// </summary>
    Obj = class
    const
      /// A bucket named "files" is the stream "OBJ_files"
      STREAM_PREFIX = 'OBJ_';

      { The two subject spaces the bucket captures }

      SUBJECT_CHUNKS_ALL = '$O.%s.C.>';
      SUBJECT_META_ALL   = '$O.%s.M.>';

      /// <summary>
      ///   Every chunk of ONE object shares ONE subject, keyed by a NUID rather
      ///   than by the object's name. Two consequences: the chunks come back in
      ///   order because they are in stream order, and replacing an object is
      ///   just writing a new NUID and purging the old subject
      /// </summary>
      SUBJECT_CHUNK = '$O.%s.C.%s';

      /// <summary>
      ///   The metadata subject carries the object's name BASE64URL-encoded,
      ///   because an object name is arbitrary text and a subject token cannot
      ///   hold spaces, dots or wildcards
      /// </summary>
      SUBJECT_META = '$O.%s.M.%s';

      /// <summary>
      ///   How much of an object goes in one message. Well under the usual 1 MB
      ///   max_payload, because the whole point is not to depend on it
      /// </summary>
      DEFAULT_CHUNK_SIZE = 128 * 1024;

      /// <summary>
      ///   The digest is stored as "SHA-256=&lt;base64url of the raw hash&gt;".
      ///   The algorithm is named rather than assumed so a later one can be
      ///   told apart from this one
      /// </summary>
      DIGEST_PREFIX = 'SHA-256=';

      /// As for a KV bucket, and for the same reasons
      VALID_BUCKET_CHARS = ['A'..'Z', 'a'..'z', '0'..'9', '_', '-'];
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

    /// <summary>
    ///   The err_code values an API or PubAck error object can carry - the
    ///   field worth branching on (the HTTP-like code only says 400/404/503).
    ///   Only the ones this library needs to recognise are named
    /// </summary>
    ErrCode = class
    const
      /// STREAM.MSG.GET found nothing for the requested sequence or subject
      NO_MESSAGE_FOUND = 10037;
      /// The stream named in the request does not exist
      STREAM_NOT_FOUND = 10059;
      /// The last-sequence expectation did not hold (Nats-Expected-Last-Sequence)
      WRONG_LAST_SEQ = 10071;
      /// Nats-Expected-Last-Subject-Sequence did not hold - the classic code
      WRONG_LAST_SUBJECT_SEQ = 10072;
      /// <summary>
      ///   Newer servers report the same Nats-Expected-Last-Subject-Sequence
      ///   failure under this code instead of 10072 - observed by the nats.py
      ///   client's KV update. A caller that only cares "the CAS lost" accepts
      ///   both
      /// </summary>
      WRONG_LAST_SUBJECT_SEQ_NEW = 10164;
    end;
  end;

implementation

end.
