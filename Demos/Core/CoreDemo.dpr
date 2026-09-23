{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{                                                                              }
{******************************************************************************}
{                                                                              }
{  Core NATS from the console - the ground DemoNats covers with a VCL form,    }
{  in a script you can read top to bottom:                                     }
{                                                                              }
{     1. connect, and read the server's INFO                                   }
{     2. publish / subscribe                                                   }
{     3. wildcard subjects (* and >)                                           }
{     4. queue groups - one message to one member of the group                 }
{     5. request / reply, both the blocking and the callback form              }
{     6. headers (HPUB / HMSG)                                                 }
{     7. binary payloads                                                       }
{     8. auto-unsubscribe after N messages                                     }
{     9. a request nobody answers                                              }
{    10. the max_payload guard                                                 }
{                                                                              }
{  Requires a plain nats-server on localhost:4222 - no JetStream needed:       }
{      nats-server                                                             }
{                                                                              }
{  Everything happens on ONE connection: the server echoes a client's own      }
{  publishes back to it (ConnectOptions.Echo is True), so this process is      }
{  both publisher and subscriber. Subjects are NUID-suffixed, so two copies    }
{  running at once do not hear each other.                                     }
{                                                                              }
{  THREADING: every handler below runs on the TNatsConsumer thread, never on   }
{  the main one. That is why they only ever take the log lock, signal an event }
{  or publish - and why none of them calls RequestSync, which would block the  }
{  very thread that has to deliver its reply.                                  }
{                                                                              }
{  There is no Sleep anywhere in here. Waiting on a signal the client actually }
{  gives - an event set by a handler, or a round trip to the server - is both  }
{  faster and honest; a sleep would turn a broken client into a slow one.      }
{******************************************************************************}
program CoreDemo;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Diagnostics,

  Nats.Consts in '..\..\Source\Nats.Consts.pas',
  Nats.Classes in '..\..\Source\Nats.Classes.pas',
  Nats.Entities in '..\..\Source\Nats.Entities.pas',
  Nats.Exceptions in '..\..\Source\Nats.Exceptions.pas',
  Nats.Parser in '..\..\Source\Nats.Parser.pas',
  Nats.Socket in '..\..\Source\Nats.Socket.pas',
  Nats.Socket.Indy in '..\..\Source\Nats.Socket.Indy.pas',
  Nats.Connection in '..\..\Source\Nats.Connection.pas',
  Nats.Nuid in '..\..\Source\Nats.Nuid.pas';

const
  JOB_COUNT = 9;      // jobs handed to the queue group in section 4
  LIMITED_MAX = 3;    // auto-unsubscribe threshold in section 8
  LIMITED_SENT = 6;   // and how many are published against it

var
  { Writeln is not reentrant and the handlers run on another thread, so every
    line - from the main thread or from the consumer - goes through this }
  GLogLock: TCriticalSection;

procedure Log(const AMessage: string);
begin
  GLogLock.Enter;
  try
    Writeln(AMessage);
  finally
    GLogLock.Leave;
  end;
end;

procedure LogFmt(const AMessage: string; const AArgs: array of const);
begin
  Log(Format(AMessage, AArgs));
end;

procedure Banner(const ATitle: string);
begin
  Log('');
  Log('=== ' + ATitle + ' ' + StringOfChar('=', 56 - Length(ATitle)));
end;

/// <summary>
///   Waits for a handler to signal, and says so when it does not. A demo that
///   carried on silently after a message never arrived would print a wall of
///   successes for a broken client
/// </summary>
function Await(AEvent: TLightweightEvent; ATimeoutMs: Cardinal; const AWhat: string): Boolean;
begin
  Result := AEvent.WaitFor(ATimeoutMs) = TWaitResult.wrSignaled;
  if not Result then
    LogFmt('  !! timed out after %d ms waiting for %s', [ATimeoutMs, AWhat]);
end;

/// <summary>
///   Reads an Integer written by the consumer thread. CompareExchange with the
///   same comparand changes nothing and returns the current value
/// </summary>
function AtomicRead(var ATarget: Integer): Integer;
begin
  Result := TInterlocked.CompareExchange(ATarget, 0, 0);
end;

function Hex(const AData: TBytes): string;
begin
  Result := '';
  for var LByte in AData do
    Result := Result + IntToHex(LByte, 2) + ' ';
  Result := Result.Trim;
end;

begin
  ReportMemoryLeaksOnShutdown := True;
  GLogLock := TCriticalSection.Create;
  try
    Log('nats.delphi - Core NATS from the console');
    Log(StringOfChar('=', 60));

    { One suffix for the whole run, so every subject below belongs to this
      process alone }
    var LRun := TNUID.NextNuid;
    var LFlushSubject := Format('demo.%s.flush', [LRun]);

    var LConnected := TLightweightEvent.Create;
    var nc := TNatsConnection.Create
      .SetName('CoreDemo')
      .SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, 5000);
    try
      { A failure on a worker thread reaches the application here and nowhere
        else - without a handler a dropped connection is silent }
      nc.OnError :=
        procedure (const AError: string)
        begin
          Log('  !! connection error: ' + AError);
        end;

      //Connect
      Banner('1. Connecting');
      try
        nc.Open(
          { The connect handler fires on INFO, BEFORE CONNECT is written, and
            receives the options as var so they can still be changed }
          procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
          begin
            LogFmt('  INFO from %s (%s), protocol %d', [AInfo.ServerName, AInfo.Version, AInfo.Proto]);
            LogFmt('  client id %d from %s, max_payload %d, headers %s',
              [AInfo.ClientId, AInfo.ClientIp, AInfo.MaxPayload, BoolToStr(AInfo.Headers, True)]);
            LConnected.SetEvent;
          end,
          procedure
          begin
            Log('  disconnected from the server');
          end
        );
      except
        on E: Exception do
        begin
          Log('');
          LogFmt('FAILED: %s: %s', [E.ClassName, E.Message]);
          Log('  Is a nats-server running on localhost:4222? Start one with "nats-server".');
          ExitCode := 1;
          Exit;
        end;
      end;

      { Open only STARTS the handshake. Connected means READY - CONNECT on the
        wire - and until then the server knows nothing about this client }
      if not nc.WaitForReady(5000) then
        raise Exception.Create('The handshake did not complete: ' + nc.LastError);
      Await(LConnected, 1000, 'the connect handler');
      LogFmt('  ready: Connected=%s, MaxPayload=%d', [BoolToStr(nc.Connected, True), nc.MaxPayload]);

      { A responder kept alive for the whole run. A round trip through it is a
        barrier: the server reads one connection's commands in order, so once
        its reply comes back everything published earlier has been dealt with.
        Section 8 needs exactly that, and it beats sleeping for a guess }
      var LFlushSid := nc.Subscribe(LFlushSubject,
        procedure (const AMsg: TNatsArgsMSG)
        begin
          if not AMsg.ReplyTo.IsEmpty then
            nc.Publish(AMsg.ReplyTo, 'flushed');
        end
      );

      //PubSub
      Banner('2. Publish / Subscribe');
      var LGreetingSubject := Format('demo.%s.greeting', [LRun]);
      var LGreeting := TLightweightEvent.Create;
      try
        var LSid := nc.Subscribe(LGreetingSubject,
          procedure (const AMsg: TNatsArgsMSG)
          begin
            LogFmt('  <- [sid %d] %s: %s', [AMsg.Id, AMsg.Subject, AMsg.Payload]);
            LGreeting.SetEvent;
          end
        );
        LogFmt('  subscribed to %s (sid %d)', [LGreetingSubject, LSid]);

        nc.Publish(LGreetingSubject, 'Hello, NATS!');
        Log('  -> published "Hello, NATS!"');
        Await(LGreeting, 2000, 'the greeting');

        nc.Unsubscribe(LSid);
        Log('  unsubscribed');
      finally
        LGreeting.Free;
      end;

      //Wildcards
      Banner('3. Wildcard subjects');
      var LSensorRoot := Format('demo.%s.sensors', [LRun]);
      var LWildcards := TLightweightEvent.Create;
      var LWildcardCount := 0;
      try
        { * matches exactly ONE token, > matches one or more to the end of the
          subject - so sensors.temperature reaches both, sensors.room1.humidity
          only the second }
        var LStarSid := nc.Subscribe(LSensorRoot + '.*',
          procedure (const AMsg: TNatsArgsMSG)
          begin
            LogFmt('  <- [*]  %s = %s', [AMsg.Subject, AMsg.Payload]);
            if TInterlocked.Increment(LWildcardCount) = 3 then
              LWildcards.SetEvent;
          end
        );
        var LArrSid := nc.Subscribe(LSensorRoot + '.>',
          procedure (const AMsg: TNatsArgsMSG)
          begin
            LogFmt('  <- [>]  %s = %s', [AMsg.Subject, AMsg.Payload]);
            if TInterlocked.Increment(LWildcardCount) = 3 then
              LWildcards.SetEvent;
          end
        );
        LogFmt('  subscribed to %s.* and %s.>', [LSensorRoot, LSensorRoot]);

        nc.Publish(LSensorRoot + '.temperature', '21.5');
        nc.Publish(LSensorRoot + '.room1.humidity', '48');
        Await(LWildcards, 2000, 'the three wildcard deliveries');
        LogFmt('  2 messages published, %d delivered', [AtomicRead(LWildcardCount)]);

        nc.Unsubscribe(LStarSid);
        nc.Unsubscribe(LArrSid);
      finally
        LWildcards.Free;
      end;

      //QueueGroups
      Banner('4. Queue groups');
      var LJobSubject := Format('demo.%s.jobs', [LRun]);
      var LJobsDone := TLightweightEvent.Create;
      var LWorker0 := 0;
      var LWorker1 := 0;
      var LWorker2 := 0;
      var LJobTotal := 0;
      try
        { Same subject, same queue name: the server picks ONE member per message
          instead of delivering to all of them. Written out three times rather
          than looped, because an anonymous method captures a loop variable by
          reference and all three handlers would share the last index }
        var LSid0 := nc.Subscribe(LJobSubject, 'workers',
          procedure (const AMsg: TNatsArgsMSG)
          begin
            TInterlocked.Increment(LWorker0);
            if TInterlocked.Increment(LJobTotal) = JOB_COUNT then
              LJobsDone.SetEvent;
          end
        );
        var LSid1 := nc.Subscribe(LJobSubject, 'workers',
          procedure (const AMsg: TNatsArgsMSG)
          begin
            TInterlocked.Increment(LWorker1);
            if TInterlocked.Increment(LJobTotal) = JOB_COUNT then
              LJobsDone.SetEvent;
          end
        );
        var LSid2 := nc.Subscribe(LJobSubject, 'workers',
          procedure (const AMsg: TNatsArgsMSG)
          begin
            TInterlocked.Increment(LWorker2);
            if TInterlocked.Increment(LJobTotal) = JOB_COUNT then
              LJobsDone.SetEvent;
          end
        );
        LogFmt('  3 members of queue group "workers" on %s', [LJobSubject]);

        for var I := 1 to JOB_COUNT do
          nc.Publish(LJobSubject, Format('job-%d', [I]));
        LogFmt('  -> published %d jobs', [JOB_COUNT]);

        Await(LJobsDone, 3000, 'the jobs to be dispatched');
        LogFmt('    worker 0 handled %d job(s)', [AtomicRead(LWorker0)]);
        LogFmt('    worker 1 handled %d job(s)', [AtomicRead(LWorker1)]);
        LogFmt('    worker 2 handled %d job(s)', [AtomicRead(LWorker2)]);
        LogFmt('  %d delivered in total - each job went to exactly one member',
          [AtomicRead(LJobTotal)]);

        nc.Unsubscribe(LSid0);
        nc.Unsubscribe(LSid1);
        nc.Unsubscribe(LSid2);
      finally
        LJobsDone.Free;
      end;

      //RequestReply
      Banner('5. Request / Reply');
      var LEchoSubject := Format('demo.%s.echo', [LRun]);
      var LAsyncReply := TLightweightEvent.Create;
      try
        { The responder. Publishing to AMsg.ReplyTo is the whole of "reply":
          a request is just a publish carrying an inbox to answer on }
        var LResponderSid := nc.Subscribe(LEchoSubject,
          procedure (const AMsg: TNatsArgsMSG)
          begin
            LogFmt('  responder <- "%s" (reply-to %s)', [AMsg.Payload, AMsg.ReplyTo]);
            if not AMsg.ReplyTo.IsEmpty then
              nc.Publish(AMsg.ReplyTo, 'you said: ' + AMsg.Payload);
          end
        );
        LogFmt('  responder listening on %s', [LEchoSubject]);

        { The blocking form. Safe HERE because this is the main thread; from a
          handler it would block the very thread that delivers the reply }
        var LReply: TNatsArgsMSG;
        if nc.RequestSync(LEchoSubject, 'ping', LReply, 2000) then
          LogFmt('  RequestSync -> "%s"', [LReply.Payload])
        else
          Log('  !! RequestSync timed out');

        { The callback form: returns at once, the reply lands on the consumer
          thread }
        nc.Request(LEchoSubject, 'ping again',
          procedure (const AMsg: TNatsArgsMSG)
          begin
            LogFmt('  Request callback -> "%s"', [AMsg.Payload]);
            LAsyncReply.SetEvent;
          end
        );
        Await(LAsyncReply, 2000, 'the async reply');

        nc.Unsubscribe(LResponderSid);
      finally
        LAsyncReply.Free;
      end;

      //Headers
      Banner('6. Headers');
      var LHeaderSubject := Format('demo.%s.headers', [LRun]);
      var LHeadersSeen := TLightweightEvent.Create;
      try
        var LSid := nc.Subscribe(LHeaderSubject,
          procedure (const AMsg: TNatsArgsMSG)
          begin
            LogFmt('  <- %s: %s', [AMsg.Subject, AMsg.Payload]);
            for var LHeader in AMsg.Headers do
              LogFmt('       %s: %s', [LHeader.Key, LHeader.Value]);
            LHeadersSeen.SetEvent;
          end
        );

        { Headers travel as HPUB/HMSG, which this server accepts only because
          ConnectOptions.Headers was True in the CONNECT - a client that has not
          declared header support gets the connection closed for sending one }
        var LHeaders: TNatsHeaders;
        LHeaders.Add('Content-Type', 'application/json');
        LHeaders.Add('X-Request-Id', TNUID.NextNuid);

        nc.Publish(LHeaderSubject, '{"ok":true}', '', LHeaders);
        Log('  -> published with 2 headers');
        Await(LHeadersSeen, 2000, 'the message with headers');

        nc.Unsubscribe(LSid);
      finally
        LHeadersSeen.Free;
      end;

      //Binary
      Banner('7. Binary payloads');
      var LBinarySubject := Format('demo.%s.binary', [LRun]);
      var LBinarySeen := TLightweightEvent.Create;
      try
        var LSid := nc.Subscribe(LBinarySubject,
          procedure (const AMsg: TNatsArgsMSG)
          begin
            { Payload is the UTF-8 READING of the bytes, and is empty when they
              are not valid UTF-8; PayloadData is always the bytes themselves,
              and PayloadBytes is the count the server declared }
            LogFmt('  <- %d byte(s): %s', [AMsg.PayloadBytes, Hex(AMsg.PayloadData)]);
            if AMsg.Payload.IsEmpty then
              Log('     as text: empty - these bytes are not valid UTF-8')
            else
              LogFmt('     as text: "%s"', [AMsg.Payload]);
            LBinarySeen.SetEvent;
          end
        );

        var LData: TBytes := [0, 1, 2, 250, 251, 252, 255];
        nc.PublishBytes(LBinarySubject, LData);
        LogFmt('  -> published %d raw bytes', [Length(LData)]);
        Await(LBinarySeen, 2000, 'the binary message');

        nc.Unsubscribe(LSid);
      finally
        LBinarySeen.Free;
      end;

      //AutoUnsub
      Banner('8. Auto-unsubscribe after N messages');
      var LLimitedSubject := Format('demo.%s.limited', [LRun]);
      var LLimitedCount := 0;

      var LLimitedSid := nc.Subscribe(LLimitedSubject,
        procedure (const AMsg: TNatsArgsMSG)
        begin
          LogFmt('  <- %s', [AMsg.Payload]);
          TInterlocked.Increment(LLimitedCount);
        end
      );

      { UNSUB <sid> 3: the server delivers three in total and then drops the
        subscription on its own. The client drops its own copy at the same
        count, so nothing lingers here either }
      nc.Unsubscribe(LLimitedSid, LIMITED_MAX);
      LogFmt('  auto-unsubscribe armed at %d messages', [LIMITED_MAX]);

      for var I := 1 to LIMITED_SENT do
        nc.Publish(LLimitedSubject, Format('message-%d', [I]));
      LogFmt('  -> published %d messages', [LIMITED_SENT]);

      { The barrier. Every one of those publishes was written before this
        request, the server handles a connection's commands in order, and the
        consumer thread runs the deliveries before it runs this reply - so when
        RequestSync returns, the count below is final }
      var LFlushReply: TNatsArgsMSG;
      if not nc.RequestSync(LFlushSubject, '', LFlushReply, 2000) then
        Log('  !! the flush round trip timed out');

      LogFmt('  %d of %d delivered - the server dropped the rest',
        [AtomicRead(LLimitedCount), LIMITED_SENT]);

      //NoAnswer
      Banner('9. A request nobody answers');
      var LSilentReply: TNatsArgsMSG;
      var LStart := TStopwatch.StartNew;
      if nc.RequestSync(Format('demo.%s.nobody.here', [LRun]), 'anyone?', LSilentReply, 1000) then
        Log('  !! unexpected: something answered')
      else
        LogFmt('  RequestSync returned False after %d ms - a timeout, not an error',
          [LStart.ElapsedMilliseconds]);
      Log('  (the inbox subscription is removed on every exit path, timeout included)');

      //MaxPayload
      Banner('10. The max_payload guard');
      try
        { Checked client-side BEFORE a byte is written. Left to the server this
          is fatal: -ERR "Maximum Payload Violation" closes the connection and
          takes every subscription with it }
        nc.Publish(Format('demo.%s.toobig', [LRun]), StringOfChar('x', nc.MaxPayload + 1));
        Log('  !! unexpected: the oversized publish was allowed');
      except
        on E: ENatsMaxPayloadError do
          LogFmt('  refused locally: %s', [E.Message]);
      end;
      LogFmt('  still connected: %s', [BoolToStr(nc.Connected, True)]);

      //Done
      Banner('Done');
      nc.Unsubscribe(LFlushSid);
      LogFmt('  open subscriptions left: %d', [Length(nc.GetSubscriptionList)]);
      nc.Close;
      Log('  connection closed');
    finally
      nc.Free;
      LConnected.Free;
    end;
  finally
    GLogLock.Free;
  end;
end.
