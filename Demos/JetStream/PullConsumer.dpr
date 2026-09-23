{******************************************************************************}
{                                                                              }
{  nats.delphi: Delphi Client Library for NATS                                 }
{  Copyright (c) 2022 Paolo Rossi                                              }
{  https://github.com/paolo-rossi/nats.delphi                                  }
{                                                                              }
{  Licensed under the MIT license                                              }
{******************************************************************************}
{                                                                              }
{  Pull consumers - the natsbyexample.com "Pull Consumers" example, ported to }
{  nats.delphi. A consumer asks the server for up to N messages at a time and }
{  acknowledges each one:                                                      }
{                                                                              }
{    1. create a stream (EVENTS, subjects events.>)                            }
{    2. create a durable pull consumer (processor)                             }
{    3. publish a few messages                                                 }
{    4. Fetch up to 5 of them                                                  }
{    5. ack each one when done with it                                         }
{                                                                              }
{  Requires a nats-server with JetStream enabled, e.g.:                       }
{      nats-server -js                                                         }
{                                                                              }
{  The stream name is NUID-suffixed so consecutive runs never collide, and    }
{  everything is deleted again in the cleanup section.                        }
{******************************************************************************}
program PullConsumer;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,

  Nats.Consts in '..\Source\Nats.Consts.pas',
  Nats.Socket in '..\Source\Nats.Socket.pas',
  Nats.Socket.Indy in '..\Source\Nats.Socket.Indy.pas',
  Nats.Connection in '..\Source\Nats.Connection.pas',
  Nats.Nuid in '..\Source\Nats.Nuid.pas',
  Nats.JetStream.Client in '..\Source\Nats.JetStream.Client.pas',
  Nats.JetStream.Consts in '..\Source\Nats.JetStream.Consts.pas',
  Nats.JetStream.Entities in '..\Source\Nats.JetStream.Entities.pas',
  Nats.JetStream.Message in '..\Source\Nats.JetStream.Message.pas';

procedure Banner(const ATitle: string);
begin
  Writeln;
  Writeln('=== ', ATitle, ' ', StringOfChar('=', 52 - Length(ATitle)));
end;

begin
  ReportMemoryLeaksOnShutdown := True;

  Writeln('nats.delphi - Pull consumers (natsbyexample.com)');
  Writeln(StringOfChar('=', 56));

  var nc := TNatsConnection.Create
    .SetName('PullConsumer')
    .SetChannel('127.0.0.1', NatsConstants.DEFAULT_PORT, 5000);
  try
    Banner('Connecting');
    try
      nc.Open(nil);
    except
      on E: Exception do
      begin
        Writeln;
        Writeln('FAILED: ', E.ClassName, ': ', E.Message);
        Writeln('  Is a nats-server with JetStream running? Start one with "nats-server -js".');
        ExitCode := 1;
      end;
    end;

    if not nc.WaitForReady(5000) then
      raise Exception.Create('The handshake did not complete: ' + nc.LastError);

    var js := TJetStreamContext.Create(nc);
    Writeln('  connected, server max_payload ', nc.MaxPayload);
    try

      //PullConsume
      Banner('Pull consumption');
      var StreamName := 'EVENTS_' + TNUID.NextNuid; // EVENTS_<nuid>: the stream name, unique per run

      { 1. Create a stream }
      var cfg := Default(TJetStreamStreamConfig);
      cfg.Name := StreamName;
      cfg.Subjects := [StreamName + '.>'];
      var nfo := js.AddStream(cfg);
      Writeln('  created stream ', nfo.Config.Name);

      { 2. Create a durable pull consumer. A DurableName makes the server
        remember its position across restarts; no DeliverSubject keeps it a PULL
        consumer }
      var cc := Default(TJetStreamConsumerConfig);
      cc.DurableName := 'processor';
      cc.AckPolicy := TJetStreamAckPolicy.Explicit;

      var ci := js.AddConsumer(StreamName, cc);
      Writeln('  created durable consumer ', ci.Name);

      { 3. Publish a few messages }
      for var I := 0 to 9 do
        js.Publish(Format('%s.%d', [StreamName, I]), Format('data-%d', [I]));
      Writeln('  published 10 messages to ', StreamName, '.0..9');

      { 4. Ask the consumer for up to 5 messages. Fetch blocks until they arrive,
        the server closes the batch, or the timeout elapses }
      var msgs := js.Fetch(StreamName, 'processor', 5, 2000);

      { 5. Handle each message, then ack it }
      for var msg in msgs do
      begin
        Writeln('    got message: ', msg.Payload, ' (seq ', msg.Metadata.StreamSeq, ')');
        msg.Ack;   { +ACK: the work for this message is done }
      end;
      Writeln('  fetched and acked ', Length(msgs), ' message(s)');


      Banner('Done');
      Writeln('  everything worked. The consumer pulled a batch and acked it.');

      //Cleanup
      Banner('Cleanup');
      if Assigned(nc) and nc.Connected then
      try
        if js.DeleteStream(StreamName) then
          Writeln('  deleted stream ', StreamName);
      except
        on E: Exception do
          Writeln('  (cleanup failed: ', E.ClassName, ': ', E.Message, ')');
      end;

    finally
      js.Free;
    end;
  finally
    nc.Free;
  end;
end.
