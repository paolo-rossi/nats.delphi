# Delphi client for NATS, the cloud native messaging system.


![Delphi For NATS](https://user-images.githubusercontent.com/4686497/177811743-be0f8bfc-5672-4c3d-84e3-7b39122c9d3f.png)


[Delphi](https://www.embarcadero.com/products/delphi) **native** implementation of a [NATS](https://nats.io) Client Library. This library supports Core NATS, JetStream is currently under implementation.

![Repo Created At](https://img.shields.io/github/created-at/paolo-rossi/nats.delphi)
[![License Apache 2][License-Image]][License-Url]
![Commit Activity](https://img.shields.io/github/commit-activity/m/paolo-rossi/nats.delphi)
![GitHub Contributors](https://img.shields.io/github/contributors/paolo-rossi/nats.delphi)


[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg

## Installation

```bash
# To get the latest released Go client:
go get github.com/nats-io/nats.go@latest

# To get a specific version:
go get github.com/nats-io/nats.go@v1.44.0

# Note that the latest major version for NATS Server is v2:
go get github.com/nats-io/nats-server/v2@latest
```

## Basic Usage

### Connecting

```pascal
uses
    Nats.Consts,
    Nats.Entities,
    Nats.Connection;


// Create a connection object
LConnection := TNatsConnection.Create;


// Connect to a NATS Server
LConnection.SetChannel('localhost', 4222, 1000).
    Open(
        procedure (AInfo: TNatsServerInfo; var AConnectOptions: TNatsConnectOptions)
        begin
            TThread.Queue(TThread.Current,
            procedure
            begin
                Log('Connected to server ' + AInfo.server_name);
            end
            );
        end,
        procedure
        begin
            TThread.Queue(TThread.Current,
            procedure
            begin
                Log('Disconnected from the server');
            end
            );

        end
        )
;
```

### Publishing messages
```pascal
LConnection.Publish('mysubject', 'My Message');
```

### Subscribing to subjects
```pascal
LConnection.Subscribe('mysubject', 
    procedure (const AMsg: TNatsArgsMSG)
    begin
        // Code to handle received Msg with subject "mysubject"
        // ** Remember! your code here must be thread safe!
        Log('Message received from NATS server: ' + AMsg);
    end
);
```

### Unubscribing to subjects
```pascal
  LConnection.Unsubscribe('mysubject');
```



## JetStream


JetStream is the built-in NATS persistence system. `nats.delphi` provides a built-in
API enabling both managing JetStream assets as well as publishing/consuming
persistent messages.

JetStream support is under active development.

