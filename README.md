# amqp-utils
Toolbox on Golang to manipulate messages from amqp broker

# Install 

Assume that you defined `GOPATH/bin` into `PATH`

    go install github.com/reddec/amqp-utils/cmd/...


Also you may setup a service like this:

File: `amqp-http-pull`

    [Unit]
	Description=AMQP utils - pull messages
	
	[Service]
	ExecStart=/usr/local/bin/amqp-http-pull
	RestartSec=5s
	
	[Install]
	WantedBy=multi-user.target


Just replace `Description` and `ExecStart` to description of service and absolute
path to required executable.

Install service by those command: 
    
	systemctl enable /path/to/file.service
	
And start by

	systemctl start file.service


# HTTP-PUSH

At fact, this is simple web hook, backed by AMQP broker

## Template

You may send message no in JSON by using Golang template: flag `--template <filename>`

Possible values in template:

* Body
* Headers
* ContentType
* ContentEncoding
* DeliveryMode
* Priority
* CorrelationId
* ReplyTo
* Expiration
* MessageId
* Timestamp
* Type
* Exchange
* RoutingKey