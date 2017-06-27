from __future__ import unicode_literals

from sys import (
    argv,
)
from time import (
    time,
)
from json import (
    dumps,
    loads,
)

from twisted.python.url import (
    URL,
)
from twisted.internet.defer import (
    Deferred,
)
from twisted.internet.endpoints import (
    HostnameEndpoint,
)
from twisted.internet.task import (
    react,
)

from autobahn.twisted.websocket import (
    WebSocketClientFactory,
    WebSocketClientProtocol,
)

class MailboxBenchmarkClientProtocol(WebSocketClientProtocol):
    def onMessage(self, payload, isBinary):
        obj = loads(payload)
        getattr(self, "msg_" + obj["type"].upper())(obj)

    def msg_ACK(self, obj):
        pass

    def msg_WELCOME(self, obj):
        self.sendMessage(dumps({
            "type": "bind",
            "appid": "tahoe-lafs.org/benchmark",
            "side": "only",
        }))
        self.sendMessage(dumps({
            "type": "allocate",
        }))

    def msg_ALLOCATED(self, obj):
        self.sendMessage(dumps({
            "type": "claim",
            "nameplate": obj["nameplate"],
        }))

    def msg_CLAIMED(self, obj):
        self.mailbox = obj["mailbox"]
        self.sendMessage(dumps({
            "type": "open",
            "mailbox": self.mailbox,
        }))
        self.ping(0)

    def msg_MESSAGE(self, obj):
        now = time()
        body = loads(obj["body"])
        sent = body["sent"]
        print("Round-trip time: {}".format(now - sent))
        count = body["count"]
        if count < 10:
            self.ping(count + 1)

    def ping(self, count):
        self.sendMessage(dumps({
            "type": "add",
            "phase": "benchmark",
            "body": dumps({
                "count": count,
                "sent": time(),
            }),
        }))


def main(reactor, relay_url):
    factory = WebSocketClientFactory(
        url=relay_url,
        reactor=reactor,
    )
    factory.protocol = MailboxBenchmarkClientProtocol

    url = URL.fromText(relay_url.decode("ascii"))

    endpoint = HostnameEndpoint(reactor, url.host, url.port)
    d = endpoint.connect(factory)
    d.addCallback(lambda ignored: Deferred())
    return d

react(main, argv[1:])
