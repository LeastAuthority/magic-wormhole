from __future__ import unicode_literals, print_function

from os import urandom
from sys import argv, stderr
from json import dumps
from itertools import count

from twisted.python.usage import UsageError, Options
from twisted.python.url import URL

from twisted.internet.defer import Deferred, gatherResults
from twisted.internet.task import react, cooperate

from wormhole.timing import DebugTiming
from wormhole.xfer_util import send, receive



def url_from_bytes(b):
    return URL.fromText(b.decode("ascii"))



class BenchmarkOptions(Options):
    optParameters = [
        ("data-size", None, None, "The number of bytes to transfer.", int),
        ("relay-url", None, None, "The URL of the wormhole relay server.", url_from_bytes),
    ]



def benchmark(reactor, relay_url, data_size):
    # Just do 5 transfers and return the average.  This is weak-sauce.
    results = []
    task = cooperate(
        benchmark_once(reactor, relay_url, data_size).addCallback(results.append)
        for i
        in range(1)
    )
    d = task.whenDone()
    d.addCallback(lambda ignored: {
        key: sum(result[key] for result in results) / len(results)
        for key
        in results[0]
    })
    return d



counter = count()

def benchmark_once(reactor, relay_url, data_size):
    receive_timing = DebugTiming()
    send_timing = DebugTiming()

    receiving = Deferred()

    before = reactor.seconds()
    stats = {
        "time-to-code": None,
        "time-to-receive": None,
    }

    def start_receiver(code):
        stats["time-to-code"] = reactor.seconds() - before
        d = receive(
            reactor,
            appid="tahoe-lafs.org/benchmark",
            relay_url=relay_url.asText(),
            code=code,
            timing=receive_timing,
        )
        d.chainDeferred(receiving)

    # Start the sender.
    sending = send(
        reactor,
        appid="tahoe-lafs.org/benchmark",
        relay_url=relay_url.asText(),
        data=urandom(data_size / 2).encode("hex"),
        code=None,
        on_code=start_receiver,
        timing=send_timing,
    )

    d = gatherResults([sending, receiving])
    def received(ignored):
        stats["time-to-receive"] = reactor.seconds() - before

        index = next(counter)
        receive_timing.write(
            "tx-{}-{}-{}.json".format(relay_url.host, data_size, index),
            stderr,
        )
        send_timing.write(
            "rx-{}-{}-{}.json".format(relay_url.host, data_size, index),
            stderr,
        )

        return stats
    d.addCallback(received)
    return d



def report(average, relay_url, data_size):
    print(dumps({
        "relay-url": relay_url.asText(),
        "data-size": data_size,
        "average-transfer-seconds": average,
    }))



@react
def main(reactor):
    o = BenchmarkOptions()
    try:
        o.parseOptions(argv[1:])
    except UsageError as e:
        raise SystemExit(e)

    d = benchmark(reactor, o["relay-url"], o["data-size"])
    d.addCallback(report, o["relay-url"], o["data-size"])
    return d
