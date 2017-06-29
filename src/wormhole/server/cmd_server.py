from __future__ import print_function, unicode_literals
import os, time
import click
from zope.interface import implementer
from twisted.python import usage
from twisted import plugin
from twisted.scripts import twistd
from twisted.application import service

from .cli import LaunchArgs

@implementer(service.IServiceMaker, plugin.IPlugin)
class MyPlugin(object):
    tapname = "magic-wormhole-server"

    description = "Securely transfer data between computers (server)"

    class options(object):
        def parseOptions(self, argv):
            @click.command()
            @LaunchArgs
            def parse(**kwargs):
                self.__dict__.update(kwargs)
            parse.main(argv, standalone_mode=False)

    def __init__(self, args=None):
        self.args = args

    def makeService(self, so):
        # delay this import as late as possible, to allow twistd's code to
        # accept --reactor= selection
        from .server import RelayServer

        if self.args is not None:
            so = self.args

        return RelayServer(
            str(so.rendezvous),
            str(so.transit),
            so.advertise_version,
            so.relay_database_path,
            so.blur_usage,
            signal_error=so.signal_error,
            stats_file=so.stats_json_path,
            allow_list=so.allow_list,
        )



class MyTwistdConfig(twistd.ServerOptions):
    subCommands = [("XYZ", None, usage.Options, "node")]

def start_server(args):
    c = MyTwistdConfig()
    #twistd_args = tuple(args.twistd_args) + ("XYZ",)
    base_args = []
    if args.no_daemon:
        base_args.append("--nodaemon")
    twistd_args = base_args + ["XYZ"]
    c.parseOptions(tuple(twistd_args))
    c.loadedPlugins = {"XYZ": MyPlugin(args)}

    print("starting wormhole relay server")
    # this forks and never comes back. The parent calls os._exit(0)
    twistd.runApp(c)

def kill_server():
    try:
        f = open("twistd.pid", "r")
    except EnvironmentError:
        print("Unable to find twistd.pid: is this really a server directory?")
        print("oh well, ignoring 'stop'")
        return
    pid = int(f.read().strip())
    f.close()
    os.kill(pid, 15)
    print("server process %d sent SIGTERM" % pid)
    return

def stop_server(args):
    kill_server()

def restart_server(args):
    kill_server()
    time.sleep(0.1)
    timeout = 0
    while os.path.exists("twistd.pid") and timeout < 10:
        if timeout == 0:
            print(" waiting for shutdown..")
        timeout += 1
        time.sleep(1)
    if os.path.exists("twistd.pid"):
        print("error: unable to shut down old server")
        return 1
    print(" old server shut down")
    start_server(args)
