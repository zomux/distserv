from twisted.internet.protocol import Protocol, ReconnectingClientFactory
from sys import stdout
import imp
import os
from datetime import datetime
import json
from multiprocessing import Process
from twisted.internet import reactor
import random

SERVER_HOST = 'serv.uaca.com'
SERVER_PORT = 8123
CLIENT_PROCESSES = 5

class DistClient(Protocol):

    def connectionMade(self):
        """
        Regularly reconnect.
        """
        delay = random.randint(5*60,10*60)
        reactor.callLater(delay, self.transport.loseConnection)

    def dataReceived(self, data):
        """
        @type data: str
        """
        result = self.processData(data)
        if result is not None:
            self.transport.write(json.dumps(result))

    def processData(self, data):
        """
        @type data: str
        """
        data = data.strip()
        if "\n" not in data:
            return "[Error] Unknown data format"
        appName, param = data.split("\n", 1)
        pathApp = "applications/%s.py" % appName
        if not os.path.exists(pathApp):
            return "[Error] Application %s does not exist" % appName
        app = imp.load_source(appName, pathApp)
        if 'run' not in dir(app):
            return "[Error] Function 'run' is not defined in application %s" % appName
        try:
            print "[%s] %s" % (datetime.now().strftime("%Y-%m-%d %H:%M"), appName)
            print param
            return app.run(json.loads(param))
        except Exception as e:
            return "[Error] %s" % e.message


class DistClientFactory(ReconnectingClientFactory):
    def startedConnecting(self, connector):
        print '[DistClientFactory] Started to connect.'

    def buildProtocol(self, addr):
        print '[DistClientFactory] Connected.'
        # print '[DistClientFactory] Resetting reconnection delay'
        self.resetDelay()
        return DistClient()

    def clientConnectionLost(self, connector, reason):
        print '[DistClientFactory] Lost connection.  Reason:', reason
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        print '[DistClientFactory] Connection failed. Reason:', reason
        ReconnectingClientFactory.clientConnectionFailed(self, connector,
                                                         reason)


if __name__ == '__main__':
    for num in range(CLIENT_PROCESSES):
        reactor.connectTCP(SERVER_HOST, SERVER_PORT, DistClientFactory())
    reactor.run()