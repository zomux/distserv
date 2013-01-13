from twisted.internet.protocol import Factory, Protocol
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from threading import Lock
import json

mutex = Lock()

VERSION = "distserv 0.1"

class Distserv(Protocol):

    def __init__(self, factory, name):
        self.factory = factory
        self.name = name
        self.isBusy = False

    def connectionMade(self):
        self.factory.clients[self.name] = self
        self.transport.write("hello\n%s" % VERSION)

    def connectionLost(self, reason):
        if self.factory.clients.has_key(self.name):
            del self.factory.clients[self.name]

    def dataReceived(self, data):
        self.isBusy = False
        self.factory.makeResponse(self.name, data)


    def doTask(self, data):
        self.isBusy = True
        self.transport.write(data)


class DistservFactory(Factory):

    def __init__(self):
        self.idMax = 0
        # name -> protocol
        self.clients = {}
        # name -> queryConnection
        self.queryPool = {}
        # (data, queryConnection)
        self.waitingPool = []

    def buildProtocol(self, addr):

        return Distserv(self, self.generateIdentity())

    def generateIdentity(self):
        self.idMax += 1
        return self.idMax

    def processWaitingPool(self):
        global mutex
        mutex.acquire()
        if not self.waitingPool:
            mutex.release()
            return

        allClientsAreBusy = True

        for name in self.clients:
            client = self.clients[name]
            if not client.isBusy:
                allClientsAreBusy = False
                data, queryConnection = self.waitingPool.pop(0)
                self.queryPool[name] = queryConnection
                client.doTask(data)
                break
        mutex.release()
        if not allClientsAreBusy:
            self.processWaitingPool()

    def makeResponse(self, name, responseData):
        if name not in self.queryPool:
            return
        queryConnection = self.queryPool[name]
        queryConnection.doResponse(responseData)
        del self.queryPool[name]
        self.processWaitingPool()

    def query(self, data, queryConnection):
        """
        @type queryConnection: QueryServer
        """
        if not self.clients:
            queryConnection.doResponse("NOCLIENTS")
        mutex.acquire()
        self.waitingPool.append((data, queryConnection))
        mutex.release()
        self.processWaitingPool()

class QueryServer(Protocol):

    def __init__(self, distservFactory):
        self.distservFactory = distservFactory

    def connectionMade(self):
        pass

    def connectionLost(self, reason):
        pass

    def dataReceived(self, data):
        self.distservFactory.query(data, self)

    def doResponse(self,data):
        self.transport.write(data)
        # do not close the connect immediately
        #self.transport.loseConnection()


class QueryServerFactory(Factory):

    def __init__(self, distservFactory):
        self.distservFactory = distservFactory

    def buildProtocol(self, addr):
        return QueryServer(self.distservFactory)

from twisted.web.server import Site,NOT_DONE_YET
from twisted.web.resource import Resource
import time

class WebServer(Resource):
    isLeaf = True
    request = None

    def __init__(self, distservFactory):
        self.distservFactory = distservFactory

    def render_GET(self, request):
        self.request = request
        appName = request.path.strip("/")
        if "param" in request.args and "callback" in request.args:
            if appName == 'status':
                clients = len(self.distservFactory.clients)
                busy = len([1 for i in self.distservFactory.clients if self.distservFactory.clients[i].isBusy])
                waiting = len(self.distservFactory.waitingPool)
                self.doResponse("{\"clients\": %d, \"busy\":%d, \"waiting\":%d}" % (clients, busy, waiting))
                return NOT_DONE_YET
            self.distservFactory.query("%s\n%s" % (appName, request.args["param"][0]), self)
            return NOT_DONE_YET
        else:
            return "[Error] Unknown JSONP Format!"

    def doResponse(self, data):
        if data.startswith("{"):
            self.request.write("%s(%s)" % (self.request.args["callback"][0], data))
        else:
            self.request.write(data)
        self.request.finish()

if __name__ == '__main__':
    print "Distserv Server Started:", VERSION
    distservFactory = DistservFactory()

    queryServerFactory = QueryServerFactory(distservFactory)

    webFactory = Site(WebServer(distservFactory))

    reactor.listenTCP(8123, distservFactory)
    reactor.listenTCP(8124, queryServerFactory)
    reactor.listenTCP(8125, webFactory)
    reactor.run()