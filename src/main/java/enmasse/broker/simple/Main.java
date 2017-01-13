package enmasse.broker.simple;


import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.reactor.Reactor;

import java.io.IOException;

class Main {
    public static void main(String args[]) throws IOException {
        String routerHost = System.getenv("MESSAGING_SERVICE_HOST");
        int routerPort = Integer.parseInt(System.getenv("MESSAGING_SERVICE_PORT"));
        String queueName = System.getenv("QUEUE_NAME");
        String containerId = System.getenv("CONTAINER_ID");
        Broker broker = new Broker(containerId, routerHost, routerPort, new Queue(queueName, 1000));

        Reactor reactor = Proton.reactor(broker);
        reactor.run();
    }
}