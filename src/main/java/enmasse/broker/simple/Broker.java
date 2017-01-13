package enmasse.broker.simple;

import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.BaseHandler;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.apache.qpid.proton.reactor.FlowController;
import org.apache.qpid.proton.reactor.Handshaker;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class Broker extends BaseHandler {
    private final String routerHost;
    private final int routerPort;
    private final String containerId;
    private final Queue queue;
    private int nextTag = 0;
    private final Set<byte[]> unsettled = new ConcurrentSkipListSet<>();

    public Broker(String containerId, String routerHost, int routerPort, Queue queue) {
        this.containerId = containerId;
        this.routerHost = routerHost;
        this.routerPort = routerPort;
        this.queue = queue;
        add(new Handshaker());
        add(new FlowController(2));
    }

    @Override
    public void onReactorInit(Event e) {
        e.getReactor().connectionToHost(routerHost, routerPort, this);
    }

    @Override
    public void onConnectionInit(Event e) {
        e.getConnection().setContainer(containerId);
        e.getConnection().open();
    }

    @Override
    public void onLinkRemoteOpen(Event e) {
        if (e.getReceiver() != null) {
            startReceiver(e.getReceiver());
        } else if (e.getSender() != null) {
            startSender(e.getSender());
        }
    }

    @Override
    public void onDelivery(Event e) {
        Delivery delivery = e.getDelivery();
        if (delivery != null && unsettled.remove(delivery.getTag())) {
            delivery.settle();
        } else {
            Receiver receiver = e.getReceiver();
            delivery = receiver.current();
            if (delivery != null && delivery.isReadable() && !delivery.isPartial()) {
                int size = delivery.pending();
                byte[] recvBuffer = new byte[size];
                receiver.recv(recvBuffer, 0, recvBuffer.length);
                receiver.advance();

                Message message = Message.Factory.create();
                message.decode(recvBuffer, 0, recvBuffer.length);
                boolean ok = queue.enqueue(message.getBody());
                delivery.settle();
                if (ok) {
                    delivery.disposition(new Accepted());
                } else {
                    delivery.disposition(new Rejected());
                }
            }
        }
    }

    @Override
    public void onLinkFlow(Event e) {
        org.apache.qpid.proton.engine.Sender snd = e.getSender();
        if (snd.getCredit() > 0) {
            Section section = queue.dequeue();
            if (section == null) {
                System.err.println("Not messages when onLinkFlow");
                return;
            }
            Message message = Message.Factory.create();
            message.setBody(section);
            message.setAddress(queue.getAddress());

            byte[] tag = String.valueOf(nextTag++).getBytes(Charset.forName("UTF-8"));
            Delivery dlv = snd.delivery(tag);
            unsettled.add(dlv.getTag());

            byte[] encodedMessage = new byte[1024];
            MessageImpl msg = (MessageImpl) message;
            int len = msg.encode2(encodedMessage, 0, encodedMessage.length);

            if (len > encodedMessage.length) {
                encodedMessage = new byte[len];
                msg.encode(encodedMessage, 0, len);
            }
            snd.send(encodedMessage, 0, len);
            snd.advance();
        }
    }


    private void startReceiver(org.apache.qpid.proton.engine.Receiver receiver) {
        Target target = new Target();
        target.setAddress(queue.getAddress());
        receiver.setTarget(target);
        receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        receiver.setSenderSettleMode(SenderSettleMode.MIXED);
        receiver.flow(1);
    }

    private void startSender(org.apache.qpid.proton.engine.Sender sender) {
        Source source = new Source();
        source.setAddress(queue.getAddress());
        sender.setSenderSettleMode(SenderSettleMode.MIXED);
        sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        sender.setSource(source);
    }

}
