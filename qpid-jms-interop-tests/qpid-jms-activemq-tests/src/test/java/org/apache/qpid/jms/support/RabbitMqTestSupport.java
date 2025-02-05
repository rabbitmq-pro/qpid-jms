package org.apache.qpid.jms.support;

import jakarta.jms.*;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class RabbitMqTestSupport {

  public static final String MESSAGE_NUMBER = "MessageNumber";
  protected static final Logger LOG = LoggerFactory.getLogger(RabbitMqTestSupport.class);

  protected Connection connection;
  protected final Vector<Throwable> exceptions = new Vector<Throwable>();
  protected int numberOfMessages;
  protected String testMethodName;
  protected javax.jms.Connection jmsConnection;

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    this.testMethodName = testInfo.getTestMethod().get().getName();
    LOG.info("========== setUp " + getTestName() + " ==========");
    exceptions.clear();
    // TODO we may have to reset the broker here (delete/create queues, etc)
    this.numberOfMessages = 2000;
  }

  @AfterEach
  public void tearDown() throws Exception {
    LOG.info("========== tearDown " + getTestName() + " ==========");
    Exception firstError = null;

    if (connection != null) {
      try {
        connection.close();
      } catch (Exception e) {
        LOG.warn("Error detected on connection close in tearDown: {}", e.getMessage());
        firstError = e;
      }
    }

    if (jmsConnection != null) {
      try {
        jmsConnection.close();
      } catch (Exception e) {
        LOG.warn("Error detected on jms connection close in tearDown: {}", e.getMessage());
        firstError = e;
      }
    }

    // TODO deal with multiple brokers
//    for (BrokerService broker : brokers) {
//      try {
//        stopBroker(broker);
//      } catch (Exception ex) {
//        LOG.warn("Error detected on close of broker in tearDown: {}", ex.getMessage());
//        firstError = ex;
//      }
//    }

    if (firstError != null) {
      throw firstError;
    }
  }

  public URI getBrokerAmqpConnectionURI() {
    try {
      return new URI("amqp://localhost:5672");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String getAmqpFailoverURI() throws Exception {
    // TODO check where failover URI is used
    return null;
  }

  // TODO check the tests that uses this method
  public String getAmqpConnectionURIOptions() {
    return "";
  }

  // TODO check the tests that uses this method
  protected boolean isFrameTracingEnabled() {
    return false;
  }

  public Connection createAmqpConnection() throws Exception {
    return createAmqpConnection(getBrokerAmqpConnectionURI());
  }

  public Connection createAmqpConnection(String username, String password) throws Exception {
    return createAmqpConnection(getBrokerAmqpConnectionURI(), username, password);
  }

  public Connection createAmqpConnection(URI brokerURI) throws Exception {
    return createAmqpConnection(brokerURI, null, null);
  }

  public Connection createAmqpConnection(URI brokerURI, String username, String password) throws Exception {
    ConnectionFactory factory = createAmqpConnectionFactory(brokerURI, username, password);
    return factory.createConnection();
  }

  public ConnectionFactory createAmqpConnectionFactory() throws Exception {
    return createAmqpConnectionFactory(getBrokerAmqpConnectionURI(), null, null);
  }

  public ConnectionFactory createAmqpConnectionFactory(URI brokerURI) throws Exception {
    return createAmqpConnectionFactory(brokerURI, null, null);
  }

  public ConnectionFactory createAmqpConnectionFactory(String username, String password) throws Exception {
    return createAmqpConnectionFactory(getBrokerAmqpConnectionURI(), username, password);
  }

  public ConnectionFactory createAmqpConnectionFactory(URI brokerURI, String username, String password) throws Exception {
    JmsConnectionFactory factory = new JmsConnectionFactory(brokerURI);
    factory.setForceAsyncSend(isForceAsyncSends());
    factory.setForceSyncSend(isForceSyncSends());
    factory.setForceAsyncAcks(isForceAsyncAcks());
    if (username != null) {
      factory.setUsername(username);
    }
    if (password != null) {
      factory.setPassword(password);
    }
    return factory;
  }

  protected boolean isForceAsyncSends() {
    return false;
  }

  protected boolean isForceSyncSends() {
    return false;
  }

  protected boolean isForceAsyncAcks() {
    return false;
  }

  protected String getTestName() {
    return getClass().getSimpleName() + "." + testMethodName;
  }

  public String getDestinationName() {
    return testMethodName;
  }

  public void startPrimaryBroker() throws Exception {
    RabbitMqCli.startBroker();
  }

  public void restartPrimaryBroker() throws Exception {
    // TODO restart broker
  }

  public void stopPrimaryBroker() throws Exception {
    RabbitMqCli.stopBroker();
  }

  protected void sendToAmqQueue(int count) throws Exception {
    // TODO send to AMQ queue
    javax.jms.Connection activemqConnection = null;
    javax.jms.Session amqSession = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    javax.jms.Queue amqTestQueue = amqSession.createQueue(testMethodName);
    sendMessages(activemqConnection, amqTestQueue, count);
    activemqConnection.close();
  }

  protected void sendToAmqTopic(int count) throws Exception {
    // TODO send to AMQ topic
    javax.jms.Connection activemqConnection = null;
    javax.jms.Session amqSession = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    javax.jms.Topic amqTestTopic = amqSession.createTopic(testMethodName);
    sendMessages(activemqConnection, amqTestTopic, count);
    activemqConnection.close();
  }

  public void sendMessages(javax.jms.Connection connection, javax.jms.Destination destination, int count) throws Exception {
    // TODO send messages
    javax.jms.Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    javax.jms.MessageProducer p = session.createProducer(destination);

    for (int i = 1; i <= count; i++) {
      javax.jms.TextMessage message = session.createTextMessage();
      message.setText("TextMessage: " + i);
      message.setIntProperty(MESSAGE_NUMBER, i);
      p.send(message);
    }

    session.close();
  }

  public void sendMessages(Connection connection, Destination destination, int count) throws Exception {
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageProducer p = session.createProducer(destination);

    for (int i = 1; i <= count; i++) {
      TextMessage message = session.createTextMessage();
      message.setText("TextMessage: " + i);
      message.setIntProperty(MESSAGE_NUMBER, i);
      p.send(message);
    }

    session.close();
  }

  protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
    // TODO support retrieving queue information
    return null;
  }

  protected QueueViewMBean getProxyToTemporaryQueue(String name) throws MalformedObjectNameException, JMSException {
    // TODO support retrieving queue information
    return null;
  }

  protected BrokerViewMBean getProxyToBroker() throws MalformedObjectNameException, JMSException {
    // TODO support retrieving queue information
    return null;
  }

  protected TopicViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
    // TODO support retrieving topic information
    return null;
  }

  // TODO check the tests that uses this method to configure broker policies
  protected void configureBrokerPolicies(BrokerService broker) {

  }

  protected boolean isPersistent() {
    return false;
  }

  public List<URI> getBrokerURIs() throws Exception {
    // TODO implement getBrokerURIs
    ArrayList<URI> result = new ArrayList<>();
    return result;
  }

  public void startNewBroker() throws Exception {
    // TODO implement startNewBroker
  }

  protected boolean isAmqpDiscovery() {
    return false;
  }

  protected String getDiscoveryNetworkInterface() {
    return null;
  }

  public javax.jms.Connection createActiveMQConnection() throws Exception {
    // TODO create connection to send test messages
    return null;
//    return createActiveMQConnection(getBrokerActiveMQClientConnectionURI());
  }

  protected String getAmqpTransformer() {
    return "jms";
  }

  protected int getSocketBufferSize() {
    return 64 * 1024;
  }

  protected int getIOBufferSize() {
    return 8 * 1024;
  }

  protected boolean isAddOpenWireConnector() {
    return false;
  }

  protected String adminUsername() {
    return "guest";
  }

  protected String adminPassword() {
    return "guest";
  }
}
