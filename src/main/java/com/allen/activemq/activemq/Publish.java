package com.allen.activemq.activemq;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Publish {

	protected String username = ActiveMQConnection.DEFAULT_USER;
	protected String password = ActiveMQConnection.DEFAULT_PASSWORD;
	protected String brokerURL = "tcp://127.0.0.1:61616";

	protected static transient ConnectionFactory factory;
	protected transient Connection connection;

	public static void main(String[] args) {
		try {
			new Publish().sendObjectMessage(new User("吴涛", "我是Allen"));
			new Publish().sendMapMessage();
			new Publish().sendTextMessage("allen，你好");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Publish() {

		try {
			factory = new ActiveMQConnectionFactory(username, password, brokerURL);
			connection = factory.createConnection();
			connection.start();
		} catch (JMSException jmse) {
			close();
		}
	}

	public Publish(String username, String password, String brokerURL) throws JMSException {
		this.username = username;
		this.password = password;
		this.brokerURL = brokerURL;

		factory = new ActiveMQConnectionFactory(username, password, brokerURL);
		connection = factory.createConnection();
		try {
			connection.start();
		} catch (JMSException jmse) {
			connection.close();
			throw jmse;
		}
	}

	public void close() {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	protected void sendObjectMessage(Serializable serializable) throws JMSException {
		Session session = null;
		try {

			session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
			Topic topic = session.createTopic("MessageTopic");
			MessageProducer producer = session.createProducer(topic);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

			Message message = session.createObjectMessage(serializable);

			producer.send(message);

			session.commit();

		} catch (JMSException e) {
			try {
				session.rollback();
			} catch (JMSException e1) {
				e1.printStackTrace();
			}
			throw e;
		} finally {
			close();
		}

	}

	protected void sendTextMessage(String text) throws JMSException {
		Session session = null;
		try {

			session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
			Topic topic = session.createTopic("MessageTopic");
			MessageProducer producer = session.createProducer(topic);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

			Message message = session.createTextMessage(text);

			producer.send(message);
			session.commit();

		} catch (JMSException e) {
			try {
				session.rollback();
			} catch (JMSException e1) {
				e1.printStackTrace();
			}
			throw e;
		} finally {
			close();
		}

	}

	protected void sendMapMessage() throws JMSException {
		Session session = null;
		try {

			session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
			Topic topic = session.createTopic("MessageTopic");
			MessageProducer producer = session.createProducer(topic);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

			MapMessage message = session.createMapMessage();
			message.setString("stock", "string");
			message.setDouble("price", 11.14);
			producer.send(message);

			session.commit();

		} catch (JMSException e) {
			try {
				session.rollback();
			} catch (JMSException e1) {
				e1.printStackTrace();
			}
			throw e;
		} finally {
			close();
		}

	}

}
