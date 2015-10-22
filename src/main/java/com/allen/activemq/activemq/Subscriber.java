package com.allen.activemq.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Subscriber {

	protected String username = ActiveMQConnection.DEFAULT_USER;
	protected String password = ActiveMQConnection.DEFAULT_PASSWORD;
	protected String brokerURL = "tcp://127.0.0.1:61616";

	protected static transient ConnectionFactory factory;
	protected transient Connection connection;

	public static void main(String[] args) {

		Subscriber subscriber = new Subscriber();
		subscriber.receiveMessage();
	}

	public Subscriber() {
		try {
			factory = new ActiveMQConnectionFactory(username, password, brokerURL);
			connection = factory.createConnection();
			connection.start();
		} catch (JMSException e) {
			close();
		}
	}

	public Subscriber(String username, String password, String brokerURL) throws JMSException {
		this.username = username;
		this.password = password;
		this.brokerURL = brokerURL;

		factory = new ActiveMQConnectionFactory(username, password, brokerURL);

		try {
			connection = factory.createConnection();
			connection.start();
		} catch (JMSException jmse) {
			connection.close();
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

	protected void receiveMessage() {
		Session session = null;
		try {
			session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
			Topic topic = session.createTopic("MessageTopic");
			MessageConsumer consumer = session.createConsumer(topic);
			consumer.setMessageListener(new MessageListener() {

				public void onMessage(Message message) {
					// TODO Auto-generated method stub
					if (message instanceof ObjectMessage) {
						System.out.println("deal ObjectMessage....");
						dealObjectMessage((ObjectMessage) message);
					} else if (message instanceof MapMessage) {
						System.out.println("deal MapMessage....");
						dealMapMessage((MapMessage) message);
					} else if (message instanceof TextMessage) {
						System.out.println("deal TextMessage....");
						dealTextMessage((TextMessage) message);
					}
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (session != null) {
				try {
					session.commit();
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 
	 * 处理 TextMessage消息
	 * 
	 * @throws JMSException
	 */
	private void dealTextMessage(TextMessage message) {
		try {
			String text = message.getText();
			System.out.println("text = " + text);
		} catch (JMSException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 
	 * 处理 MapMessage消息
	 * 
	 * @throws JMSException
	 */
	private void dealMapMessage(MapMessage message) {
		try {
			String stack = message.getString("stock");
			Double price = message.getDouble("price");
			System.out.println("stock = " + stack + " , price =" + price);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 处理ObjectMessage消息
	 */
	private void dealObjectMessage(ObjectMessage message) {

		try {
			User user = (User) message.getObject();
			System.out.println(user.toString());
		} catch (JMSException e) {
			e.printStackTrace();
		}

	}
}
