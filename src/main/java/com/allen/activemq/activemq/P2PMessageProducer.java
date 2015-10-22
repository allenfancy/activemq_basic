package com.allen.activemq.activemq;

import java.io.Serializable;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;


public class P2PMessageProducer {

	protected String username = ActiveMQConnection.DEFAULT_USER;
	protected String password = ActiveMQConnection.DEFAULT_PASSWORD;
	
	protected String brokerURL = "tcp://127.0.0.1:61616";
	
	protected static transient ConnectionFactory factory;
	protected transient javax.jms.Connection connection;
	
	public static void main(String[] args){
		try{
			new P2PMessageProducer().sendObjectMessage(new User("allen","wt6815876"));
			new P2PMessageProducer().sendMapSession();
			new P2PMessageProducer().sendTextMessage("您好，我是allen");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public P2PMessageProducer(){
		try{
			factory = new ActiveMQConnectionFactory(username,password,brokerURL);
			connection  = factory.createConnection();
			connection.start();
		}catch(JMSException e){
			e.printStackTrace();
		}
	}
	
	public P2PMessageProducer(String username,String password,String brokerURL) throws JMSException{
		this.username  = username;
		this.password = password;
		this.brokerURL = brokerURL;
		
		factory = new ActiveMQConnectionFactory(username,password,brokerURL);
		try{
			connection = factory.createConnection();
			connection.start();
		}catch(JMSException jmse){
			connection.close();
			throw jmse;
		}
	}
	
	public void close(){
		try{
			if(connection != null){
				connection.close();
			}
		}catch(JMSException e){
			e.printStackTrace();
		}
	}
	
	protected void sendObjectMessage(Serializable serializable) throws JMSException{
		Session session = null;
		try{
			session = connection.createSession(Boolean.TRUE,Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue("MessageQueue");
			MessageProducer producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			Message message = session.createObjectMessage(serializable);
			producer.send(message);
			session.commit();
		}catch(JMSException e){
			try{
				session.rollback();
			}catch(JMSException e1){
				e1.printStackTrace();
			}
		}finally{
			close();
		}
	}
	
	
	protected void sendTextMessage(String text) throws JMSException{
		Session session = null;
		try{
			session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue("MessageQueue");
			MessageProducer producer = session.createProducer(destination);
			Message message = session.createTextMessage(text);
			producer.send(message);
			session.commit();
		}catch(JMSException e){
			session.rollback();
		}finally{
			close();
		}
	}
	
	protected void sendMapSession() throws JMSException{
		Session session = null;
		try{
			session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue("MessageQueue");
			MessageProducer producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			MapMessage message = session.createMapMessage();
			message.setString("stock","String");
			message.setBoolean("boolean", false);
			message.setDouble("price", 11.32);
			message.setInt("Int",12);
			message.setBytes("bytes",new byte[]{123,32,43});
			producer.send(message);
			
			session.commit();
		}catch(JMSException e){
			try{
				session.rollback();
			}catch(JMSException e1){
				e1.printStackTrace();
			}
		}finally{
			close();
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
}
