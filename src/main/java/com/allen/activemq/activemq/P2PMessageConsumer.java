package com.allen.activemq.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class P2PMessageConsumer {

	protected String username = ActiveMQConnection.DEFAULT_USER;
	
	protected String password = ActiveMQConnection.DEFAULT_PASSWORD;
	
	protected String brokerURL = "tcp://127.0.0.1:61616";
	
	protected static transient ConnectionFactory factory;  
	
    protected transient Connection connection; 
    
    
	public static void main(String[] agrs){
		P2PMessageConsumer consumer = new P2PMessageConsumer();
		consumer.receiveMessage();
	}
	
	public P2PMessageConsumer(){
		try{
			factory = new ActiveMQConnectionFactory(username,password,brokerURL);
			connection = factory.createConnection();
			connection.start();
		}catch(JMSException e){
			close();
		}
	}
	
	public P2PMessageConsumer(String username,String password,String brokerURL) throws JMSException{
		this.username = username;
		this.password = password;
		this.brokerURL = brokerURL;
		
		factory = new ActiveMQConnectionFactory(username,password,brokerURL);
		connection = factory.createConnection();
		try{
			connection.start();
		}catch(JMSException e){
			connection.close();
			throw e;
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
	
	protected void receiveMessage(){
		Session session = null;
		try{
			session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue("MessageQueue");
			MessageConsumer consumer = session.createConsumer(destination);
			while(true){
				Message message = consumer.receive();
				if(null != message){
					if(message instanceof ObjectMessage){
						System.out.println("deal ObjectMessage....");
						dealObjectMessage((ObjectMessage) message);
					}else if(message instanceof MapMessage){
						System.out.println("deal MapMessage....");
						dealMapMessage((MapMessage) message);
					}else if(message instanceof TextMessage){
						System.out.println("deal TextMessage....");
						dealTextMessage((TextMessage) message);
					}
				}else{
					break;
				}
			}
		}catch(JMSException e){
			e.printStackTrace();
		}finally{
			if(session !=null){
				try{
					session.commit();
				}catch(JMSException e){
					e.printStackTrace();
				}
			}
		}
	}
	
	private void dealTextMessage(TextMessage message) throws JMSException{
		String text = message.getText();
		System.out.println("text = " + text);
	}
	
	private void dealMapMessage(MapMessage message) throws JMSException{
		String stock = message.getString("stock");
		boolean b  = message.getBoolean("boolean");
		double d = message.getDouble("price");
		int i = message.getInt("Int");
		byte[] bs = message.getBytes("bytes");
		
		System.out.println(stock);
		System.out.println(b);
		System.out.println(d);
		System.out.println(i);
		System.out.println(new String(bs));
	}
	
	private void dealObjectMessage(ObjectMessage message) throws JMSException {  
		  
        User user = (User) message.getObject();  
        System.out.println(user.toString());  
  
    }  
	
}
