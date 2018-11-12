package example.connection.jms;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.log4j.Logger;

public class JMSConnector {
	private static final Logger logger = Logger.getLogger(JMSConnector.class);

	public static String sendMessage(String message, String serviceUser, String password) throws Exception {
		logger.debug("Sending message to queue...");
		
		InitialContext ctx = null;
		QueueConnectionFactory connFactory = null;
		QueueConnection conn = null;
		QueueSession session = null;
		MessageProducer producer = null;
		Queue queue = null;
		
		try {
			
			
			
			// Creating context
			logger.debug("Creating context...");
			ctx = new InitialContext();
			
			// Lookup queue JMS of Connection Factory
			logger.debug("Lookup ConnectionFactory: ExampleConnectionFactory" );
			connFactory = (QueueConnectionFactory) ctx.lookup("ExampleConnectionFactory");
			
			logger.debug("Lookup Queue: mqBrokerExampleReq");
			queue = (Queue) ctx.lookup("mqBrokerExampleReq");
			
			// Creating connection
			logger.debug("Creating Connection...");
			 
            conn = connFactory.createQueueConnection (serviceUser, password); 
						
			logger.debug("Connection... Service User....: " +serviceUser);
			logger.debug("Connection... Servico Password......: " +password);
			// Creating session
			logger.debug("Creating Session...");
			session = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			
			// Creating sender
			logger.debug("Creating Sender...");
			producer = session.createProducer(queue);
			producer.setTimeToLive(60000);
			
			// Send message
			logger.debug("Sending message: " + message);
			TextMessage textMessage = session.createTextMessage(message);
			logger.debug("TextMessage .....: " + textMessage);
			logger.debug("TextMessage .....: textMessage.getJMSMessageID() .....: " + textMessage.getJMSMessageID());
			textMessage.setJMSExpiration(60000);
			producer.send(textMessage); 
			logger.debug("TextMessage .....: passed producer.send(textMessage) .....: " + textMessage.getJMSMessageID());

			// Return ID of message
			return(textMessage.getJMSMessageID());
		} catch (NamingException e) {
			logger.error(e);
			throw new Exception("It's impossible to locate queue for send the message.", e);
		} catch (JMSException e) {
			logger.error(e);
			throw new Exception("Sending message Erro.", e);
		} finally {
			logger.debug("Closing resources queue...");
			try { producer.close(); } catch (Exception e) { logger.error(e); }
			try { session.close(); } catch (Exception e) { logger.error(e); }
			try { conn.close(); } catch (Exception e) { logger.error(e); }
			connFactory = null;
			
		}
	}
	
	public static String receiveMessage(String correlationID, String serviceUser, String password) throws Exception {
		logger.debug("Receiving queue message ...");
		
		InitialContext ctx = null;
		QueueConnectionFactory connFactory = null;
		QueueConnection conn = null;
		QueueSession session = null;
		MessageConsumer consumer = null;
		Queue queue = null;
		Message message = null;
		
		try {
			// Creating context
			logger.debug("Creating context...");
			ctx = new InitialContext();

			// lookup of queue JMS of Connection Factory
			logger.debug("Lookup ConnectionFactory: ExampleConnectionFactory");
			connFactory = (QueueConnectionFactory) ctx.lookup("ExampleConnectionFactory");
			
			logger.debug("Lookup Queue: mqBrokerExampleRsp");
			queue = (Queue) ctx.lookup("mqBrokerExampleRsp");
			
			// Creating connection
			logger.debug("Creating Connection...");
			conn = connFactory.createQueueConnection (serviceUser, password);
			logger.debug("Passed Connection receiveMessage... Service User....: " +serviceUser);
			logger.debug("Passed Connection receiveMessage... Service Password......: " +password);
			
			// Creating session
			logger.debug("Creating Session...");
			session = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			
			// Creating receiver
			logger.debug("Creating Receiver: CorrelationID=" + correlationID);
			consumer = session.createConsumer(queue, "JMSCorrelationID='" + correlationID +"'");
			
			// Waiting message received
			logger.debug("Waiting message for 60000 milliseconds.");
			conn.start();
			message = consumer.receive( 60000 );
			
			
			// verify recepted message
			if(message == null) {
				throw new Exception("Ero recepting message [message=null]");
			} else {
				logger.debug("Verifing message recepted: " + message);
				logger.debug("[message=" + message.getClass() + "]");
				
				// Reading a bytes array
				if ( message instanceof BytesMessage )
				{
					return handledMessageReceived(message);


				}
				else
				{
				
				
				if(!(message instanceof TextMessage)) {
					throw new Exception("Error message received [message=" + message.getClass() + "]");
				}
				}
			}

			//Show text received in message
			return(((TextMessage)message).getText());
		} catch (NamingException e) {
			logger.error(e);
			throw new Exception("Impossible to locate queue for receive the message.", e);
		} catch (JMSException e) {
			logger.error(e);
			throw new Exception("Error message received.", e);
		} finally 
		{
			logger.debug("Closing resources of queue.");
			try { consumer.close(); } catch (Exception e) { logger.error(e); }
			try { session.close(); } catch (Exception e) { logger.error(e); }
			try { conn.close(); } catch (Exception e) { logger.error(e); }
			connFactory = null;
		}
	}

	private static String handledMessageReceived(Message message)
			throws JMSException {
		BytesMessage msgbytes;
		String text;
		msgbytes = (BytesMessage) message;

		// Create array of bytes with size of message
		byte[] charge = new byte[(int)msgbytes.getBodyLength()]; 
		// Read message
		msgbytes.readBytes(charge);
		StringBuffer sb = new StringBuffer("");
		chargeBytes(charge, sb);
		text = sb.toString();
		return text;
	}

	private static void chargeBytes(byte[] charge, StringBuffer sb) {
		for( int i=0; i<charge.length; i++)
		{
			sb.append( (char)charge[i]);
		}
	}
	
	
	
	

	
	
	
}
