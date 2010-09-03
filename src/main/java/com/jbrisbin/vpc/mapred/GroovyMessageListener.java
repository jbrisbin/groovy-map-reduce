package com.jbrisbin.vpc.mapred;

import groovy.lang.*;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitMessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class GroovyMessageListener implements ApplicationContextAware {

  private Logger log = LoggerFactory.getLogger( getClass() );

  private ObjectMapper mapper = new ObjectMapper();
  private ConcurrentSkipListMap<String, ReduceResults> reduceResults = new ConcurrentSkipListMap<String, ReduceResults>();
  private Timer timer = new Timer();
  private ApplicationContext applicationContext;

  @Autowired
  private SingleConnectionFactory mqConnFactory;
  @Autowired
  private RabbitAdmin rabbitAdmin;
  @Autowired
  private TopicExchange reduceExchange;

  private String type = "map";

  @Override
  public void setApplicationContext( ApplicationContext ctx ) throws BeansException {
    this.applicationContext = ctx;
  }

  public String getType() {
    return type;
  }

  public void setType( String type ) {
    this.type = type;
  }

  public void handleMessage( final Script script ) throws Exception {
    //log.debug( "evaluating groovy: " + script.toString() );
    if ( null == script ) {
      throw new MessageConversionException( "Script was null." );
    }
    script.run();
    String correlationId = script.getBinding().getVariable( "correlationId" ).toString();
    String reduceTo = null;
    int reduceAfter = 0;
    int flushAfter = 500;
    try {
      reduceTo = script.getProperty( "reduceTo" ).toString();
    } catch ( Exception e ) {
      // INGORED, no reduce
    }
    try {
      reduceAfter = (Integer) script.getProperty( "reduceAfter" );
    } catch ( Exception e ) {
      // INGORED, no reduce limit
    }
    try {
      flushAfter = (Integer) script.getProperty( "flushAfter" );
    } catch ( Exception e ) {
      // INGORED
    }

    Closure closure = null;
    try {
      closure = (Closure) script.getProperty( type );
      Object data = script.getBinding().getVariables().remove( "data" );
      Object result = null;
      if ( null != data ) {
        result = closure.call( new Object[]{data} );
      } else {
        result = closure.call();
      }
      if ( type.equals( "map" ) ) {
        if ( null != result && null != reduceTo ) {
          ReduceResults l = reduceResults.get( correlationId );
          if ( null == l ) {
            l = new ReduceResults( (Closure) script.getProperty( "reduce" ),
                script.getProperty( "script" ).toString(), correlationId, reduceTo,
                reduceAfter, script.getProperty( "replyTo" ).toString(), flushAfter );
            reduceResults.put( correlationId, l );
          }
          l.addResult( result );
        }
      } else {
        RabbitTemplate tmpl = applicationContext
            .getBean( "replyTemplate", RabbitTemplate.class );
        tmpl.send( script.getProperty( "replyTo" ).toString(),
            new ReplyMessageCreator( correlationId, (null != result ? result : "") ) );
      }
    } catch ( MissingPropertyException ignored ) {
      // This script doesn't have one of these
    }
    //log.debug( "run script..." );
  }

  private class ReplyMessageCreator implements MessageCreator {

    private String id;
    private byte[] data;
    private RabbitMessageProperties props = new RabbitMessageProperties();

    private ReplyMessageCreator( String id, Object obj ) throws IOException {
      this.id = id;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      mapper.writeValue( out, obj );
      data = out.toByteArray();
      props.setContentType( "application/json" );
      props.setCorrelationId( id.getBytes() );
    }

    @Override
    public Message createMessage() {
      return new Message( data, props );
    }
  }

  private class ScriptPathMessageCreator implements MessageCreator {

    private String id;
    private String scriptPath;
    private byte[] data;
    private RabbitMessageProperties props = new RabbitMessageProperties();

    ScriptPathMessageCreator( String correlationId, String scriptPath, String replyTo,
                              Object obj ) throws IOException {
      this.id = correlationId;
      this.scriptPath = scriptPath;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      mapper.writeValue( out, obj );
      this.data = out.toByteArray();
      props.getHeaders().put( "script", scriptPath );
      props.setReplyTo( new Address( replyTo ) );
      props.setContentType( "application/json" );
      props.setCorrelationId( id.getBytes() );
    }

    @Override
    public Message createMessage() {
      return new Message( data, props );
    }
  }

  private class ReduceResults {

    String scriptPath;
    String id;
    String reduceTo;
    int reduceAfter = 0;
    String replyTo;
    SimpleMessageListenerContainer reduceContainer;
    List<Object> partialResults = new ArrayList<Object>();
    Closure reduce;
    TimerTask flush;
    int flushAfter;

    ReduceResults( Closure cl, String scriptPath, String correlationId, String reduceTo,
                   int reduceAfter, String replyTo, int flushAfter ) {
      this.reduce = cl;
      this.scriptPath = scriptPath;
      this.id = correlationId;
      this.reduceTo = reduceTo;
      this.reduceAfter = reduceAfter;
      this.replyTo = replyTo;
      this.flushAfter = flushAfter;
      if ( null != reduceTo ) {
        Queue reduceToQ = new Queue( reduceTo );
        reduceToQ.setAutoDelete( false );
        //reduceToQ.setExclusive( true );
        rabbitAdmin.declareQueue( reduceToQ );
        rabbitAdmin.declareBinding( new Binding( reduceToQ, reduceExchange, reduceTo ) );
        reduceContainer = applicationContext
            .getBean( "reduceListenerContainer", SimpleMessageListenerContainer.class );
        reduceContainer.setQueues( reduceToQ );
        reduceContainer.start();
      }
    }

    void addResult( Object obj ) {
      partialResults.add( obj );
      if ( null != flush ) {
        flush.cancel();
      }
      if ( reduceAfter > 0 && partialResults.size() > reduceAfter ) {
        // Over limit, reduce
        reduce();
      }
      flush = new TimerTask() {
        @Override
        public void run() {
          // Flush results
          reduce();
          try {
            reduceResults.remove( id );
          } catch ( NullPointerException npe ) {
            // IGNORED
          }
        }
      };
      timer.schedule( flush, flushAfter );
    }

    void reduce() {
      if ( null != reduceTo ) {
        // Reduce result
        RabbitTemplate tmpl = applicationContext
            .getBean( "reduceTemplate", RabbitTemplate.class );
        try {
          synchronized (partialResults) {
            tmpl.send( reduceTo,
                new ScriptPathMessageCreator( id, scriptPath, replyTo, partialResults ) );
            partialResults.clear();
          }
        } catch ( Exception e ) {
          log.error( e.getMessage(), e );
        }
      } else {
        throw new IllegalStateException( "Cannot call reduce() when reduceTo is null." );
      }
    }

    void stop() {
      reduceContainer.stop();
    }

  }

}
