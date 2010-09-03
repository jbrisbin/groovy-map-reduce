package com.jbrisbin.vpc.mapred;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import groovy.util.GroovyScriptEngine;
import groovy.util.ResourceException;
import groovy.util.ScriptException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class GroovyMessageConvertor implements MessageConverter {

  private Logger log = LoggerFactory.getLogger( getClass() );
  @Autowired
  private GroovyScriptEngine groovyScriptEngine;
  @Autowired
  private GroovyShell groovyShell;
  private ObjectMapper mapper = new ObjectMapper();
  private ConcurrentSkipListMap<String, Script> scriptCache = new ConcurrentSkipListMap<String, Script>();

  @Override
  public Message toMessage( Object object,
                            MessageProperties messageProperties ) throws MessageConversionException {
    if ( object instanceof Script ) {
      Script script = (Script) object;
      String contentType = messageProperties.getContentType();
      if ( contentType.equals( "text/groovy" ) ) {
        return new Message( script.getProperty( "src" ).toString().getBytes(),
            messageProperties );
      } else if ( contentType.equals( "application/json" ) ) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
          mapper.writeValue( out, object );
        } catch ( IOException e ) {
          log.error( e.getMessage(), e );
        }
        return new Message( out.toByteArray(), messageProperties );
      }
    } else {
      throw new MessageConversionException( "Can only convert Groovy script objects." );
    }
    return null;
  }

  @Override
  public Object fromMessage( Message message ) throws MessageConversionException {
    Script script = null;
    MessageProperties props = message.getMessageProperties();
    if ( null == props.getCorrelationId() ) {
      throw new MessageConversionException( "Cannot use map/reduce without a correlationId." );
    }
    Binding binding = new Binding();
    if ( message.getMessageProperties().getContentType().equals( "text/groovy" ) ) {
      binding.setVariable( "log", LoggerFactory.getLogger( "mapred" ) );
      String src = new String( message.getBody() );
      String md5sum = null;
      try {
        MessageDigest md5 = MessageDigest.getInstance( "MD5" );
        md5.update( message.getBody() );
        md5sum = new BigInteger( 1, md5.digest() ).toString( 16 );
      } catch ( NoSuchAlgorithmException e ) {
        log.error( e.getMessage(), e );
      }

      if ( null != md5sum ) {
        script = scriptCache.get( md5sum );
        if ( null == script ) {
          script = compile( src );
          script.setProperty( "src", src );
          scriptCache.put( md5sum, script );
        }
      } else {
        script = compile( src );
      }
      if ( null != script ) {
        binding.setVariable( "correlationId", new String( props.getCorrelationId() ) );
        Object reduceTo = props.getHeaders().get( "reduceTo" );
        if ( null != reduceTo ) {
          script.setProperty( "reduceTo", reduceTo );
        }
        Object reduceAfter = props.getHeaders().get( "reduceAfter" );
        if ( null != reduceAfter ) {
          script.setProperty( "reduceAfter", reduceAfter );
        }
        script.setBinding( binding );
      }
    } else if ( message.getMessageProperties().getContentType().equals( "application/json" ) ) {
      Map<String, Object> headers = props.getHeaders();
      if ( headers.containsKey( "script" ) ) {
        String scriptPath = headers.get( "script" ).toString();
        binding.setVariable( "log",
            LoggerFactory.getLogger( scriptPath.replaceAll( "\\.(.*)$", "" ) ) );
        try {
          script = groovyScriptEngine.createScript( scriptPath, binding );
          binding.setVariable( "correlationId", new String( props.getCorrelationId() ) );
          script.setProperty( "script", scriptPath );
          script.setProperty( "replyTo", props.getReplyTo() );
          Object reduceTo = props.getHeaders().get( "reduceTo" );
          if ( null != reduceTo ) {
            script.setProperty( "reduceTo", reduceTo );
          }
          Object reduceAfter = props.getHeaders().get( "reduceAfter" );
          if ( null != reduceAfter ) {
            script.setProperty( "reduceAfter", reduceAfter );
          }
          binding.setVariable( "data",
              mapper.readValue( new String( message.getBody() ), List.class ) );
        } catch ( ResourceException e ) {
          throw new MessageConversionException( e.getMessage(), e );
        } catch ( IOException e ) {
          throw new MessageConversionException( e.getMessage(), e );
        } catch ( ScriptException e ) {
          throw new MessageConversionException( e.getMessage(), e );
        }
      }
    }

    return script;
  }

  private Script compile( String src ) {
    return groovyShell.parse( src );
  }
}
