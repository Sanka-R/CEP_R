package org.wso2.siddhi.extension;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.api.query.output.stream.OutStream;
import org.wso2.siddhi.extension.RScriptTransformProcessor;


public class RTransformTestCase {
	static final Logger log = Logger.getLogger(RTransformTestCase.class);

	private int count;
	private boolean eventArrived;
	private static SiddhiConfiguration siddhiConfiguration;

	@Before
	public void init() {
		count = 0;
		eventArrived = false;
	}
	@BeforeClass
    public static void setUp() throws Exception {
		log.info("Time Window test1");
		siddhiConfiguration = new SiddhiConfiguration();

		List<Class> extensions = new ArrayList<Class>(1);
		extensions.add(RScriptTransformProcessor.class);
		siddhiConfiguration.setSiddhiExtensions(extensions);
        
	}
	@Test
	public void testTimeWindowQuery1() throws InterruptedException {

		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition()
				.name("weather").attribute("time", Attribute.Type.LONG)
				.attribute("temp", Attribute.Type.DOUBLE));

		String script = "\"fit<-lm(temp~time); "
				+ "c <- coef(fit)[[\'(Intercept)\']]; m<-coef(fit)[[\'time\']];\"";
		/*Query query = QueryFactory.createQuery();
		query.from(QueryFactory.inputStream("InStream").transform(
				"R", "runScript", Expression.value(script),
				Expression.value("10"), Expression.value("m, c")));
		query.select(QueryFactory.outputSelector()
				.select("m", Expression.variable("m"))
				.select("c", Expression.variable("c")));
		query.insertInto("OutStream", OutStream.OutputEventsFor.ALL_EVENTS);
		*/String query = "from weather#transform.R:runScript("+script+", \"2\", \"m,c\") "+
						"select * "+
						"insert into weatherOutput";
		log.info(query);
		String queryReference = siddhiManager.addQuery(query);
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents,
					Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				if (inEvents != null) {
					count++;
				}
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("weather");
		inputHandler.send(new Object[] { 10l, 55.6d });
		inputHandler.send(new Object[] { 20l, 65.6d });
		Thread.sleep(500);
		inputHandler.send(new Object[] { 30l, 75.6d });
		Thread.sleep(6000);
		// Assert.assertEquals("In and Remove events has to be equal", 0,
		// count);
		log.info("count "+count);
		Assert.assertEquals("Event arrived", true, eventArrived);
		siddhiManager.shutdown();
	}
}
