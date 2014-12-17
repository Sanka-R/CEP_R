package org.wso2.siddhi.extension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

@SiddhiExtension(namespace = "R", function = "runScript")
public class RTransformProcessor extends TransformProcessor {

	String script;
	String outPutVariables;
	boolean time = true;
	int eventCount;
	int currentCount;

	long lastRun;
	long duration;
	static Rengine re;
	static Logger log = Logger.getLogger("RTransformProcessor");
	private Map<String, Integer> paramPositions = new HashMap<String, Integer>();

	// ?? 
	static {
		if (!Rengine.versionCheck()) {
			log.error("** Version mismatch - Java files don't match library version.");
		}
		log.info("Creating Rengine");
		String arr[] = { "--no-save" };
		re = new Rengine(arr, false, null);
		log.info("Rengine created, waiting for R");
		if (!re.waitForR()) {
			log.error("Cannot load R");
		}
		log.info("Successfully loaded R");
	}

	@Override
	protected InStream processEvent(InEvent inEvent) {
		boolean run = false;
		if (time) {
			if (System.currentTimeMillis() >= lastRun + duration) {
				run = true;
				lastRun = System.currentTimeMillis();
			}
		} else {
			currentCount++;
			if (currentCount == eventCount) {
				run = true;
				currentCount = 0;
			}
		}

		if (run) {
			REXP x;
			String[] lines = script.split(";");
			for (String line : lines) {
				re.eval(line, false);
				log.info(line);
			}
			x = re.eval("c(" + outPutVariables + ")");
			double[] out = x.asDoubleArray();
			Object[] data = new Object[out.length];

			for (int i = 0; i < out.length; i++) {
				data[i] = out[i] + "";
				log.info(out[i]);
			}
			currentCount = 0;
			return new InEvent(inEvent.getStreamId(),
					System.currentTimeMillis(), data);

		}
		return null; //??

	}

	@Override
	protected InStream processEvent(InListEvent inListEvent) {
		InListEvent transformedListEvent = new InListEvent();
		for (Event event : inListEvent.getEvents()) {
			if (event instanceof InEvent) {
				transformedListEvent
						.addEvent((Event) processEvent((InEvent) event));
			}
		}
		return transformedListEvent;
	}

	@Override
	protected Object[] currentState() {
		return null;
	}

	@Override
	protected void restoreState(Object[] objects) {
	}

	@Override
	protected void init(Expression[] expressions,
			List<ExpressionExecutor> expressionExecutors,
			StreamDefinition inStreamDefinition,
			StreamDefinition outStreamDefinition, String elementId,
			SiddhiContext siddhiContext) {
		if (expressions.length != 3) {
			log.error("Parameters count is not matching, There should be three parameters ");
		}

		script = ((StringConstant) expressions[0]).getValue();
		String temp = ((StringConstant) expressions[1]).getValue().trim();
		outPutVariables = ((StringConstant) expressions[2]).getValue();
		log.info(script);
		log.info(outPutVariables);

		if (temp.endsWith("s")) {
			duration = Integer.parseInt(temp.substring(0, temp.length() - 1)
					.trim()) * 1000;
			lastRun = System.currentTimeMillis();
		} else if (temp.endsWith("min")) {
			duration = Integer.parseInt(temp.substring(0, temp.length() - 3)
					.trim()) * 60 * 1000;
			lastRun = System.currentTimeMillis();
		} else {
			eventCount = Integer.parseInt(temp);
			currentCount = 0;
			time = false;
		}

		StreamDefinition streamDef = new StreamDefinition()
				.name("ROutputStream");
		String[] vars = outPutVariables.split(",");
		for (String var : vars) {
			streamDef = streamDef.attribute(var, Attribute.Type.STRING);
		}
		this.outStreamDefinition = streamDef;

	}

	@Override
	public void destroy() {
	}
}