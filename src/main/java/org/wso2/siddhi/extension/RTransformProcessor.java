package org.wso2.siddhi.extension;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPLogical;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.REngine;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.JRI.JRIEngine;
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

public abstract class RTransformProcessor extends TransformProcessor {

	boolean time = true;
	int eventCount;
	long lastRun;
	long duration;

	List<Attribute> eventAttributes;
	List<InEvent> eventList = new ArrayList<InEvent>();

	REXP outputs;
	REXP script;
	REXP env;

	static REngine re;
	static Logger log = Logger.getLogger("RTransformProcessor");

	@Override
	protected InStream processEvent(InEvent inEvent) {
		eventList.add(inEvent);
		if (time) {
			if (System.currentTimeMillis() >= lastRun + duration) {
				lastRun = System.currentTimeMillis();
				return runScript(inEvent.getStreamId());
			}
		} else {
			if (eventList.size() == eventCount) {
				return runScript(inEvent.getStreamId());
			}
		}
		return null;
	}

	private InEvent runScript(String streamId) {
		try {
			REXP eventData;
			Attribute attr;
			for (int j = 0; j < eventAttributes.size(); j++) {
				attr = eventAttributes.get(j);
				switch (attr.getType()) {
				case DOUBLE:
					eventData = doubleToREXP(eventList, j);
					break;
				case FLOAT:
					eventData = floatToREXP(eventList, j);
					break;
				case INT:
					eventData = intToREXP(eventList, j);
					break;
				case STRING:
					eventData = stringToREXP(eventList, j);
					break;
				case LONG:
					eventData = longToREXP(eventList, j);
					break;
				case BOOL:
					eventData = boolToREXP(eventList, j);
					break;
				default:
					continue;
				}
				re.assign(attr.getName(), eventData, env);
			}
			re.eval(script, env, false);
			REXP x = re.eval(outputs, env, true);

			double[] out = x.asDoubles();
			Object[] data = new Object[out.length];
			for (int i = 0; i < out.length; i++) {
				data[i] = out[i];
				log.info(out[i]);
			}

			eventList.clear();
			return new InEvent(streamId, System.currentTimeMillis(), data);
		} catch (REXPMismatchException e) {
			log.info(e.getMessage());
		} catch (REngineException e) {
			log.info(e.getMessage());
		}
		return null;

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

	protected void initialize(String scriptString, String temp,
			String outputString) {
		try {
			// Get the JRIEngine or create one
			re = JRIEngine.createEngine();
			// Create a new R environment
			env = re.newEnvironment(null, true);
		} catch (REngineException e) {
			log.info(e.getMessage());
		} catch (REXPMismatchException e) {
			log.info(e.getMessage());
		}

		log.info(scriptString);
		log.info(outputString);

		if (temp.endsWith("s")) {
			duration = Integer.parseInt(temp.substring(0, temp.length() - 1)
					.trim()) * 1000;
			lastRun = System.currentTimeMillis();
		} else if (temp.endsWith("min")) {
			duration = Integer.parseInt(temp.substring(0, temp.length() - 3)
					.trim()) * 60 * 1000;
			lastRun = System.currentTimeMillis();
		} else if (temp.endsWith("h")) {
			duration = Integer.parseInt(temp.substring(0, temp.length() - 3)
					.trim()) * 60 * 60 * 1000;
			lastRun = System.currentTimeMillis();
		} else {
			eventCount = Integer.parseInt(temp);
			time = false;
		}

		StreamDefinition streamDef = new StreamDefinition()
				.name("ROutputStream");
		String[] vars = outputString.split(",");
		for (String var : vars) {
			streamDef = streamDef.attribute(var.trim(), Attribute.Type.DOUBLE);
		}
		this.outStreamDefinition = streamDef;
		eventAttributes = inStreamDefinition.getAttributeList();

		// outputString = "c(" + outputString + ")";
		outputString = new StringBuilder("c(").append(outputString).append(")")
				.toString();

		try {
			// Parse the expression
			outputs = re.parse(outputString, false);
			// Parse the script
			script = re.parse(scriptString, false);
		} catch (REngineException e) {
			log.info(e.getMessage());
		}
	}

	@Override
	public void destroy() {
		re.close();
	}

	private REXP doubleToREXP(List<InEvent> list, int index) {
		double[] arr = new double[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = (Double) list.get(i).getData(index);
		}
		return new REXPDouble(arr);
	}

	private REXP floatToREXP(List<InEvent> list, int index) {
		double[] arr = new double[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = (Float) list.get(i).getData(index);
		}
		return new REXPDouble(arr);
	}

	private REXP intToREXP(List<InEvent> list, int index) {
		int[] arr = new int[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = (Integer) list.get(i).getData(index);
		}
		return new REXPInteger(arr);
	}

	private REXP longToREXP(List<InEvent> list, int index) {
		int[] arr = new int[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = ((Long) list.get(i).getData(index)).intValue();
		}
		return new REXPInteger(arr);
	}

	private REXP stringToREXP(List<InEvent> list, int index) {
		String[] arr = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = (String) list.get(i).getData(index);
		}
		return new REXPString(arr);
	}

	private REXP boolToREXP(List<InEvent> list, int index) {
		boolean[] arr = new boolean[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = (Boolean) list.get(i).getData(index);
		}
		return new REXPLogical(arr);
	}

}