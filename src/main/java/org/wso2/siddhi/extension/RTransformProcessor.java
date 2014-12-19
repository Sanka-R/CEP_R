package org.wso2.siddhi.extension;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPWrapper;
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
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;



@SiddhiExtension(namespace = "R", function = "runScript")
public class RTransformProcessor extends TransformProcessor {

//	String script;
	boolean time = true;
	int eventCount;
	long lastRun;
	long duration;
	
	List<Attribute> eventAttributes = new ArrayList<Attribute>();
	List<InEvent> eventList = new LinkedList<InEvent>();
	
	REXP outputs;
	REXP script;
	REXP env;
	
	static JRIEngine re;
	static Logger log = Logger.getLogger("RTransformProcessor");

	static {
		try {
			re = new JRIEngine();
		} catch (REngineException e) {
			log.info(e.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see org.wso2.siddhi.core.query.processor.transform.TransformProcessor#processEvent(org.wso2.siddhi.core.event.in.InEvent)
	 */
	@Override
	protected InStream processEvent(InEvent inEvent) {
		eventList.add(inEvent);
		boolean run = false;
		if (time) {
			if (System.currentTimeMillis() >= lastRun + duration) {
				run = true;
				lastRun = System.currentTimeMillis();
			}
		} else {
			if (eventList.size()==eventCount) {
				run = true;
			}
		}

		if (run) {			
			try {
				Type type;
				Object[] eventData= new Object[eventAttributes.size()];
				for (int j = 0; j < eventAttributes.size(); j++) {
					type = eventAttributes.get(j).getType();
					switch(type) {
						case DOUBLE:
						case FLOAT:
							eventData[j] = new Double[eventList.size()];
							break;
						case INT:
							eventData[j] = new Integer[eventList.size()];
							break;
						case LONG:
							eventData[j] = new Long[eventList.size()];
							break;
						case BOOL:
							eventData[j] = new Boolean[eventList.size()];
							break;
						case STRING:
							eventData[j] = new String[eventList.size()];
							break;
						default:
							eventData[j] = new Object[eventList.size()];
					}
				}
				int index=0;
				for(Event event:eventList) {
					for (int j = 0; j < eventAttributes.size(); j++) {
						((Object[])eventData[j])[index] = event.getData(j);
					}
					index++;
				}
				for (int j = 0; j < eventAttributes.size(); j++) {
					re.assign(eventAttributes.get(j).getName(), REXPWrapper.wrap(eventData[j]), env);
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
				return new InEvent(inEvent.getStreamId(),
						System.currentTimeMillis(), data);
			} catch (REXPMismatchException e) {
				log.info(e.getMessage());
			} catch (REngineException e) {
				log.info(e.getMessage());
			}
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

		String scriptString = ((StringConstant) expressions[0]).getValue();
		String temp = ((StringConstant) expressions[1]).getValue().trim();
		String outputString = ((StringConstant) expressions[2]).getValue();
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
		} else {
			eventCount = Integer.parseInt(temp);
			time = false;
		}

		StreamDefinition streamDef = new StreamDefinition()
				.name("ROutputStream");
		String[] vars = outputString.split(",");
		for (String var : vars) {
			streamDef = streamDef.attribute(var, Attribute.Type.DOUBLE);
		}
		this.outStreamDefinition = streamDef;
		List<Attribute> attributeList = inStreamDefinition.getAttributeList();
		for(Attribute attr : attributeList) {
			if (attr.getType() == Attribute.Type.DOUBLE 
					|| attr.getType() == Attribute.Type.LONG
					|| attr.getType() == Attribute.Type.INT
					|| attr.getType() == Attribute.Type.FLOAT) {
				eventAttributes.add(attr);
			}
		}
		
		
		outputString = "c(" + outputString + ")";
		//outputString a = new StringBuilder("c(").append(outputString).append(")").toString();
		
		try {
			// Create a new R environment 
			env = re.newEnvironment(null, true);
			// Parse the expression
			outputs = re.parse(outputString, false);
			// Parse the script
			script = re.parse(scriptString, false);
		} catch (REXPMismatchException e) {
			log.info(e.getMessage());
		} catch (REngineException e) {
			log.info(e.getMessage());
		}
	}

	@Override
	public void destroy() {
	}
}