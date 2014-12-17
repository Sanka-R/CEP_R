package org.test.cep.extension;

/*
* Copyright 2004,2005 The Apache Software Foundation.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.query.QueryPostProcessingElement;
import org.wso2.siddhi.core.query.processor.window.WindowProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;


@SiddhiExtension(namespace = "custom", function = "lastUnique")
public class CustomWindowExtension extends WindowProcessor {

    int eventCount;
    int currCount = 0;
    String script;
    String outPutVariables;
    Rengine re;
    
    @Override
    /**
     *This method called when processing an event
     */
    protected void processEvent(InEvent inEvent) {
        acquireLock();
        try {
            doProcessing(inEvent);
        } finally

        {
            releaseLock();
        }

    }

    @Override
    /**
     *This method called when processing an event list
     */
    protected void processEvent(InListEvent inListEvent) {

        for (int i = 0; i < inListEvent.getActiveEvents(); i++) {
            InEvent inEvent = (InEvent) inListEvent.getEvent(i);
            processEvent(inEvent);
        }
    }

    @Override
    /**
     * This method iterate through the events which are in window
     */
    public Iterator<StreamEvent> iterator() {
        return null;
    }

    @Override
    /**
     * This method iterate through the events which are in window but used in distributed processing
     */
    public Iterator<StreamEvent> iterator(String s) {
        return null;
    }

    @Override
    /**
     * This method used to return the current state of the window, Used for persistence of data
     */
    protected Object[] currentState() {
        return new Object[]{};
    }

    @Override
    /**
     * This method is used to restore from the persisted state
     */
    protected void restoreState(Object[] objects) {
    }

    @Override
    /**
     * Method called when initialising the extension
     */
    protected void init(Expression[] expressions,
                        QueryPostProcessingElement queryPostProcessingElement,
                        AbstractDefinition abstractDefinition, String s, boolean b,
                        SiddhiContext siddhiContext) {
    	
    	/*if(System.getenv("R_HOME") == null) {
    		log.error("R_HOME is not set");
    	}
    	if(System.getenv("JRI_HOME") == null) {
    		log.error("JRI_HOME is not set");
    	}
    	System.setProperty("java.library.path", System.getenv("JRI_HOME"));
    	*/
    	
        if (expressions.length != 3) {
            log.error("Parameters count is not matching, There should be three parameters ");
        }
        
        script = ((StringConstant) expressions[0]).getValue();
        eventCount = ((IntConstant) expressions[1]).getValue();
        outPutVariables = ((StringConstant) expressions[2]).getValue();
        log.info(script);
        log.info(outPutVariables);
        
        
        if (!Rengine.versionCheck()) {
    	    log.error("** Version mismatch - Java files don't match library version.");
    	}
            log.info("Creating Rengine (with arguments)");
    		// 1) we pass the arguments from the command line
    		// 2) we won't use the main loop at first, we'll start it later
    		//    (that's the "false" as second argument)
    		// 3) the callbacks are implemented by the TextConsole class above
            String arr[] = {"--no-save"};
    		re = new Rengine(arr, false, null);
            log.info("Rengine created, waiting for R");
    		// the engine creates R is a new thread, so we should wait until it's ready
            if (!re.waitForR()) {
                log.error("Cannot load R");
                return;
            }
    }

    private void doProcessing(InEvent event) {
       currCount ++;
       if(eventCount == currCount){
    	   //run the bloody script
    	   REXP x;
    	   String[] lines = script.split(";");
    	   for(String line: lines){
        	   re.eval(line,false);
        	   log.info(line);
    	   }
    	   x = re.eval("c("+outPutVariables+ ")");
    	   double[] out = x.asDoubleArray();
    	   for(double result:out){
    		   log.info(result);
    	   }
    	   currCount = 0;
       }

    }

    @Override
    public void destroy() {
    }
}
