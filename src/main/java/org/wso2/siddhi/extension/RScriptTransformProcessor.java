package org.wso2.siddhi.extension;

import java.util.List;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

@SiddhiExtension(namespace = "R", function = "runScript")
public class RScriptTransformProcessor extends RTransformProcessor {
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
		String time = ((StringConstant) expressions[1]).getValue().trim();
		String outputString = ((StringConstant) expressions[2]).getValue();

		initialize(scriptString, time, outputString);
	}
}