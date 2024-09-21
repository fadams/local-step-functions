# Service Integrations
The ASL Workflow Engine integrates with a number of services so that you can call API actions, and coordinate executions directly from the Amazon States Language in Step Functions.

With ASL you call and pass parameters to the API of those services from a [Task](https://states-language.net/spec.html#task-state) state in the Amazon States Language.

#### Lambda/FaaS
 The ASL Workflow Engine currently has integrations with:
 
 * [AMQP](https://www.amqp.org/) based RPC Message invocations (as described here: https://www.rabbitmq.com/tutorials/tutorial-six-python.html)
 * [Oracle Fn Project](https://github.com/fnproject) (TODO)
 * [OpenFaaS](https://github.com/openfaas/faas) (TODO)
 * [AWS Lambda](https://docs.aws.amazon.com/step-functions/latest/dg/connect-lambda.html) (TODO)

Service Integrations use the value of the URI contained in the Task state “Resource” field to determine the type of the task to execute. For real AWS Step Functions the service integrations are described in: https://docs.aws.amazon.com/step-functions/latest/dg/concepts-service-integrations.html

For now the emphasis will be on executing FaaS functions, initially via AMQP 0.9.1 request/response, Oracle Fn Project and OpenFaaS and also (hopefully) real AWS Lambda.

For consistency with real AWS Step Functions the intention is to specify resource URIs as "Amazon Resource Names" as detailed in the link: https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html

Clearly for real AWS services this is essential, so for Lambda we'd use:

```
arn:aws:lambda:region:account-id:function:function-name
```

however, it also makes sense to follow this pattern even for non-AWS resources.

The initial proposal is for the following formats:

**For async messaging based (e.g. AMQP) RPC invoked functions/services:**

```
arn:aws:rpcmessage:local::function:function-name
```

In addition, this resource supports the following Parameters in the Task state in order to control the configuration of the messaging system used to transport the RPC.

```json
"Parameters": {
    "URL": "amqp://localhost:5672?connection_attempts=20&retry_delay=10&heartbeat=0",
    "Type": "AMQP-0.9.1",
    "Queue", "Worker's input queue name",
    "Message.$": "$"
}
```

If the Parameters field is omitted from the ASL then the messaging connection used to connect to the event queue shall be used and the "effective parameters" passed to execute_task shall simply be the Task state's effective input (after InputPath processing). If, however, the Parameters field is included then the "effective parameters" passed to execute_task shall be as above where Message will be set to the Task State's effective input.

In this case the Resource ARN should have the function-name omitted. This is to allow us to disambiguate between when we want to call the resource ARN directly and when we need to supplement the ARN with the Parameters.
        
**For OpenFaaS (https://www.openfaas.com) functions:**
```
arn:aws:openfaas:local::function:function-name
```

**For Fn (https://fnproject.io) functions**
```
arn:aws:fn:local::function:function-name
```

As these are all essentially Function As A Service (FaaS) approaches the "ARN" format is largely the same as for AWS Lambda except the service namespace part is rpcmessage, openfaas or fn to reflect the actual service, the region is "local" and the account-id is omitted.

If the supplied resource starts with $ the resource will be treated as an environment variable and the real ARN will be looked up from there.

#### Step Functions
The ability to trigger "child" Step Functions is a useful pattern, especially in a hybrid orchestration/choreography microservice integration model. Until recently (September 2019) the *official* Amazon AWS Step Functions implementation required the use of a Lambda intermediary to achieve this, however it now has a direct service integration, which this implementation also uses:

https://docs.aws.amazon.com/step-functions/latest/dg/connect-stepfunctions.html

https://docs.aws.amazon.com/step-functions/latest/dg/concepts-nested-workflows.html

The Service Integration to Step Functions is initially limited to integrating with Step Functions running on this **ASL Workflow Engine**, however it should be possible to integrate with *real* AWS Step Functions relatively easily in due course using boto3, for example:

```python
import boto3
from botocore.exceptions import ClientError

sfn = boto3.client("stepfunctions")

try:
	response = sfn.start_execution(
            stateMachineArn=state_machine_arn,
            #name=execution_name,
            input=item
        )
except ClientError as e:
        self.logger.error(e.response)
```

The resource URI specified in the Task State used to trigger the Step Function should be an ARN of the form:

```
arn:aws:states:region:account-id:states:startExecution
```

The  Resource field however does not have sufficient information, so this service integration requires the use of Task state Parameters:

```json
"Parameters": {
    "Input": "ChildStepFunctionInput",
    "StateMachineArn": "ChildStateMachineArn",
    "Name": "OptionalExecutionName"
},
```

If the optional execution Name is not specified in the Parameters a UUID will be assigned by the service. The way to specify specific execution names (if so desired) is to pass the execution name in the stepfunction input and extract it in the Parameter's JSONPath processing e.g. something like:

```
"Name.$": "$.executionName"
```

The example [step_by_step](py/test/step_by_step.py) illustrates the use of the Step Function Service Integration. The important parts of the example are the Resource URI and Parameters fields in the StepFunctionLauncher Task State in the caller ASL:

```json
{
    "StartAt": "StepFunctionLauncher",
    "States": {
        "StepFunctionLauncher": {
            "Type": "Task",
            "Resource": "arn:aws:states:local:0123456789:states:startExecution",
            "Parameters": {  
                "Input.$": "$",
                "StateMachineArn": "arn:aws:states:local:0123456789:stateMachine:simple_state_machine"
            },
            "End": true
        }
    }
}
```

The use of `local` vice an actual AWS region should be a reasonable way of deciding whether to use the ASL Workflow Engine or real AWS Step Functions as the Service Integration.
