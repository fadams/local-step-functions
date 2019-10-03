# asl-workflow-engine
This project is a workflow engine written in Python 3 based on the [Amazon States Language](https://states-language.net/spec.html) (ASL). The intention is to incrementally provide the features of AWS Step Functions in an engine that can be deployed to [a range of different hosting environments](docker).

The initial goal is primarily learning about ASL and the focus will be on enabling relatively simple "straight line" state transitions (i.e. currently [Parallel](https://states-language.net/spec.html#parallel-state) states are not supported, though [Choice](https://states-language.net/spec.html#choice-state) states are) and [Task](https://states-language.net/spec.html#task-state) state resources shall initially concentrate on integrations with [Oracle Fn Project](https://github.com/fnproject), [OpenFaaS](https://github.com/openfaas/faas) and [AMQP](https://www.amqp.org/) based RPC Message invocations (as described here: https://www.rabbitmq.com/tutorials/tutorial-six-python.html).

**Warning** the project is still very much a work-in-progress although most of the basics are now in place.

### Initial Design Choices
ASL is essentially a Finite State Machine and a common approach for triggering FSMs is the [Event-driven Finite State Machine](https://en.wikipedia.org/wiki/Event-driven_finite-state_machine). ASL implementations can push [data](https://states-language.net/spec.html#data) between states in the form of JSON objects, so we need some way to facilitate this in the form of a queue of JSON objects. There are obviously many possible queue implementations and ideally we should abstract the detail, but initially we shall be using AMQP 0.9.1 via RabbitMQ and the Pika 1.0.1 client.

In order to maximise the possibilities for horizontally scaling, in an ideal world we would want to keep the main state machine engine, slightly ironically, as stateless as possible. The approach taken for this is to pass the **current state name** in the application context, along with the application data to be delivered to the next state on the event queue.

The advantages of using a full-blown messaging system become apparent when managing state in this way, as it becomes possible to attach multiple instances of the state engine to the event queue to facilitate horizontal scaling and we can leverage reliable message delivery and durability from the messaging system.

The state machine engine obviously needs to hold the actual state machine(s) representing the Step Functions and again we would wish to maximise opportunities for horizontal scaling. To this end as well as passing the current state ID it is necessary to pass some information about the state machine actually being executed on the event queue.

As ASL state machines tend to be relatively modest in size one option is simply to pass the complete JSON ASL definition object, which makes scaling trivial at the expense of increased messaging bandwidth. Another option is to pass a reference ID for the state engine to look up (and subsequently cache). It is not clear which of those approaches is most useful, so supporting both initially is probably a good idea.

The AWS documentation describes the format of the context object here: https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html. More information on the context object may be found in the section on waiting for a callback with a task token: https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html.
```
{
	"Execution": {
		"Id": <String>,
		"Input": <Object>,
		"StartTime": <String Format: ISO 8601>
	},
	"State": {
		"EnteredTime": <String Format: ISO 8601>,
		"Name": <String>,
		"RetryCount": <Number>
	},
	"StateMachine": {
		"Id": <String>,
		"Definition": <Object representing ASL state machine>
	},
	"Task": {
		"Token": <String>
	}
}
```
Given these choices the format of the JSON objects on the event queue should look something like this:
```
{
	"data": <Object representing application data>,
	"context": <Object representing application context>,
}
```
The `data` field contains application [Data](https://states-language.net/spec.html#data) as defined in the ASL Specification.

The `context` field contains application [Context](https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html) as defined above.

The [Paths](https://states-language.net/spec.html#paths) section of the ASL specification describes the use of JSONPath root (`$`) to refer to the root of the data object and the [Parameters](https://states-language.net/spec.html#parameters) describes the use of `$$` to refer to the root of the context object.


The `$$.State.Name` (i.e. the **current state**) field must contain either a state name valid in the ASL state machine being referred to in ASL, or null, or an empty string or be undefined. In the case of null or empty string or undefined it shall be assumed that the state transition will be to the ASL [StartAt](https://states-language.net/spec.html#toplevelfields) state.

The (otional) `$$.StateMachine.Definition` field contains a complete ASL state machine definition as defined in the [Amazon States Language Specification](https://states-language.net/spec.html). Note that the `$$.StateMachine.Definition` path is an optional extension and is not found in the context object of the *official* Amazon AWS Step Functions implementation.

The `$$.StateMachine.Id` field contains a unique reference to an ASL state machine.

Either one or both of `$$.StateMachine.Definition` or 
`$$.StateMachine.Id` must be supplied.

* If both are supplied the state engine will attempt to cache the ASL.
* If only `$$.StateMachine.Id` is supplied the state engine will attempt to use a cached value and will fail if one is not present.
* If only `$$.StateMachine.Definition` is present the state engine will used that, but will be unable to cache it.

The format of `$$.StateMachine.Id` *may* simply be any unique ID, however ideally it *should* follow the pattern of [Amazon Resource Names (ARNs)](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) and in particular *should* follow the stateMachine ARN form given in [syntax for Step Functions](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-step-functions) e.g.
```
arn:aws:states:region:account-id:activity:activityName
arn:aws:states:region:account-id:stateMachine:stateMachineName
arn:aws:states:region:account-id:execution:stateMachineName:executionName
```
### Service Integrations
The ASL Workflow Engine integrates with a number of services so that you can call API actions, and coordinate executions directly from the Amazon States Language in Step Functions. You can directly call and pass parameters to the API of those services. You coordinate these services directly from a [Task](https://states-language.net/spec.html#task-state) state in the Amazon States Language.

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
```
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
```
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
```
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
```
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

### REST API
The ASL Workflow Engine implements a REST API that is (currently) a subset of the AWS Step Functions API as described in the AWS documentation:
https://docs.aws.amazon.com/step-functions/latest/apireference/API_Operations.html. The intention is that over time the complete API will be implemented.

By implementing the AWS REST API semantics it becomes possible to use Amazon's CLI and SDKs so applications can use this ASL Workflow Engine as an alternative to Amazon's for scenarios such as hybrid cloud workloads.

The Actions from the official AWS API currently implemented by the ASL Workflow Engine are:

* [CreateStateMachine](https://docs.aws.amazon.com/step-functions/latest/apireference/API_CreateStateMachine.html)
* [DeleteStateMachine](https://docs.aws.amazon.com/step-functions/latest/apireference/API_DeleteStateMachine.html)
* [DescribeStateMachine](https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeStateMachine.html)
* [DescribeStateMachineForExecution](https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeStateMachineForExecution.html)
* [ListStateMachines](https://docs.aws.amazon.com/step-functions/latest/apireference/API_ListStateMachines.html)
* [StartExecution](https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html)
* [UpdateStateMachine](https://docs.aws.amazon.com/step-functions/latest/apireference/API_UpdateStateMachine.html)
* [ListExecutions](https://docs.aws.amazon.com/step-functions/latest/apireference/API_ListExecutions.html)
* [DescribeExecution](https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html)
* [GetExecutionHistory](https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html)

The official API Actions not currently implemented are:

* [CreateActivity](https://docs.aws.amazon.com/step-functions/latest/apireference/API_CreateActivity.html)
* [DeleteActivity](https://docs.aws.amazon.com/step-functions/latest/apireference/API_DeleteActivity.html)
* [DescribeActivity](https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeActivity.html)
* [GetActivityTask](https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetActivityTask.html)
* [ListActivities](https://docs.aws.amazon.com/step-functions/latest/apireference/API_ListActivities.html)
* [ListTagsForResource](https://docs.aws.amazon.com/step-functions/latest/apireference/API_ListTagsForResource.html)
* [SendTaskFailure](https://docs.aws.amazon.com/step-functions/latest/apireference/API_SendTaskFailure.html)
* [SendTaskHeartbeat](https://docs.aws.amazon.com/step-functions/latest/apireference/API_SendTaskHeartbeat.html)
* [SendTaskSuccess](https://docs.aws.amazon.com/step-functions/latest/apireference/API_SendTaskSuccess.html)
* [StopExecution](https://docs.aws.amazon.com/step-functions/latest/apireference/API_StopExecution.html)
* [TagResource](https://docs.aws.amazon.com/step-functions/latest/apireference/API_TagResource.html)
* [UntagResource](https://docs.aws.amazon.com/step-functions/latest/apireference/API_UntagResource.html)

The Activity API will be implemented in due course, though in an Open Source ASL implementation such as this implementing additional Service Integrations might be more elegant, though Step Function Activities do provide as fairly general integration approach.

Some examples of using the AWS CLI follow, note the use of the `--endpoint` (or `--endpoint-url`) flag to tell the CLI to use a different (non-default) REST endpoint.
```
# List state machines
aws stepfunctions --endpoint http://localhost:4584 list-state-machines --max-results 20

# Create a new state machine
aws stepfunctions --endpoint http://localhost:4584 create-state-machine --name my-state-machine --definition '{"Comment":"A Hello World example of the Amazon States Language using a Pass state","StartAt":"HelloWorld","States":{"HelloWorld":{"Type":"Pass","End":true}}}' --role-arn arn:aws:iam::0123456789:role/service-role/MyRole

# Create a new state machine from a file
aws stepfunctions --endpoint http://localhost:4584 create-state-machine --name simple_state_machine --definition file://<path-to-ASL-JSON> --role-arn arn:aws:iam::0123456789:role/service-role/MyRole

# Update a state machine
aws stepfunctions --endpoint http://localhost:4584 update-state-machine --definition '{"Comment":"A Hello World example of the Amazon States Language using a Pass state","StartAt":"HelloWorld","States":{"HelloWorld":{"Type":"Pass","End":true}}}' --role-arn arn:aws:iam::0123456789:role/service-role/MyRole --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine

# Describe state machine
aws stepfunctions --endpoint http://localhost:4584 describe-state-machine --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine

# Delete state machine
aws stepfunctions --endpoint http://localhost:4584 delete-state-machine --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine

# Start state machine execution
aws stepfunctions --endpoint http://localhost:4584 start-execution --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine --name my-execution --input '{"comment":"I am a great input !"}'

# List state machine executions
aws stepfunctions --endpoint http://localhost:4584 list-executions --state-machine-arn arn:aws:states:local:0123456789:stateMachine:my-state-machine

# Describe execution
aws stepfunctions --endpoint http://localhost:4584 describe-execution --execution-arn arn:aws:states:local:0123456789:execution:my-state-machine:my-execution

# Describe state machine related to execution
aws stepfunctions --endpoint http://localhost:4584 describe-state-machine-for-execution --execution-arn arn:aws:states:local:0123456789:execution:my-state-machine:my-execution

# Get execution history
aws stepfunctions --endpoint http://localhost:4584 get-execution-history --execution-arn arn:aws:states:local:0123456789:execution:my-state-machine:my-execution
```

In order to use boto3 with the ASL Workflow Engine the endpoint_url should be set as follows:
```
sfn = boto3.client("stepfunctions", endpoint_url="http://localhost:4584")
```