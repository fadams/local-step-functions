# asl-workflow-engine
This project is a workflow engine written in Python 3 based on the [Amazon States Language](https://states-language.net/spec.html) (ASL). The intention is to incrementally provide the features of AWS Step Functions in an engine that can be deployed to [a range of different hosting environments](docker).

The initial goal is primarily learning about ASL and the focus will be on enabling relatively simple "straight line" state transitions (i.e. currently [Parallel](https://states-language.net/spec.html#parallel-state) states are not supported, though [Choice](https://states-language.net/spec.html#choice-state) states are) and [Task](https://states-language.net/spec.html#task-state) state resources shall initially concentrate on integrations with [Oracle Fn Project](https://github.com/fnproject), [OpenFaaS](https://github.com/openfaas/faas) and [AMQP](https://www.amqp.org/) based RPC Message invocations (as described here: https://www.rabbitmq.com/tutorials/tutorial-six-python.html).

**Warning** this project is still a work-in-progress although most of the basics are now in place. The main gaps are currently lack of support for the Parallel and Map states and Activities. Clustering/scaling is also yet ro be implemented.

### Initial Design Choices
The [Initial Design Choices](initial_design_choices.md) document describes why certain choices, like the use of a messaging fabric for the event queue, were made.

This document is a good first port of call to understand some of the nuances in the implementation covering the structure of the JSON objects on the event queue and the ASL context objects used by Step Functions internally.

### Notification Events
With AWS Step Functions it is possible to configure Step Functions to emit [CloudWatch Events](https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html) when an execution status changes.

Obviously with a local on premises ASL implementation the CloudWatch service is not available, however we can provide similar behaviour. The [Notification Events](notification_events.md) document describes how CloudWatch style events are emitted by the ASL Workflow Engine.

### Clustering and Scaling
**TODO**

### Service Integrations
The ASL Workflow Engine integrates with a number of services so that you can call API actions, and coordinate executions directly from the Amazon States Language in Step Functions.

With ASL you call and pass parameters to the API of those services from a [Task](https://states-language.net/spec.html#task-state) state in the Amazon States Language.

See the ASL Workflow Engine [Service Integrations](service_integrations.md) page for details of the Service Integrations currently implemented.

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