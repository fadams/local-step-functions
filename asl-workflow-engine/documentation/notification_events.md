# Notification Events
With *real* AWS Step Functions it is possible to configure Step Functions to emit [CloudWatch Events](https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html) (recently renamed [EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/what-is-amazon-eventbridge.html) by Amazon) when an execution status changes. This enables you to monitor your workflows without having to constantly poll using the [DescribeExecution](https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeExecution.html) API.

Obviously with a local on premises ASL implementation the CloudWatch/EventBridge service is not readily available, however we can provide similar behaviour.

With this implementation we make use of the messaging fabric used for the event queue (currently RabbitMQ) where the CloudWatch-like notification events are published to a topic of the form:
```
asl_workflow_engine/<state_machine_arn>.<status>
```
That is to say we publish to the **asl_workflow_engine** topic exchange with a subject comprising the *State Machine ARN* and the *status* separated by a dot. e.g.
```
asl_workflow_engine/arn:aws:states:local:0123456789:stateMachine:simple_state_machine.SUCCEEDED
```
Note that the topic (e.g. AMQP topic exchange) used to broadcast notification events is currently **asl_workflow_engine**, however that is configurable.

Consumer applications may subscribe to specific events using the full subject, or groups of event using wildcards.

For example, to subscribe only to the SUCCEEDED event for the given state machine the following subject (routing key) should be used:
```
arn:aws:states:local:0123456789:stateMachine:simple_state_machine.SUCCEEDED
```
To subscribe to all notification events published for a given state machine the following subject (routing key) should be used:
```
arn:aws:states:local:0123456789:stateMachine:simple_state_machine.*
```
To subscribe to all notification events from the asl_workflow_engine use the wildcard `#` as the subject (routing key).

The format of the message bodies is as described in [Event Patterns in CloudWatch Events](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/CloudWatchEventsandEventPatterns.html) and more specifically in [CloudWatch Events for Step Functions Execution Status Changes](https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html).

e.g. for Execution Started
```
{
    "version": "0",
    "id": "315c1398-40ff-a850-213b-158f73e60175",
    "detail-type": "Step Functions Execution Status Change",
    "source": "aws.states",
    "account": "012345678912",
    "time": "2019-02-26T19:42:21Z",
    "region": "us-east-1",
    "resources": [
      "arn:aws:states:us-east-1:012345678912:execution:state-machine-name:execution-name"
    ],
    "detail": {
        "executionArn": "arn:aws:states:us-east-1:012345678912:execution:state-machine-name:execution-name",
        "stateMachineArn": "arn:aws:states:us-east-1:012345678912:stateMachine:state-machine",
        "name": "execution-name",
        "status": "RUNNING",
        "startDate": 1551225271984,
        "stopDate":  null,
        "input": "{}",
        "output": null
    }
}
```
Execution Succeeded and Execution Failed follow the same pattern except for the obvious difference in the ["detail]["status"] field.

Execution Timed Out and Execution Aborted events are not yet implemented.

The AWS documentation is not terribly clear whether a timeout would result in both an Execution Timed Out and Execution Failed event. It probably should as the [ASL Specification](https://states-language.net/spec.html) states:

*A State Machine MAY have an integer field named “TimeoutSeconds”. If provided, it provides the maximum number of seconds the machine is allowed to run. If the machine runs longer than the specified time, then the interpreter fails the machine with a States.Timeout Error Name.*

and

 *If the state runs longer than the specified timeout, or if more time than the specified heartbeat elapses between heartbeats from the task, then the interpreter fails the state with a States.Timeout Error Name.*

### CloudWatch Documentation
The most relevant sections of the AWS CloudWatch documentation for Step Functions may be found at the following links:

https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/WhatIsCloudWatchEvents.html

https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/CloudWatchEventsandEventPatterns.html

https://docs.aws.amazon.com/step-functions/latest/dg/cw-events.html
