# asl-workflow-engine
This project is a workflow engine written in Python based on [Amazon States Language](https://states-language.net/spec.html) (ASL). The intention is to incrementally provide the features of AWS Step Functions in an engine that can be deployed to a range of different hosting environments.

**N.B. This is currently more or less just a toy for experimenting with ASL**. The initial goal is primarily learning about ASL and the focus will be on enabling a very simple "straight line" state transition (i.e. initially no [Parallel](https://states-language.net/spec.html#parallel-state) or [Choice](https://states-language.net/spec.html#choice-state) states) and [Task](https://states-language.net/spec.html#task-state) state resources shall initially concentrate on integrations with [Oracle Fn Project](https://github.com/fnproject), [OpenFaaS](https://github.com/openfaas/faas) and [AMQP](https://www.amqp.org/) based RPC invocations (as described here: https://www.rabbitmq.com/tutorials/tutorial-six-python.html).

### Initial Design Choices
ASL is essentially a Finite State Machine and a common approach for triggering FSMs is the [Event-driven Finite State Machine](https://en.wikipedia.org/wiki/Event-driven_finite-state_machine). ASL can push [data](https://states-language.net/spec.html#data) between states in the form of JSON objects, so we need some way to facilitate this in the form of a queue of JSON objects. There are obviously many possible queue implementations and ideally we should abstract the detail, but initially we shall be using AMQP 0.9.1 via RabbitMQ.

In order to maximise the possibilities for horizontally scaling, in an ideal world we would want to keep the main state machine engine, slightly ironically, as stateless as possible. The approach taken for this is to pass the **current state ID** along with the application data to be delivered to the next state on the event queue.

The advantages of using a full-blown messaging system become apparent when managing state in this way, as it becomes possible to attach multiple instances of the state engine to the event queue to facilitate horizontal scaling and we can leverage reliable message delivery and durability from the messaging system.

The state machine engine obviously needs to hold the actual state machine(s) representing the Step Functions and again we would wish to maximise opportunities for horizontal scaling. To this end as well as passing the current state ID it is necessary to pass some information about the state machine actually being executed on the event queue. As ASL state machines tend to be relatively modest in size one option is simply to pass the complete JSON ASL object, another option is to pass a reference for the state engine to look up (and subsequently cache). It is not clear which of those approaches is most useful so supporting both is probably a good idea.

Given these choices the format of the JSON objects on the event queue should look something like this:
```
{
	"CurrentState": <String representing current state ID>,
	"Data": <Object representing application data>,
	"ASL": <Object representing ASL state machine>,
	"ASLRef": <String representing unique ref to ASL>
}
```
The **CurrentState** field must contain either a state name valid in the ASL state machine being referred to in ASL or ASLRef or null or an empty string. In the case of null or empty string it shall be assumed that the state transition will be to the ASL "StartAt" state.

The **Data** field contains application [Data](https://states-language.net/spec.html#data) as defined in the [Amazon States Language Specification](https://states-language.net/spec.html).

The **ASL** field contains a complete ASL state machine as defined in the [Amazon States Language Specification](https://states-language.net/spec.html).

The **ASLRef** field contains a unique reference to an ASL state machine.

Either one or both of ASL or ASLRef must be supplied. If both are supplied the state engine will attempt to cache (and in later iterations persist) the ASL. If only ASLRef is supplied the state engine will attempt to use a cached value and will fail if one is not present. If only ASL is present the state engine will used that, but will be unable to cache it.

The format of **ASLRef** *may* simply be any unique ID, however ideally it *should* follow the pattern of [Amazon Resource Names (ARNs)](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) and in particular *should* follow the stateMachine ARN form given in [syntax for Step Functions](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-step-functions) e.g.
```
arn:aws:states:region:account-id:activity:activityName
arn:aws:states:region:account-id:stateMachine:stateMachineName
arn:aws:states:region:account-id:execution:stateMachineName:executionName
```
