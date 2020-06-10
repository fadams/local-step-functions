# Initial Design Choices
ASL is essentially a Finite State Machine and a common approach for triggering FSMs is the [Event-driven Finite State Machine](https://en.wikipedia.org/wiki/Event-driven_finite-state_machine). ASL implementations can push [data](https://states-language.net/spec.html#data) between states in the form of JSON objects, so we need some way to facilitate this in the form of a queue of JSON objects. There are obviously many possible queue implementations and ideally we should abstract the detail, but initially we shall be using AMQP 0.9.1 provided by RabbitMQ and the Pika 1.0.1 client.

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

The (otional) `$$.StateMachine.Definition` field contains a complete ASL state machine definition as defined in the [Amazon States Language Specification](https://states-language.net/spec.html). Note that the `$$.StateMachine.Definition` path is an optional extension provided by this implementation and is not found in the context object of the *official* Amazon AWS Step Functions implementation.

**Important Note** passing the Definition by value as described in the previous paragraph was used during the initial development of the ASL Workflow Engine whilst the REST API did not exist. Using the REST API and SDKs is now the preferred approach and support for passing the State Machine Definition by value in the context might well be deprecated.

The `$$.StateMachine.Id` field contains a unique reference to an ASL state machine.

Either one or both of `$$.StateMachine.Definition` or 
`$$.StateMachine.Id` must be supplied.

* If both are supplied the state engine will attempt to cache the ASL.
* If only `$$.StateMachine.Id` is supplied the state engine will attempt to use a cached value and will fail if one is not present.
* If only `$$.StateMachine.Definition` is present the state engine will used that, but will be unable to cache it.

The format of `$$.StateMachine.Id` follows the pattern of [Amazon Resource Names (ARNs)](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html) and in particular the stateMachine ARN form given in [syntax for Step Functions](https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arn-syntax-step-functions) e.g.
```
arn:aws:states:region:account-id:stateMachine:stateMachineName
```
Other ARNs used in this implementation include the execution ARN used to identify specific execution instances of the State Machine.
```
arn:aws:states:region:account-id:execution:stateMachineName:executionName
```
The activity ARN is not currently used as Step Function activities are not yet supported, though the intention is to do so IDC.
```
arn:aws:states:region:account-id:activity:activityName
```