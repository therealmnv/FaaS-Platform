# Technical Report

## What we built

We built a Function as a Service that implemented a Rest API that allowed a client to register functions and run them asynchronously on our workers by sending a request using the function's id. 

We implemented a number of workers, local workers on the task dispatcher, a pull worker that requested work whenver ready, and a push worker that was sent tasks when needed. 

The reason why we would build a Function as a Service is to allow clients to borrow compute from us in order to complete a function with given parameters. If you are someone trying to do particle simulations and only have a weak laptop, a FAAS can be great when it is hooked up to high performance servers that can handle a given task.

We have a couple of simple GET and POST requests on our RestAPI where a client can register a function, execute a task, see the status of a task, and see the result of a task. This simple interface really complements the function as a service where you are essentially sending work and getting results, which is all a FAAS-user could ask for!

We implemented a couple of different types of workers that could potentially favor a client given the archetype of work they are doing, if they have a low number of tasks that want to be quickly done and given back, the local workers are great. If they have a couple of high compute tasks, the pull workers are great for getting those done and shooting them back. If you have a lot of tasks that should be done concurrently and might take some time, the push workers will be good!

## Local Workers

The local worker has a processor pool where it can run functions concurrently. This processor pulls from a Queue of tasks and starts deploying asynchronous executions. When it is done within that execution, it places the results onto a queue that gets processed to update the status of the task on the Redis Server.

The local worker has a fixed amount of processors but by using it Queue we can make sure we don't miss any tasks and also make sure we don't overload our processors with more work than it can handle.

[LIMITATIONS] Some of the drawbacks are the ability to get overwhelmed, this is of course a pretty reasonable outcome for something that won't exactly scale but it is important to note that we don't want things breaking, we want things to slow down if needed.

## Pull Workers

The pull worker is interacting with the task dispatcher using a REQ/REP format in which the worker indicates to the task dispatcher that is ready to do work and then the task dispatcher can reply with a task if it has any in the queue. This allows for the pull worker to minimize its downtime since whenever it finishes a task, it asks for more. 

The pull worker also uses Queues to load up on work and also start shipping it back. The queues are important since we may work on multiple tasks at once but there is a fixed back and forth communication style to the REQ/REP.

[LIMITATIONS] The pull workers are limited by a single REQ-REP socket connection. ZMQ internally handles the synchronization of requests and may cause blocking in case we use the same port for hundreds of processors. 

[NOTE] We also tried an architecture where we have multiple processors in a worker making an individual connection to the dispatcher but that was bad for scaling horizontally because the number of processors having a socket connection to the dispatcher increase exponentially. 

## Push Workers

The push worker is interacting with the task dispatcher using a DEALER/ROUTER format in which the worker joins the fleet of workers for the task dispatcher and sends a heartbeat to indicate it is still available. With this, the task dispatcher is able to send out a jump to one of the workers in the fleet in a nice distributed way and can simply monitor the heartbeat of the workers on the fleet to make sure there are no worker failure exceptions. 

The pull worker also uses Queues to load up on work and also start shipping it back. These queues can be good because a worker may get pushed a lot of work and does not want to completely lose out on it doing it.

[LIMITATIONS] A drawback of the push worker setup is that of the heartbeat checking, we are checking if workers are alive by iterating over the worker_id array and checking when their last heartbeat was last logged in. This establishes a bit of overhead.

