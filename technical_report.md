# Technical Report

## What we built

We built a Function as a Service that implemented a Rest API that allowed a client to register functions and run them asynchronously on our workers by sending a request using the function's id. 

We implemented a number of workers, local workers on the task dispatcher, a pull worker that requested work whenver ready, and a push worker that was sent tasks when needed. 

## Why we built

The reason why we would build a Function as a Service is to allow clients to borrow compute from us in order to complete a function with given parameters. If you are someone trying to do particle simulations and only have a weak laptop, a FAAS can be great when it is hooked up to high performance servers that can handle a given task.

## How we built
