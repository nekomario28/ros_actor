# ros_actor
is a framework based on actor like model for ROS/Python.  
This repository is for developing the library.
Please switch to [actor_demo](https://github.com/momoiorg-repository/actor_demo) repository for the use of it.

## Asynchronous transaction cancellation

`run_actor_async()` returns a transaction. `transaction.close()` silently
suppresses future completion delivery. `transaction.abort()` also suppresses
future completion delivery; for a synchronous actor invoked asynchronously,
it first reports `False` once to distinguish cancellation from a successful
`None` result. Regular asynchronous actors are aborted without a completion
notification. Completion and cancellation compete to claim the transaction;
if completion wins, its callback may finish after `abort()` returns.

These operations do not forcibly stop a running Python function or send a ROS
Action goal cancellation request. Callers such as ActorBT must retain the
transaction and explicitly interrupt it; application-specific motor stopping
remains separate. Executor capacity is released when the scheduled worker exits,
not when cancellation is requested.

Run the transaction lifecycle tests without ROS:

```sh
python -m unittest discover -s tests -v
```
