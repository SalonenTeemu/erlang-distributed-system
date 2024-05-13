# erlang-distributed-system

This repository contains a system implemented with erlang as an independent course project.
The system consists of servers, clients and control processes. Control processes direct clients what do ask from servers: subscribe to keys, write values to keys and read values from keys. Data is a simple list of tuples with basic single letter atom keys and integer values like: [{a, 1}, {b, 2}...]. The program can be started with 2 different control processes using integer 1 or 2 in start function argument. 1 is the default that runs basic client-server interactions and 2 adds failing and recovering of a server to it. In this case, I have used simply three clients, three servers and a datastructure of 3 tuple-elements.

## Running the app

Run the system in erlang shell.

```bash
# Compile the module
c(distributed_system).

# Start the system with:
distributed_system:start(1).
# or with:
distributed_system:start(2).
```
