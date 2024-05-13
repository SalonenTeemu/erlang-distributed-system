-module(distributed_system).
-export([start/0, start/1]).

% System of servers, clients and control processes.
% Control processes direct clients what do ask from servers: subscribe to keys,
% write values to keys and read values from keys. Data is a simple list of tuples
% with basic single letter atom keys and integer values like: [{a, 1}, {b, 2}...].
% The program can be started with 2 different control processes using integer 1 or
% 2 in start function argument. 1 is default that runs basic client-server interactions
% and 2 adds failing and recovering of a server to it. In this case, I have used
% simply three clients, three servers and a datastructure of 3 tuple-elements.

% Run default control process if no number was given
start() ->
    start(1).

% Run either control process based on given value. If the value is not 1 or 2, run the default one
start(ControlProcessToUse) ->
    % Simple data structure
    Data = [{a, 1}, {b, 2}, {c, 3}],
    % Start the servers (3) and give them the data
    Servers = start_servers(Data),
    case ControlProcessToUse of
        2 ->
            io:format("Starting with control process 2 to simulate recovery and failure~n", []),
            control_2_failure_recovery(Servers);
        _ ->
            io:format("Starting with control process 1 (default) to simulate basic functionality~n", []),
            control_1_default(Servers)
    end.

% Default control process with interactions but not fail or recover
control_1_default(Servers) ->
    % Start three clients
    Client1 = spawn(fun() -> client(Servers) end),
    Client2 = spawn(fun() -> client(Servers) end),
    Client3 = spawn(fun() -> client(Servers) end),
    timer:sleep(1000),
    
    % Simulate client-server interactions
    % Index of the server to call is the last integer of every message sent to a client
    Client1 ! {subscribe, a, 1},
    Client2 ! {subscribe, b, 2},
    Client3 ! {subscribe, c, 3},
    Client1 ! {subscribe, c, 3},
    % Test that subscribing to non-existent keys will print an error
    Client1 ! {subscribe, x, 2},
    Client2 ! {subscribe, a, 3},
    Client3 ! {subscribe, b, 1},
    Client2 ! {write, a, 10, 1},
    timer:sleep(500),
    % Test that writing to non-existent keys will print an error
    Client1 ! {write, y, 404, 3},
    Client3 ! {read, a, 3},
    Client1 ! {write, b, 20, 1},
    Client3 ! {write, c, 30, 2},
    timer:sleep(500),
    % Test that trying to read a value of non-existing key will print an error
    Client1 ! {read, z, 2},
    Client2 ! {read, b, 1},
    Client1 ! {write, a, 100, 2},
    Client3 ! {write, b, 200, 3},
    Client1 ! {read, c, 3},
    timer:sleep(500),
    Client1 ! {read, a, 1},
    Client2 ! {read, b, 2},
    Client3 ! {read, c, 3},
    timer:sleep(1000),
    
    % Quit all servers and clients
    [S ! quit || S <- Servers],
    Client1 ! quit,
    Client2 ! quit,
    Client3 ! quit.

% Control process 2 with failing and recovering of a server
control_2_failure_recovery(Servers) ->
    % Start three clients
    Client1 = spawn(fun() -> client(Servers) end),
    Client2 = spawn(fun() -> client(Servers) end),
    Client3 = spawn(fun() -> client(Servers) end),
    % Take server 2 from the servers list to simulate its failure and recovery
    Server2 = lists:nth(2, Servers),
    timer:sleep(1000),
     
    % Simulate client interactions
    Client1 ! {subscribe, a, 1},
    Client3 ! {subscribe, c, 3},
    Client2 ! {subscribe, b, 2},
    Client1 ! {subscribe, c, 1},
    % Test that subscribing to non-existent keys will print an error
    Client3 ! {subscribe, x, 3},
    Client2 ! {subscribe, b, 1},
    Client3 ! {subscribe, a, 2},
    Client2 ! {write, a, 10, 1},
    timer:sleep(500),
    % Test that writing to non-existent keys will print an error
    Client1 ! {write, y, 404, 3},
    % Fail server 2
    Server2 ! fail,
    Client3 ! {read, a, 1},
    Client2 ! {write, b, 20, 1},
    Client3 ! {write, c, 30, 3},
    timer:sleep(500),
    % Test that trying to read a value of non-existing key will print an error
    Client1 ! {read, z, 1},
    % Recover server 2
    Server2 ! recover,
    timer:sleep(500),
    Client2 ! {read, b, 1},
    Client1 ! {write, a, 100, 2},
    Client3 ! {write, b, 200, 3},
    Client1 ! {read, c, 3},
    timer:sleep(500),
    Client1 ! {read, a, 1},
    Client2 ! {read, b, 2},
    Client3 ! {read, c, 3},
    timer:sleep(1000),
     
    % Quit all servers and clients
    [S ! quit || S <- Servers],
    Client1 ! quit,
    Client2 ! quit,
    Client3 ! quit.

% Start the servers with the data from the start-function
start_servers(Data) ->
    % Start the servers (3 in this case)
    S1 = spawn(fun() -> server(Data, [], []) end),
    S2 = spawn(fun() -> server(Data, [], []) end),
    S3 = spawn(fun() -> server(Data, [], []) end),
    timer:sleep(100),

    Servers = [S1, S2, S3],

    % Tell the other servers to each server process
    S1 ! {set_servers, Servers},
    S2 ! {set_servers, Servers},
    S3 ! {set_servers, Servers},

    % Return servers list so clients and control processes can use it
    Servers.

% Server process with the data structure, other servers and its current client subscribers to keys
server(Data, Servers, Subscribers) ->
    receive
        % Set the other servers by deleting self() from the list
        {set_servers, ServersToSet} ->
            OtherServers = lists:delete(self(), ServersToSet),
            io:format("Server [~p] sets list of other servers to: ~p~n", [self(), OtherServers]),
            server(Data, OtherServers, Subscribers);

        % Add a subscriber to a key in the subscribers list if the key provided exists in the data 
        {subscribe, Key, ClientPID} ->
            case proplists:get_value(Key, Data) of
                undefined ->
                    io:format("Key [~p] was not found in the data, so client [~p] was not set as subscriber for that key~n", [Key, ClientPID]),
                    server(Data, Servers, Subscribers);
                 _ ->
                    io:format("Server [~p] sets client [~p] as a subscriber for key [~p]~n", [self(), ClientPID, Key]),
                    % Add a subscriber to the key and update the whole subscribers list
                    UpdatedSubscribers = add_subscriber(Key, ClientPID, Subscribers),
                    server(Data, Servers, UpdatedSubscribers)
            end;

        % Write a new value to a key in the data if the key provided exists and tell this change to other servers
        {write, Key, Value} ->
            case proplists:get_value(Key, Data) of
                undefined ->
                    io:format("Server [~p] received WRITE message but key [~p] was not found in the data so value [~p] was not stored~n", [self(), Key, Value]),
                    server(Data, Servers, Subscribers);
                _ ->
                    io:format("Server [~p] received WRITE message to store value [~p] to key [~p]~n", [self(), Value, Key]),
                    % Replace the value of the key with the given value
                    NewData = lists:keyreplace(Key, 1, Data, {Key, Value}),
                    % Tell the other servers that the value for this key has changed
                    [Server ! {write_copy, Key, Value} || Server <- Servers],
                    SubscribersOfKey = proplists:get_value(Key, Subscribers, []),
                    % Send every subscriber of that key the new value
                    lists:foreach(fun(PID) -> PID ! {Key, Value} end, SubscribersOfKey),
                    server(NewData, Servers, Subscribers)
            end;

        % Write a new value to a key in the data if the key provided exists
        {write_copy, Key, Value} ->
            case proplists:get_value(Key, Data) of
                undefined ->
                    io:format("Server [~p] received WRITE_COPY message but key [~p] was not found in the data so value [~p] was not stored~n", [self(), Key, Value]),
                    server(Data, Servers, Subscribers);
                _ ->
                    io:format("Server [~p] received WRITE_COPY message to store value [~p] to key [~p]~n", [self(), Value, Key]),
                    % Replace the value of the key with the given value
                    NewData = lists:keyreplace(Key, 1, Data, {Key, Value}),
                    SubscribersOfKey = proplists:get_value(Key, Subscribers, []),
                    % Send every subscriber of that key the new value
                    lists:foreach(fun(PID) -> PID ! {Key, Value} end, SubscribersOfKey),
                    server(NewData, Servers, Subscribers)
            end;

        % Send value of the provided key to the client if the key provided exists
        {read, Key, ClientPID} ->
            case proplists:get_value(Key, Data) of
                undefined ->
                    io:format("Server [~p] received READ message but key [~p] was not found in the data so value can't be provided~n", [self(), Key]),
                    server(Data, Servers, Subscribers);
                _ ->
                    io:format("Server [~p] received READ message from client [~p] to send back value with key [~p]~n", [self(), ClientPID, Key]),
                    ClientPID ! {Key, proplists:get_value(Key, Data)},
                    server(Data, Servers, Subscribers)
                end;

        % Stop processing messages and wait for a RECOVER message to recover most current data
        fail ->
            io:format("Server [~p] received FAIL message and fails~n", [self()]),
            receive
                % Try to recover the most current data by asking the first server in the Servers-list
                recover ->
                    FirstServer = hd(Servers),
                    io:format("Server [~p] received RECOVER message and tries to recover latest data from server [~p]~n", [self(), FirstServer]),
                    FirstServer ! {get_data, self()},
                    receive
                        % When received current data from another server, update data and send values for clients that have subscribed to keys
                        {data, UpdatedData} ->
                            io:format("Server [~p] received back data from server [~p] and it sends values to subscribers~n", [self(), FirstServer]),
                            lists:foreach(
                                fun({Key, Value}) ->
                                    SubscribersOfKey = proplists:get_value(Key, Subscribers, []),
                                    lists:foreach(
                                        fun(PID) -> PID ! {Key, Value} end,
                                        SubscribersOfKey
                                    )
                                end,
                                UpdatedData
                            ),
                            server(UpdatedData, Servers, Subscribers)
                    end
            end;

        % Send the data to another server asking
        {get_data, PID} ->
            io:format("Server [~p] received GET_DATA message from server [~p] and sends them its data~n", [self(), PID]),
            PID ! {data, Data},
            server(Data, Servers, Subscribers);

        % Quit completely
        quit ->
            io:format("Server [~p] quits~n", [self()]);
    
        % Print other unknown messages
        Msg ->
            io:format("Server [~p] received unknown message: ~p~n", [self(), Msg]),
            server(Data, Servers, Subscribers)
    end.

% Helper function to add a subscriber to the key and update the whole subscribers list
add_subscriber(Key, ClientPID, Subscribers) ->
    case lists:keyfind(Key, 1, Subscribers) of
        false ->
            % Key not found
            [{Key, [ClientPID]} | Subscribers];
        {Key, Clients} ->
            % Key found
            UpdatedClients = lists:keystore(Key, 1, Subscribers, {Key, [ClientPID | Clients]}),
            UpdatedClients
    end.

% Client process with the servers as a list. Client receives messages from a control process and from servers.
% Control processes provide the index of a server for the client to send a message to.
client(Servers) ->
    receive
        % Subscribe to a certain key to a certain server
        {subscribe, Key, IndexOfServer} ->
            Server = lists:nth(IndexOfServer, Servers),
            Server ! {subscribe, Key, self()},
            client(Servers);

        % Write a new value to a certain key to a certain server 
        {write, Key, Value, IndexOfServer} ->
            Server = lists:nth(IndexOfServer, Servers),
            Server ! {write, Key, Value},
            client(Servers);

        % Read a value of a certain key from a certain server
        {read, Key, IndexOfServer} ->
            Server = lists:nth(IndexOfServer, Servers),
            Server ! {read, Key, self()},
            client(Servers);

        % Receive value for a key from a server and print it out
        {Key, Value} ->
            io:format("Client [~p] received value for key [~p] = [~p]~n", [self(), Key, Value]),
            client(Servers);

        % Quit the client process
        quit ->
            io:format("Client [~p] quits~n", [self()]);

        % Print other unknown messages
        Msg ->
            io:format("Client [~p] received unknown message: ~p~n", [self(), Msg]),
            client(Servers)
    end.