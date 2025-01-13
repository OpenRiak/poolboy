%% Poolboy - A hunky Erlang worker pool factory

-module(poolboy).
-behaviour(gen_fsm).

-compile({nowarn_deprecated_function, 
            [{gen_fsm, start_link, 3},
                {gen_fsm, start, 3},
                {gen_fsm, reply, 2},
                {gen_fsm, sync_send_event, 3},
                {gen_fsm, send_event, 2},
                {gen_fsm, sync_send_all_state_event, 2}]}).

-export([checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
         get_pool_size/1, set_pool_size/2, set_pool_size/3,
         child_spec/2, child_spec/3, start/1, start/2, start_link/1,
         start_link/2, stop/1, status/1, status_ext/1]).
-export([init/1, ready/2, ready/3, overflow/2, overflow/3, full/2, full/3,
         handle_event/3, handle_sync_event/4, handle_info/3, terminate/3,
         code_change/4]).
-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_fsm, pulse_gen_fsm},
                                 {gen_server, pulse_gen_server},
                                 {supervisor, pulse_supervisor}]}).
-endif.

-define(TIMEOUT, 5000).

-ifdef(namespaced_types).
-type poolboy_queue() :: queue:queue().
-else.
-type poolboy_queue() :: queue().
-endif.

-record(state, {
    supervisor :: pid() | undefined,
    workers :: poolboy_queue() |undefined,
    waiting :: poolboy_queue(),
    monitors :: ets:tid(),
    size = 5 :: non_neg_integer(),
    latched_size = 5 :: non_neg_integer(),  %% as resized; size to converge eventually with that
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer()
}).

-spec checkout(Pool :: node() | pid()) -> pid().
checkout(Pool) ->
    checkout(Pool, true).

-spec checkout(Pool :: node() | pid(), Block :: boolean()) -> pid() | full.
checkout(Pool, Block) ->
    checkout(Pool, Block, ?TIMEOUT).

-spec checkout(Pool :: node() | pid(), Block :: boolean(), Timeout :: timeout())
    -> pid() | full.
checkout(Pool, Block, Timeout) ->
    gen_fsm:sync_send_event(Pool, {checkout, Block, Timeout}, Timeout).

-spec checkin(Pool :: node() | pid(), Worker :: pid()) -> ok.
checkin(Pool, Worker) when is_pid(Worker) ->
    gen_fsm:send_event(Pool, {checkin, Worker}).

-spec transaction(Pool :: node() | pid(), Fun :: fun((Worker :: pid()) -> any()))
    -> any().
transaction(Pool, Fun) ->
    Worker = poolboy:checkout(Pool),
    try
        Fun(Worker)
    after
        ok = poolboy:checkin(Pool, Worker)
    end.

-spec get_pool_size(pid()) -> {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
get_pool_size(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, get_pool_size).

-spec set_pool_size(pid(), non_neg_integer()) -> ok.
set_pool_size(Pid, NewSize) ->
    gen_fsm:sync_send_all_state_event(Pid, {set_pool_size, NewSize}).
-spec set_pool_size(pid(), non_neg_integer(), non_neg_integer()) -> ok.
set_pool_size(Pid, NewSize, NewMaxOverflow) ->
    gen_fsm:sync_send_all_state_event(Pid, {set_pool_size, NewSize, NewMaxOverflow}).

-spec child_spec(Pool :: node(), PoolArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(Pool, PoolArgs) ->
    child_spec(Pool, PoolArgs, []).

-spec child_spec(Pool :: node(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(Pool, PoolArgs, WorkerArgs) ->
    {Pool, {poolboy, start_link, [PoolArgs, WorkerArgs]},
     permanent, 5000, worker, [poolboy]}.

-spec start(PoolArgs :: proplists:proplist())
    -> {ok, pid()}.
start(PoolArgs) ->
    start(PoolArgs, PoolArgs).

-spec start(PoolArgs :: proplists:proplist(),
            WorkerArgs:: proplists:proplist())
    -> {ok, pid()}.
start(PoolArgs, WorkerArgs) ->
    start_pool(start, PoolArgs, WorkerArgs).

-spec start_link(PoolArgs :: proplists:proplist())
    -> {ok, pid()}.
start_link(PoolArgs)  ->
    %% for backwards compatability, pass the pool args as the worker args as well
    start_link(PoolArgs, PoolArgs).

-spec start_link(PoolArgs :: proplists:proplist(),
                 WorkerArgs:: proplists:proplist())
    -> {ok, pid()}.
start_link(PoolArgs, WorkerArgs)  ->
    start_pool(start_link, PoolArgs, WorkerArgs).

-spec stop(Pool :: node()) -> ok.
stop(Pool) ->
    gen_fsm:sync_send_all_state_event(Pool, stop).

-spec status(Pool :: node()) -> {atom(), integer(), integer(), integer()}.
status(Pool) ->
    gen_fsm:sync_send_all_state_event(Pool, status).
-spec status_ext(Pool :: node()) -> {atom(), integer(), integer(), integer(),
                                     non_neg_integer(), non_neg_integer()}.
status_ext(Pool) ->
    gen_fsm:sync_send_all_state_event(Pool, status_ext).

init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    init(PoolArgs, WorkerArgs, #state{waiting=Waiting, monitors=Monitors}).

init([{worker_module, Mod} | Rest], WorkerArgs, State) when is_atom(Mod) ->
    {ok, Sup} = poolboy_sup:start_link(Mod, WorkerArgs),
    init(Rest, WorkerArgs, State#state{supervisor=Sup});
init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
    init(Rest, WorkerArgs, State#state{size=Size, latched_size=Size});
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State) when is_integer(MaxOverflow) ->
    init(Rest, WorkerArgs, State#state{max_overflow=MaxOverflow});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size=Size, supervisor=Sup, max_overflow=MaxOverflow}=State) ->
    Workers = prepopulate(Size, Sup),
    StartState = case Size of
        Size when Size < 1, MaxOverflow < 1 -> full;
        Size when Size < 1 -> overflow;
        Size -> ready
    end,
    {ok, StartState, State#state{workers=Workers}}.

ready({checkin, Pid}, State) ->
    #state{size = Size,
           overflow = Overflow,
           max_overflow = MaxOverflow,
           latched_size = LatchedSize,
           supervisor = Sup,
           monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            {NewSize, NewOverflow, Workers} =
                case Size + Overflow > LatchedSize + MaxOverflow of  %% when we shrunk
                    true when Size > LatchedSize ->
                        ok = dismiss_worker(Sup, Pid),
                        {Size - 1, Overflow, State#state.workers};
                    true ->
                        ok = dismiss_worker(Sup, Pid),
                        {Size, Overflow - 1, State#state.workers};
                    false ->
                        {Size, Overflow, queue:in(Pid, State#state.workers)}
                end,
            {next_state, ready, State#state{workers = Workers,
                                            size = NewSize,
                                            overflow = NewOverflow}};
        [] ->
            {next_state, ready, State}
    end;
ready(_Event, State) ->
    {next_state, ready, State}.

ready({checkout, Block, Timeout}, {FromPid, _}=From, State) ->
    #state{supervisor = Sup,
           size = Size,
           latched_size = LatchedSize,
           workers = Workers,
           monitors = Monitors,
           max_overflow = MaxOverflow} = State,
    if Size < LatchedSize ->  %% we grew
            %% we are here after a set_pool_size with Size > OldSize
            {Pid, Ref} = new_worker(Sup, FromPid),
            true = ets:insert(Monitors, {Pid, Ref}),
            {reply, Pid, ready, State#state{size = Size + 1}};
       el/=se ->
            case queue:out(Workers) of
                {{value, Pid}, Left} ->
                    Ref = erlang:monitor(process, FromPid),
                    true = ets:insert(Monitors, {Pid, Ref}),
                    NextState = case queue:is_empty(Left) of
                                    true when MaxOverflow < 1 -> full;
                                    true -> overflow;
                                    false -> ready
                                end,
                    {reply, Pid, NextState, State#state{workers=Left}};
                {empty, Empty} when MaxOverflow > 0 ->
                    {Pid, Ref} = new_worker(Sup, FromPid),
                    true = ets:insert(Monitors, {Pid, Ref}),
                    {reply, Pid, overflow, State#state{workers=Empty, overflow=1}};
                {empty, Empty} when Block =:= false ->
                    {reply, full, full, State#state{workers=Empty}};
                {empty, Empty} ->
                    Waiting = add_waiting(From, Timeout, State#state.waiting),
                    {next_state, full, State#state{workers=Empty, waiting=Waiting}}
            end
    end;
ready(_Event, _From, State) ->
    {reply, ok, ready, State}.

overflow({checkin, Pid}, #state{overflow=0}=State) ->
    #state{monitors = Monitors,
           size = Size,
           supervisor = Sup,
           latched_size = LatchedSize} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            NextState = case Size > 0 of
                true  -> ready;
                false -> overflow
            end,
            Workers =
                case Size > LatchedSize of
                    true ->
                        ok = dismiss_worker(Sup, Pid),
                        State#state.workers;
                    false ->
                        queue:in(Pid, State#state.workers)
                end,
            {next_state, NextState, State#state{workers=Workers}};
        [] ->
            {next_state, overflow, State}
    end;
overflow({checkin, Pid}, State) ->
    #state{supervisor = Sup,
           monitors = Monitors,
           overflow = Overflow,
           size = Size,
           latched_size = LatchedSize} = State,
    {NextState, NewOverflow} =
        if Size > LatchedSize ->
                {full, Overflow};
           el/=se ->
                {overflow, Overflow - 1}
        end,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            ok = dismiss_worker(Sup, Pid),
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            {next_state, NextState, State#state{overflow = NewOverflow}};
        [] ->
            {next_state, NextState, State#state{overflow = NewOverflow}}
    end;
overflow(_Event, State) ->
    {next_state, overflow, State}.

overflow({checkout, Block, Timeout}, From,
         #state{overflow=Overflow,
                max_overflow=MaxOverflow}=State) when Overflow >= MaxOverflow ->
    case Block of
        true ->
            Waiting = add_waiting(From, Timeout, State#state.waiting),
            {next_state, full, State#state{waiting=Waiting}};
        false ->
            {reply, full, full, State}
    end;
overflow({checkout, _Block, _Timeout}, {From, _}, State) ->
    #state{supervisor = Sup,
           overflow = Overflow,
           max_overflow = MaxOverflow} = State,
    {Pid, Ref} = new_worker(Sup, From),
    true = ets:insert(State#state.monitors, {Pid, Ref}),
    NewOverflow = Overflow + 1,
    NextState = case NewOverflow >= MaxOverflow of
        true  -> full;
        false -> overflow
    end,
    {reply, Pid, NextState, State#state{overflow=NewOverflow}};
overflow(_Event, _From, State) ->
    {reply, ok, overflow, State}.

full({checkin, Pid}, State) ->
    #state{monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            checkin_while_full(Pid, State);
        [] ->
            {next_state, full, State}
    end;
full(_Event, State) ->
    {next_state, full, State}.

full({checkout, Block, Timeout}, {FromPid, _} = From, State) ->
    #state{size = Size,
           latched_size = LatchedSize,
           max_overflow = MaxOverflow,
           overflow = Overflow,
           monitors = Monitors,
           supervisor = Sup} = State,
    if Size + Overflow < LatchedSize + MaxOverflow ->
            {Pid, Ref} = new_worker(Sup, FromPid),
            true = ets:insert(Monitors, {Pid, Ref}),
            {NextState, NewOverflow, NewSize} =
                if Size < LatchedSize ->
                        {full, Overflow, Size + 1};
                   el/=se ->
                        {overflow, Overflow + 1, Size}
                end,
            {reply, Pid, NextState, State#state{size = NewSize,
                                                overflow = NewOverflow}};
       Block == true ->
            Waiting = add_waiting(From, Timeout, State#state.waiting),
            {next_state, full, State#state{waiting=Waiting}};
       el/=se ->
            {reply, full, full, State}
    end;
full(_Event, _From, State) ->
    {reply, ok, full, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName, State) ->
    {reply, {StateName, queue:len(State#state.workers), State#state.overflow,
             ets:info(State#state.monitors, size)},
     StateName, State};
handle_sync_event(status_ext, _From, StateName, State) ->
    {reply, {StateName, queue:len(State#state.workers), State#state.overflow,
             ets:info(State#state.monitors, size), State#state.size, State#state.latched_size},
     StateName, State};
handle_sync_event(get_avail_workers, _From, StateName, State) ->
    Workers = State#state.workers,
    WorkerList = queue:to_list(Workers),
    {reply, WorkerList, StateName, State};
handle_sync_event(get_all_workers, _From, StateName, State) ->
    Sup = State#state.supervisor,
    WorkerList = supervisor:which_children(Sup),
    {reply, WorkerList, StateName, State};
handle_sync_event(get_all_monitors, _From, StateName, State) ->
    Monitors = ets:tab2list(State#state.monitors),
    {reply, Monitors, StateName, State};
handle_sync_event(get_pool_size, _From, StateName, State) ->
    {reply, {State#state.size, State#state.latched_size, State#state.max_overflow}, StateName, State};
handle_sync_event({set_pool_size, NewSize}, _From, StateName, State) ->
    handle_sync_event({set_pool_size, NewSize, State#state.max_overflow}, _From, StateName, State);
handle_sync_event({set_pool_size, NewSize, NewMaxOverflow}, _From, StateName, State) ->
    %% minimize overflow
    SizeDiff = NewSize - State#state.size,
    MegaDiff = NewSize - (State#state.size + State#state.overflow),
    {SizeCorrected, OverflowCorrected} =
        if MegaDiff > 0 ->
                {State#state.size + State#state.overflow, 0};  %% with some slack
           SizeDiff > 0 ->
                {State#state.size + SizeDiff, State#state.overflow - SizeDiff};
           el/=se ->
                {State#state.size, State#state.overflow}
        end,
    {reply, ok, StateName, State#state{latched_size = NewSize,
                                       max_overflow = NewMaxOverflow,
                                       size = SizeCorrected,
                                       overflow = OverflowCorrected}};
handle_sync_event(stop, _From, _StateName, State) ->
    Sup = State#state.supervisor,
    true = exit(Sup, shutdown),
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = {error, invalid_message},
    {reply, Reply, StateName, State}.

handle_info({'DOWN', Ref, _, _, _}, StateName, State) ->
    case ets:match(State#state.monitors, {'$1', Ref}) of
        [[Pid]] ->
            Sup = State#state.supervisor,
            ok = supervisor:terminate_child(Sup, Pid),
            %% Don't wait for the EXIT message to come in.
            %% Deal with the worker exit right now to avoid
            %% a race condition with messages waiting in the
            %% mailbox.
            true = ets:delete(State#state.monitors, Pid),
            handle_worker_exit(Pid, StateName, State);
        [] ->
            {next_state, StateName, State}
    end;
handle_info({'EXIT', Pid, _Reason}, StateName, State) ->
    #state{supervisor = Sup,
           monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, Ref}] ->
            true = erlang:demonitor(Ref),
            true = ets:delete(Monitors, Pid),
            handle_worker_exit(Pid, StateName, State);
        [] ->
            case queue:member(Pid, State#state.workers) of
                true ->
                    W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
                    {next_state, StateName, State#state{workers=queue:in(new_worker(Sup), W)}};
                false ->
                    {next_state, StateName, State}
            end
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(shutdown, _StateName, #state{workers=Workers}) ->
    lists:foreach(fun (W) -> unlink(W) end, queue:to_list(Workers));
terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

start_pool(StartFun, PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            gen_fsm:StartFun(?MODULE, {PoolArgs, WorkerArgs}, []);
        Name ->
            gen_fsm:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
    end.

new_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    true = link(Pid),
    Pid.

new_worker(Sup, FromPid) ->
    Pid = new_worker(Sup),
    Ref = erlang:monitor(process, FromPid),
    {Pid, Ref}.

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

prepopulate(N, _Sup) when N < 1 ->
    queue:new();
prepopulate(N, Sup) ->
    prepopulate(N, Sup, queue:new()).

prepopulate(0, _Sup, Workers) ->
    Workers;
prepopulate(N, Sup, Workers) ->
    prepopulate(N-1, Sup, queue:in(new_worker(Sup), Workers)).

add_waiting(Pid, Timeout, Queue) ->
    queue:in({Pid, Timeout, os:timestamp()}, Queue).

wait_valid(infinity, _Timeout) ->
    true;
wait_valid(StartTime, Timeout) ->
    Waited = timer:now_diff(os:timestamp(), StartTime),
    (Waited div 1000) < Timeout.

checkin_while_full(Pid, State) ->
    #state{supervisor = Sup,
           waiting = Waiting,
           monitors = Monitors,
           workers = Workers,
           max_overflow = MaxOverflow,
           overflow = Overflow,
           size = Size,
           latched_size = LatchedSize} = State,
    case queue:out(Waiting) of
        {{value, {{FromPid, _}=From, Timeout, StartTime}}, Left} ->
            case wait_valid(StartTime, Timeout) of
                true ->
                    Ref1 = erlang:monitor(process, FromPid),
                    true = ets:insert(Monitors, {Pid, Ref1}),
                    gen_fsm:reply(From, Pid),
                    {next_state, full, State#state{waiting=Left}};
                false ->
                    checkin_while_full(Pid, State#state{waiting=Left})
            end;
        {empty, Empty} when MaxOverflow < 1 ->
            if Size > LatchedSize ->
                    ok = dismiss_worker(Sup, Pid),
                    {next_state, full, State#state{waiting = Empty,
                                                   size = Size - 1}};
               el/=se ->
                    {next_state, ready, State#state{workers = queue:in(Pid, Workers),
                                                    waiting = Empty}}
            end;
        {empty, Empty} ->
            {NextState, NewSize, NewOverflow, NewWorkers} =
                if Size > LatchedSize ->
                        ok = dismiss_worker(Sup, Pid),
                        {full, Size - 1, Overflow, Workers};
                   Overflow > 0 ->
                        ok = dismiss_worker(Sup, Pid),
                        {overflow, Size, Overflow - 1, Workers};
                   el/=se ->
                        {ready, Size, 0, queue:in(Pid, Workers)}
                end,
            {next_state, NextState, State#state{waiting = Empty,
                                                workers = NewWorkers,
                                                size = NewSize,
                                                overflow = NewOverflow}}
    end.

handle_worker_exit(Pid, StateName, State) ->
    #state{supervisor = Sup,
           overflow = Overflow,
           waiting = Waiting,
           monitors = Monitors,
           max_overflow = MaxOverflow} = State,
    case StateName of
        ready ->
            W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
            {next_state, ready, State#state{workers=queue:in(new_worker(Sup), W)}};
        overflow when Overflow =:= 0 ->
            W = queue:filter(fun (P) -> P =/= Pid end, State#state.workers),
            {next_state, ready, State#state{workers=queue:in(new_worker(Sup), W)}};
        overflow ->
            {next_state, overflow, State#state{overflow=Overflow-1}};
        full when MaxOverflow < 1 ->
            case queue:out(Waiting) of
                {{value, {{FromPid, _}=From, Timeout, StartTime}}, LeftWaiting} ->
                    case wait_valid(StartTime, Timeout) of
                        true ->
                            MonitorRef = erlang:monitor(process, FromPid),
                            NewWorker = new_worker(Sup),
                            true = ets:insert(Monitors, {NewWorker, MonitorRef}),
                            gen_fsm:reply(From, NewWorker),
                            {next_state, full, State#state{waiting=LeftWaiting}};
                        false ->
                            handle_worker_exit(Pid, StateName, State#state{waiting=LeftWaiting})
                    end;
                {empty, Empty} ->
                    Workers2 = queue:in(new_worker(Sup), State#state.workers),
                    {next_state, ready, State#state{waiting=Empty,
                                                    workers=Workers2}}
            end;
        full when Overflow =< MaxOverflow ->
            case queue:out(Waiting) of
                {{value, {{FromPid, _}=From, Timeout, StartTime}}, LeftWaiting} ->
                    case wait_valid(StartTime, Timeout) of
                        true ->
                            MonitorRef = erlang:monitor(process, FromPid),
                            NewWorker = new_worker(Sup),
                            true = ets:insert(Monitors, {NewWorker, MonitorRef}),
                            gen_fsm:reply(From, NewWorker),
                            {next_state, full, State#state{waiting=LeftWaiting}};
                        _ ->
                            handle_worker_exit(Pid, StateName, State#state{waiting=LeftWaiting})
                    end;
                {empty, Empty} ->
                    {next_state, overflow, State#state{overflow=Overflow-1,
                                                       waiting=Empty}}
            end;
        full ->
            {next_state, full, State#state{overflow=Overflow-1}}
    end.
