-module(pool_utils).
-export([call/2,call/3]).
-export([cast/2,cast/3]).

call(Pool, Msg) ->
    Worker = poolboy:checkout(Pool),
    Reply = gen_server:call(Worker, Msg),
    poolboy:checkin(Pool, Worker),
    Reply.

call(Pool, Msg, Timeout) ->
    Worker = poolboy:checkout(Pool),
    Reply = gen_server:call(Worker, Msg, Timeout),
    poolboy:checkin(Pool, Worker),
    Reply.

cast(Pool,Msg)->
    cast(Pool,Msg,checkin).

cast(Pool,Msg,checkin)->
    Worker = poolboy:checkout(Pool),
    gen_server:cast(Worker,Msg),
    poolboy:checkin(Pool,Worker);
cast(Pool,Msg,not_checkin) ->
    Worker = poolboy:checkout(Pool),
    gen_server:cast(Worker,Msg);
cast(_Pool,_Msg,_) ->
    erlang:error(badarg).


    
