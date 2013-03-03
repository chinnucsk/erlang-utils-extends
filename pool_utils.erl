-module(pool_utils).
-export([call/2, call/3]).
-export([cast/2]).

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
    Worker = poolboy:checkout(Pool),
    Reply = gen_server:cast(Worker,Msg),
    poolboy:checkin(Pool,Worker),
    Reply.
