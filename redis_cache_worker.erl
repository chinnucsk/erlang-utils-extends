%%%-------------------------------------------------------------------
%%% @author David Alpha Fox <>
%%% @copyright (C) 2013, David Alpha Fox
%%% @doc
%%%
%%% @end
%%% Created :  5 Jun 2013 by David Alpha Fox <>
%%%-------------------------------------------------------------------
-module(redis_cache_worker).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
		 terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {
		  redis_client,
		  options
		 }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Options) ->
	gen_server:start_link(?MODULE, Options, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Options) ->
	Config = get_adapter_config(Options),
	Result = eredis:start_link(Config),
	case Result of
		{ok,RedisClient}->
			State = #state{
					   redis_client = RedisClient,
					   options = Options
					  },
			{ok, State};
		 {error,Reason}	 ->
			{stop,Reason}
	end.
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%-------------------------------------------------------------------
handle_call({sismember,Prefix,Key,Item},_From,State)->
	Reply = command_sismember({Prefix,Key,Item}),
	{reply,Reply,State};
handle_call({spop,Prefix,Key},_From,State)->
	Reply = command_spop({Prefix,Key,Item},State),
	{reply,Reply,State};
handle_call({sadd,Prefix,Key,Item},_From,State)->
	Reply = command_sadd({Prefix,Key,Item},Satet),
	{reply,Reply,State};
handle_call({srem,Prefix,Key,Item},_From,State)->
	Reply = command_srem({Prefix,Key,Item},Satet),
	{reply,Reply,State};
handle_call({set,Prefix,Key,Val},_From,State)->
	Reply = command_set({Prefix,Key,Val},State),
	{reply,Reply,State};
handle_call({hset,Prefix,Key,Val},_From,State)->
	Reply = command_hset({Prefix,Key,Val},State),
	{reply,Reply,State};
handle_call({get,Prefix,Key},_From,State)->
	Reply = command_get({Prefix,Key},State),
	{reply,Reply,State};
handle_call({hget,Prefix,Key,Field},_From,State)->
	Reply = command_hget({Prefix,Key,Field},State),
	{reply,Reply,State};
handle_call({hgetall,Prefix,Key},_From,State)->
	Reply = command_hgetall({Prefix,Key},State),
	{reply,Reply,State};
handle_call({exists,Prefix,Key},_From,State)->
	Reply = command_exists({Prefix,Key},State),
	{reply,Reply,State};
handle_call({hmset,Prefix,Key,Val},_From,State)->
	Reply = command_hmset({Prefix,Key,Val},State),
	{reply,Reply,State};
handle_call(_Request, _From, State) ->
	{reply, {error,unknown_msg}, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({set,Prefix,Key,Val},State)->
	command_set({Prefix,Key,Val},State),
	{noreply, State};
handle_cast({hset,Prefix,Key,Val},State)->
	command_hset({Prefix,Key,Val},State),
	{noreply,State};
handle_cast({del,Prefix,Key},State)->
	command_del({Prefix,Key},State),
	{noreply,State};
handle_cast({hmset,Prefix,Key,Val},State)->
	command_hmset({Prefix,Key,Val},State),
	{noreply,State};
handle_cast(_Msg, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{redis_client = RedisClient}) ->
	eredis:stop(RedisClient),
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
to_kvlist(TupleList)->
	FoldFun = fun(Item,AccIn)->
					  Tuple = erlang:is_tuple(Item),
					  case Tuple of
						  true ->
							  {Key,Val} = Item,
							  KeyBin = hm_converter:term_to_iolist(Key),
							  ValBin = hm_converter:term_to_iolist(Val),
							  [KeyBin,ValBin | AccIn];
						  _ ->
							  AccIn
					  end
			  end,
	lists:foldl(FoldFun,[],TupleList).
					
get_adapter_config(Options)->
	AdapterConfig = [
					 {host,"127.0.0.1"},
					 {port,6379},
					 {database,0},
					 {password,""},
					 {reconnect_sleep,100}],
	proplists:get_value(adapter_config,Options,AdapterConfig).

get_cache_key(Prefix,Key) when erlang:is_binary(Prefix) and
							   erlang:is_binary(Key) ->
	CacheKeyBin = <<Prefix/bitstring,<<":">>/bitstring,Key/bitstring>>,
	CacheKeyBin;
get_cache_key(Prefix,Key) when not erlang:is_binary(Prefix)->
	PrefixBin = hm_converter:term_to_iolist(Prefix),
	get_cache_key(PrefixBin,Key);
get_cache_key(Prefix,Key) when not erlang:is_binary(Key) ->
	KeyBin = hm_converter:term_to_iolist(Key),
	get_cache_key(Prefix,KeyBin).
   


command_set({Prefix,Key,Val},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	ValBin = hm_converter:term_to_iolist(Val),
	Result = eredis:q(RedisClient,["SET",CacheKey,ValBin]),
	lager:log(debug,redis_cache_worker,"SET:(~p,~p) Result:~p~n",[CacheKey,Val,Result]),
	Result.

command_hset({Prefix,Key,Val},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	Tuple = erlang:is_tuple(Val),
	R = case Tuple of
			true ->
				{FieldKey,FieldVal} = Val,
				FieldKeyBin = hm_converter:term_to_iolist(FieldKey),
				FieldValBin = hm_converter:term_to_iolist(FieldVal),
				Result =  eredis:q(RedisClient,["HSET",CacheKey,FieldKeyBin,FieldValBin]),
				Result;
			false ->
				{error,not_a_tuple}
		end,
	lager:log(debug,redis_cache_worker,"HSET:(~p,~p) Result:~p~n",[CacheKey,Val,R]),
	R.

command_hmset({Prefix,Key,Val},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	List = erlang:is_list(Val),
	R = case List of
			true ->
				KVList = to_kvlist(Val),
				BaseList = ["HMSET",CacheKey],
				case KVList of
					[] ->
						{error,no_data};
					_-> 
						Command = BaseList ++ KVList,
						Result = eredis:q(RedisClient,Command),
						Result
				end;
			_->
				{error,not_tuple_list}
		end,
	lager:log(debug,redis_cache_worker,"HMSET:(~p,~p) Result:~p~n",[CacheKey,Val,R]),
	R.

command_get({Prefix,Key},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	Result = eredis:q(RedisClient,["GET",CacheKey]),
	lager:log(debug,redis_cache_worker,"GET:~p Result:~p~n",[CacheKey,Result]),
	Result.

command_hget({Prefix,Key,Field},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	FieldBin = hm_converter:term_to_iolist(Field),
	Result  = eredis:q(RedisClient,["HGET",CacheKey,FieldBin]),
	lager:log(debug,redis_cache_worker,"HGET:(~p,~p) Result:~p~n",[CacheKey,FieldBin,Result]),
	Result.

command_hgetall({Prefix,Key},#state{redis_client = RedisClient})->					   
	CacheKey = get_cache_key(Prefix,Key),
	Result = eredis:q(RedisClient,["HGETALL",CacheKey]),
	lager:log(debug,redis_cache_worker,"HGETALL:~p Result:~p~n",[CacheKey,Result]),
	Result.

command_del({Prefix,Key},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	Result = eredis:q(RedisClient,["DEL",CacheKey]),
	lager:log(debug,redis_cache_worker,"DEL:~p Result:~p~n",[CacheKey,Result]),
	Result.

command_exists({Prefix,Key},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	Result = eredis:q(RedisClient,["EXISTS",CacheKey]),
	lager:log(debug,redis_cache_worker,"EXISTS:~p Result:~p~n",[CacheKey,Result]),
	Result.
command_sadd({Prefix,Key,Item},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	Result = eredis:q(RedisClient,["SADD",CacheKey,Item]),
	lager:log(debug,redis_cache_worker,"SADD:~p Result:~p~n",[CacheKey,Result]),
	Result.
command_srem({Prefix,Key,Item},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	Result = eredis:q(RedisClient,["SREM",CacheKey,Item]),
	lager:log(debug,redis_cache_worker,"SREM:~p Result:~p~n",[CacheKey,Result]),
	Result.
command_spop({Prefix,Key},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	Result = eredis:q(RedisClient,["SPOP",CacheKey]),
	lager:log(debug,redis_cache_worker,"SPOP:~p Result:~p~n",[CacheKey,Result]),
	Result.
command_sismember({Prefix,Key,Item},#state{redis_client = RedisClient})->
	CacheKey = get_cache_key(Prefix,Key),
	Result = eredis:q(RedisClient,["SISMEMBER",CacheKey,Item]),
	lager:log(debug,redis_cache_worker,"SISMEMBER:~p Result:~p~n",[CacheKey,Result]),
	Result.