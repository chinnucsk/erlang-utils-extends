-module(emysql_extends).
-export([load_all_mysql/2]).
-export([execute/3]).
-export([transfer_datetime/1]).

load_all_mysql(AppName,EnvName)->
    DBs = hm_misc:get_env(AppName,EnvName,[]),
    add_dbs_to_pool(DBs).

add_dbs_to_pool([])->
    ok;

add_dbs_to_pool([H|T])->
    {PoolName,PoolArgs} = H, 
    PoolSize = proplists:get_value(size,PoolArgs,10),
    Username = proplists:get_value(username,PoolArgs,"root"),
    Password = proplists:get_value(password,PoolArgs,""),
    Host = proplists:get_value(host,PoolArgs,"localhost"),
    Port = proplists:get_value(port,PoolArgs,3306),
    PoolNameString = erlang:atom_to_list(PoolName),
    DBName = proplists:get_value(db_name,PoolArgs,PoolNameString),
    Charset = proplists:get_value(charset,PoolArgs,utf8),
    emysql:add_pool(PoolName,PoolSize,
		    Username,Password, Host, Port,
		    DBName, Charset),
    add_dbs_to_pool(T).

execute({pool,Pool},SQL,Fun)->
    SQLBin = hm_converter:term_to_iolist(SQL),
    lager:log(info,sql,"SQL:~p without transaction~n",[SQLBin]),
    Packet = emysql:execute(Pool,SQLBin),
    Fun(Packet);

execute({conn,Connection},SQL,Fun)->
    SQLBin = hm_converter:term_to_iolist(SQL),
    lager:log(info,sql,"SQL:~p with transaction~n",[SQLBin]),
    Packet = emysql_conn:execute(Connection,SQLBin,[]),
    Fun(Packet).


transfer_datetime(DateTime)->
    case DateTime of
        undefined ->
            undefined;
        _->
            Result = sql_encode:encode(DateTime),
            case Result of
                {error,_}->
                    DateTime;
                _ ->
                    Result
            end
    end.