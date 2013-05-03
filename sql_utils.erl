-module(sql_utils).

-export([encode/1,encode/2,escape/1,escape_like/1]).
-export([quote/1,quote/2]).

-export([encode_select/1]).
-export([encode_insert/2,encode_insert/3]).
-export([encode_update/2,encode_update/3]).
-export([encode_delete/1,encode_delete/2]).

encode_select(Tab) when is_atom(Tab) ->
	encode_select({Tab, ['*'], undefined});

encode_select({Tab, Fields}) when is_atom(Tab) and is_list(Fields) ->
    encode_select({Tab, Fields, undefined});

encode_select({Tab, Where}) when is_atom(Tab) and is_tuple(Where) ->
	encode_select({Tab, ['*'], Where});

encode_select({Tab, Fields, undefined}) when is_atom(Tab) and is_list(Fields) ->
	List = ["SELECT ", encode_fields(Fields), " FROM ", atom_to_list(Tab), ";"],
	lists:concat(List);

encode_select({Tab, Fields, Where}) when is_atom(Tab) 
	and is_list(Fields) and is_tuple(Where) ->
	List = ["SELECT ", encode_fields(Fields), " FROM ",
	 atom_to_list(Tab), " WHERE ", encode_where(Where), ";"],
	lists:concat(List).


encode_insert(Tab, Record) ->
	{Fields, Values} = lists:unzip([{atom_to_list(F), encode(V)} 
		|| {F, V} <- Record]),
	List = ["INSERT INTO ", atom_to_list(Tab), " (",
		 string:join(Fields, ","), ") VALUES (",
		 string:join(Values, ","), ");"],
	lists:concat(List).

encode_insert(Tab, Fields, Rows) ->
	Rows1 = [ encode(Row) || Row <- Rows],
	List = ["INSERT INTO ", atom_to_list(Tab), " (",
		string:join([atom_to_list(F) || F <- Fields], ","), 
		") VALUES (", string:join(Rows1, ","), ");"],
	lists:concat(List).


encode_update(Tab, Record) when is_atom(Tab) and is_list(Record) ->
	case proplists:get_value(id, Record) of 
    	undefined ->
			Updates = string:join([encode_column(Col) || Col <- Record], ","),
			List = ["UPDATE ", atom_to_list(Tab), " SET ", Updates, ";"],
			lists:concat(List);
    	Id ->
        	encode_update(Tab, lists:keydelete(id, 1, Record), {id, Id})
	end.

encode_update(Tab, Record, Where) ->
	Update = string:join([encode_column(Col) || Col <- Record], ","),
    List = ["UPDATE ", atom_to_list(Tab), " SET ", Update,
		" WHERE ", encode_where(Where), ";"],
	lists:concat(List).

encode_delete(Tab) when is_atom(Tab) ->
	List = ["DELETE FROM ", atom_to_list(Tab), ";"],
	lists:concat(List).

encode_delete(Tab, Id) when is_atom(Tab)and is_integer(Id) ->
    List = ["DELETE FROM ", atom_to_list(Tab), 
			 " WHERE ", encode_where({id, Id})],
	lists:concat(List);

encode_delete(Tab, Where) when is_atom(Tab) and is_tuple(Where) ->
    List = ["DELETE FROM ", atom_to_list(Tab),
			 " WHERE ", encode_where(Where)],
	lists:concat(List).


encode_fields(Fields) ->
    string:join([atom_to_list(F) || F <- Fields], " ,").

encode_column({F, V}) when is_atom(F) ->
	lists:concat([atom_to_list(F), "=", encode(V)]).

encode_where({'and', L, R}) ->
	encode_where(L) ++ " AND " ++ encode_where(R);

encode_where({'and', List}) when is_list(List) ->
	string:join([encode_where(E) || E <- List], " AND ");

encode_where({'or', L, R}) ->
	encode_where(L) ++ " OR " ++ encode_where(R);

encode_where({'or', List}) when is_list(List) ->
	string:join([encode_where(E) || E <- List], " OR ");

encode_where({like, Field, Value}) ->	
	atom_to_list(Field) ++ " LIKE " ++ encode(Value);

encode_where({'<', Field, Value}) ->	
	atom_to_list(Field) ++ " < " ++ encode(Value);

encode_where({'>', Field, Value}) ->	
	atom_to_list(Field) ++ " > " ++ encode(Value);

encode_where({'in', Field, Values}) ->	
	InStr = string:join([encode(Value) || Value <- Values], ","),
	atom_to_list(Field) ++ " IN (" ++ InStr ++ ")";

encode_where({'<>',Field,Values})->
	atom_to_list(Field) ++ " <> " ++ encode(Value);

encode_where({Field, Value}) ->
	atom_to_list(Field) ++ " = " ++ encode(Value).



%% Escape character that will confuse an SQL engine
%% Percent and underscore only need to be escaped for pattern matching like
%% statement
escape_like(S) when is_list(S) ->
    [escape_like(C) || C <- S];
escape_like($%) -> "\\%";
escape_like($_) -> "\\_";
escape_like(C)  -> escape(C).

%% Escape character that will confuse an SQL engine
escape(S) when is_list(S) ->
	[escape(C) || C <- S];
%% Characters to escape
escape($\0) -> "\\0";
escape($\n) -> "\\n";
escape($\t) -> "\\t";
escape($\b) -> "\\b";
escape($\r) -> "\\r";
escape($')  -> "\\'";
escape($")  -> "\\\"";
escape($\\) -> "\\\\";
escape(C)   -> C.


encode(Val) ->
    encode(Val, list).

encode(Val, ReturnType) when is_atom(Val)->
	encode(atom_to_list(Val), ReturnType, latin1);
encode(Val,ReturnType)->
	encode(Val, ReturnType, utf8).

encode(null, list, _)-> 
	"NULL";
encode(undefined, list, _)-> 
	"NULL";
encode(null, binary, _)->
	<<"NULL">>;
encode(undefined, binary, _)->
	<<"NULL">>;

encode(Val, list, latin1) when is_binary(Val) ->
	quote(binary_to_list(Val));
encode(Val, list, Encoding) when is_binary(Val) ->
	quote(unicode:characters_to_list(Val, Encoding));
encode(Val, list, _) when is_list(Val) ->
	quote(Val);
encode(Val, list, _) when is_integer(Val) ->
	integer_to_list(Val);
encode(Val, list, _) when is_float(Val) ->
	[Res] = io_lib:format("~w", [Val]),
	Res;

encode(Val, binary, latin1) when is_list(Val) -> 
	list_to_binary(quote(Val));
encode(Val, binary, Encoding) when is_list(Val) ->
	unicode:characters_to_binary(quote(Val), Encoding, Encoding);
encode(Val, binary, latin1) when is_binary(Val) ->
	quote(Val, latin1);
encode(Val, binary, Encoding) when is_binary(Val) ->
	quote(Val, Encoding);
encode(Val, binary, _) when is_integer(Val) ->
	list_to_binary(integer_to_list(Val));
encode(Val, binary, _) when is_float(Val) ->
	[Res] = io_lib:format("~w", [Val]),
	erlang:list_to_binary(Res);

encode({datetime, Val}, ReturnType, Encoding) ->
	encode(Val, ReturnType, Encoding);

encode({date, Val}, ReturnType, Encoding) ->
	encode(Val, ReturnType, Encoding);

encode({time, Val}, ReturnType, Encoding) ->
	encode(Val, ReturnType, Encoding);

encode({{Year, Month, Day}, {Hour, Minute, Second}}, list, _) ->
	Res = two_digits([Year, Month, Day, Hour, Minute, Second]),
	lists:flatten(Res);

encode({{_Year, _Month, _Day}, {_Hour, _Minute, _Second}}=Val, binary, E) ->
	list_to_binary(encode(Val, list, E));

encode({Time1, Time2, Time3}, list, _) ->
	Res = two_digits([Time1, Time2, Time3]),
	lists:flatten(Res);

encode({_Time1, _Time2, _Time3}=Val, binary, E) ->
	list_to_binary(encode(Val, list, E));

encode(Val, _, _) ->
	{error, {unrecognized_value, Val}}.

two_digits(Nums) when is_list(Nums) ->
    [two_digits(Num) || Num <- Nums];
two_digits(Num) ->
    [Str] = io_lib:format("~b", [Num]),
    case length(Str) of
	1 -> [$0 | Str];
	_ -> Str
    end.

%%  Quote a string or binary value so that it can be included safely in a
%%  MySQL query.

quote(String) when is_list(String) ->
    [39 | lists:reverse([39 | quote_loop(String)])];	%% 39 is $'

quote(Bin) when is_binary(Bin) ->
    quote(Bin,utf8).

quote(Bin, latin1) when is_binary(Bin) ->
    list_to_binary(quote(binary_to_list(Bin)));

quote(Bin, Encoding) when is_binary(Bin) ->
    case unicode:characters_to_list(Bin,Encoding) of
	{error,E1,E2} -> 
	    exit({invalid_encoding_binary, E1, E2});
	{incomplete,E1,E2} ->
	    exit({invalid_encoding_binary, E1, E2});
	List ->
	    unicode:characters_to_binary(quote(List),Encoding,Encoding)    	
    end.

quote_loop(List) ->
	quote_loop(List, []).

quote_loop([], Acc) ->
	Acc;
quote_loop([0 | Rest], Acc) ->
	quote_loop(Rest, [$0, $\\ | Acc]);
quote_loop([10 | Rest], Acc) ->
	quote_loop(Rest, [$n, $\\ | Acc]);
quote_loop([13 | Rest], Acc) ->
	quote_loop(Rest, [$r, $\\ | Acc]);
quote_loop([$\\ | Rest], Acc) ->
	quote_loop(Rest, [$\\ , $\\ | Acc]);
quote_loop([39 | Rest], Acc) -> %% 39 is $'
	quote_loop(Rest, [39, $\\ | Acc]); %% 39 is $'
quote_loop([34 | Rest], Acc) -> %% 34 is $"
	quote_loop(Rest, [34, $\\ | Acc]); %% 34 is $"
quote_loop([26 | Rest], Acc) ->
	quote_loop(Rest, [$Z, $\\ | Acc]);
quote_loop([C | Rest], Acc) ->
	quote_loop(Rest, [C | Acc]).
