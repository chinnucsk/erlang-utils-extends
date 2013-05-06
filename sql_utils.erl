-module(sql_utils).

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
	{Fields, Values} = lists:unzip([{atom_to_list(F), sql_encode:encode(V,list)} 
		|| {F, V} <- Record]),
	List = ["INSERT INTO ", atom_to_list(Tab), " (",
		 string:join(Fields, ","), ") VALUES (",
		 string:join(Values, ","), ");"],
	lists:concat(List).

encode_insert(Tab, Fields, Rows) ->
	Rows1 = [ sql_encode:encode(Row) || Row <- Rows],
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
	lists:concat([atom_to_list(F), "=", sql_encode:encode(V,list)]).

encode_where({'and', L, R}) ->
	encode_where(L) ++ " AND " ++ encode_where(R);

encode_where({'and', List}) when is_list(List) ->
	string:join([encode_where(E) || E <- List], " AND ");

encode_where({'or', L, R}) ->
	encode_where(L) ++ " OR " ++ encode_where(R);

encode_where({'or', List}) when is_list(List) ->
	string:join([encode_where(E) || E <- List], " OR ");

encode_where({like, Field, Value}) ->	
	atom_to_list(Field) ++ " LIKE " ++ sql_encode:encode(Value,list);

encode_where({'<', Field, Value}) ->	
	atom_to_list(Field) ++ " < " ++ sql_encode:encode(Value,list);

encode_where({'>', Field, Value}) ->	
	atom_to_list(Field) ++ " > " ++ sql_encode:encode(Value,list);

encode_where({'in', Field, Values}) ->	
	InStr = string:join([sql_encode:encode(Value,list) || Value <- Values], ","),
	atom_to_list(Field) ++ " IN (" ++ InStr ++ ")";

encode_where({'<>',Field,Value})->
	atom_to_list(Field) ++ " <> " ++ sql_encode:encode(Value,list);

encode_where({Field, Value}) ->
	atom_to_list(Field) ++ " = " ++ sql_encode:encode(Value,list).



