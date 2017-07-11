-module(cache_db).
-author("regupathy.b").

-include("cache_db_records.hrl").

%% API

-define(Incomplete_query,incomplete_db_query).

-export([execute/1]).
-export([write/2,read/2,read/3,write_new/3,update/3,update_counter/3,delete/2,replace_primary/3]).
-export([read_query/1,read_more/2,read_more/3]).
-export([change_lock_kind/2,change_transaction/2]).

%% Helper module for mnesia database
%% Library will be easy to access mnesia database without knowing expect API methods of it
%% Developer knows basic database knowledge they can use it

%%%===================================================================
%%% Cache DB API methods
%%%===================================================================

read_query(QLC_Query) ->
  #db_query{operation = #db_operation.read_query,table = no_need,db_params = #db_params{qlc_query = QLC_Query}}.

read_more(Table,PrimaryKeyList) -> #db_query{operation = #db_operation.many_read,table = Table,lock_kind = #lock_kind.read,
  db_params = #db_params{primary_key = PrimaryKeyList}}.

read_more(Table,IndexKey, IndexVals) when is_list(IndexVals) -> #db_query{operation = #db_operation.many_index_read,table = Table,
  db_params = #db_params{index_key = IndexKey,index_value = IndexVals}}.

read(Table,PrimaryKey) -> #db_query{operation = #db_operation.read,table = Table,lock_kind = #lock_kind.read,
  db_params = #db_params{primary_key = PrimaryKey}}.

read(Table,IndexKey,IndexVal) -> #db_query{operation = #db_operation.index_read,table = Table,
  db_params = #db_params{index_key = IndexKey,index_value = IndexVal}}.

write(Table,Record) -> #db_query{operation = #db_operation.write,table = Table,
  db_params = #db_params{record = Record}}.

write_new(Table,PrimaryKey,Record) -> #db_query{operation = #db_operation.write_new,table = Table,
  db_params = #db_params{record = Record,primary_key = PrimaryKey}}.

update(Table,PrimaryKey,UpdateFun)when is_function(UpdateFun) ->
  #db_query{operation = #db_operation.update,lock_kind = #lock_kind.write,table = Table,
    db_params = #db_params{primary_key = PrimaryKey,func = UpdateFun}}.

update_counter(Table,Key,Count)  -> #db_query{operation = #db_operation.update_counter,table = Table,
  db_params = #db_params{primary_key = Key,count = Count}}.

replace_primary(Table,PrimaryKey,UpdateFun) -> #db_query{operation = #db_operation.replace_primary,table = Table,
  db_params = #db_params{primary_key = PrimaryKey,func = UpdateFun}}.

delete(Table,PrimaryKey) -> #db_query{table = Table,operation = #db_operation.delete,
  lock_kind = #lock_kind.sticky_write,db_params = #db_params{primary_key = PrimaryKey}}.

change_lock_kind(#db_query{} = Q,#lock_kind.read) -> Q#db_query{lock_kind = #lock_kind.read};
change_lock_kind(#db_query{} = Q,#lock_kind.write) -> Q#db_query{lock_kind = #lock_kind.write};
change_lock_kind(#db_query{} = Q,#lock_kind.sticky_write) -> Q#db_query{lock_kind = #lock_kind.sticky_write};
change_lock_kind(#db_query{} = Q,_) -> Q.

change_transaction(#db_transaction.async,#db_query{} = Q) -> Q#db_query{transaction = #db_transaction.async};
change_transaction(#db_transaction.async_dirty,#db_query{} = Q) -> Q#db_query{transaction = #db_transaction.async_dirty};
change_transaction(#db_transaction.sync,#db_query{} = Q) -> Q#db_query{transaction = #db_transaction.sync};
change_transaction(#db_transaction.sync_dirty,#db_query{} = Q) -> Q#db_query{transaction = #db_transaction.sync_dirty};
change_transaction(#db_transaction.ets,#db_query{} = Q) -> Q#db_query{transaction = #db_transaction.ets};
change_transaction(Q,_) -> Q.

execute(#db_query{} = Q ) -> query(Q).

%%%===================================================================
%%% Mnesia Query Management
%%%===================================================================

query(#db_query{operation = undefined} =_) -> ?Incomplete_query;
query(#db_query{table = undefined} =_) -> ?Incomplete_query;
query(#db_query{db_params = undefined} = _) -> ?Incomplete_query;
query(#db_query{operation = #db_operation.read,table = Tab,transaction = Trans,lock_kind = LK,
  db_params = #db_params{primary_key = Key}} =_) ->
  C = conform_params([Key]),
  transaction(C,Trans,fun() -> mnesia:read(Tab,Key,set_lock_kind(LK)) end);

query(#db_query{operation = #db_operation.many_read,table = Tab,transaction = Trans,lock_kind = LK,
  db_params = #db_params{primary_key = Keys}} =_) ->
  C = conform_params([Keys]),
  transaction(C,Trans,fun() -> [mnesia:read(Tab,Key,set_lock_kind(LK)) || Key <- Keys] end);

query(#db_query{operation = #db_operation.index_read,table = Tab,transaction = Trans,
  db_params = #db_params{index_key = Position,index_value = Val}}) ->
  C = conform_params([Position,Val]),
  transaction(C,Trans,fun() -> mnesia:index_read(Tab,Val,Position) end);

query(#db_query{operation = #db_operation.many_index_read,table = Tab,transaction = Trans,
  db_params = #db_params{index_key = Position,index_value = Vals}}) ->
  C = conform_params([Position,Vals]),
  transaction(C,Trans,fun() -> [mnesia:index_read(Tab,Val,Position) || Val <- Vals]  end);

query(#db_query{operation = #db_operation.write,table = Tab,transaction = Trans,lock_kind = LK,
  db_params = #db_params{record = Record}}) ->
  C = conform_params([Record]),
  transaction(C,Trans,fun() -> mnesia:write(Tab,Record,set_lock_kind(LK)) end);

query(#db_query{operation = #db_operation.write_new,table = Tab,transaction = Trans,lock_kind = LK,
  db_params = #db_params{record = Record,primary_key = PrimaryKey}}) ->
  C = conform_params([Record,PrimaryKey]),
  transaction(C,Trans,
    fun() ->
      case mnesia:read(Tab,PrimaryKey) of
        [] ->  mnesia:write(Tab,Record,set_lock_kind(LK));
        Rec -> {duplicate,Rec}
      end
    end);

query(#db_query{operation = #db_operation.read_query,transaction = Trans,
  db_params = #db_params{qlc_query = QLc}} = _) ->
  C = conform_params([QLc]),
  transaction(C,Trans,fun() -> qlc:e(QLc) end);

query(#db_query{operation = #db_operation.delete,table = Tab,transaction = Trans,lock_kind = LK,
  db_params = #db_params{primary_key = Key}} = _) ->
  C = conform_params([Key]),
  transaction(C,Trans,fun() -> mnesia:delete(Tab,Key,set_lock_kind(LK)) end);

query(#db_query{operation = #db_operation.update,table = Tab, transaction = Trans,lock_kind = Lk,
  db_params = #db_params{primary_key = PrimaryKey,func = UpdateFun}} = _) when is_function(UpdateFun) ->
  C = conform_params([PrimaryKey]),
  transaction(C,Trans,
    fun() ->
      case mnesia:read(Tab,PrimaryKey,set_lock_kind(Lk)) of
        [Record] -> NewRecord = UpdateFun(Record),
          mnesia:write(NewRecord);
        _ -> empty_record
      end
    end);

query(#db_query{operation = #db_operation.replace_primary,table = Tab, transaction = Trans,lock_kind = Lk,
  db_params = #db_params{primary_key = PrimaryKey,func = UpdateFun}} = _) when is_function(UpdateFun) ->
  C = conform_params([PrimaryKey]),
  transaction(C,Trans,
    fun() ->
      case mnesia:read(Tab,PrimaryKey,set_lock_kind(Lk)) of
        [Record] -> NewRecord = UpdateFun(Record),
          mnesia:delete(Record),
          mnesia:write(NewRecord);
        _ -> empty_record
      end
    end);

query(#db_query{operation = #db_operation.update_counter,table = Tab,
  db_params = #db_params{primary_key = Key,count = Count}} = _) ->
  case conform_params([Key]) of
    yes ->   mnesia:dirty_update_counter(Tab, Key, Count);
    no -> ?Incomplete_query
  end;

query(_) -> ?Incomplete_query.

transaction(yes,Transaction,Fun) -> transaction(Transaction,Fun);
transaction(no,_,_) -> ?Incomplete_query.

transaction(#db_transaction.async,Fun) ->
  mnesia:activity(transaction,Fun);
transaction(#db_transaction.sync,Fun) ->
  mnesia:activity(sync_transaction,Fun);
transaction(#db_transaction.async_dirty,Fun) ->
  mnesia:activity(async_dirty,Fun);
transaction(#db_transaction.sync_dirty,Fun) ->
  mnesia:activity(sync_dirty,Fun);
transaction(#db_transaction.ets,Fun) ->
  mnesia:activity(ets,Fun).

conform_params([]) -> yes;
conform_params([undefined|_]) -> no;
conform_params([_|T]) -> conform_params(T).

set_lock_kind(#lock_kind.read) -> read;
set_lock_kind(#lock_kind.write) -> write;
set_lock_kind(#lock_kind.sticky_write) -> sticky_write.
