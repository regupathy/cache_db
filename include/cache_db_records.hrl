%%%-------------------------------------------------------------------
%%% @author tringapps
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Nov 2016 4:39 PM
%%%-------------------------------------------------------------------


-record(db_transaction,{sync,async,sync_dirty,async_dirty,ets}).

-record(lock_kind,{read,write,sticky_write}).

-record(db_query,{transaction = #db_transaction.async,table,operation,lock_kind = #lock_kind.write,db_params}).

-record(db_params,{primary_key,index_key,index_value,record,func,ms,count = 1,qlc_query}).

-record(db_operation,{read_query,read, many_read,delete,write_new,write,index_read,many_index_read,
  update,update_counter,replace_primary}).


