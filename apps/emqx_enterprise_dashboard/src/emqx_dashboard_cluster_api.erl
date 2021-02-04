%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_cluster_api).

-include("emqx_dashboard.hrl").

-import(minirest, [return/1]).

-rest_api(#{ name   => cluster_info
           , method => 'GET'
           , path   => "/cluster"
           , func   => cluster_info
           , descr  => "Get a Cluster info"
           }).

-rest_api(#{ name   => invite_node
           , method => 'POST'
           , path   => "/cluster/invite_node"
           , func   => invite_node
           , descr  => "Invite a node to join the cluster"
           }).

-rest_api(#{ name   => force_leave
           , method => 'DELETE'
           , path   => "/cluster/force_leave/:bin:node"
           , func   => force_leave
           , descr  => "Force a node to leave the cluster"
           }).

-export([ cluster_info/2
        , invite_node/2
        , force_leave/2
        ]).

cluster_info(_Bindings, _Params) ->
    ClusterName = application:get_env(ekka, cluster_name, emqxcl),
    {Type, ClusterConf} = application:get_env(ekka, cluster_discovery, {manual,[]}),
    Info = [{name, ClusterName},
            {type, Type},
            {config, format(ClusterConf)}],
    return({ok, Info}).

invite_node(_Bindings, Params) ->
    Node = proplists:get_value(<<"node">>, Params),
    case rpc:call(ekka_node:parse_name(binary_to_list(Node)), ekka, join, [node()]) of
        ok -> return(ok);
        ignore -> return({error, <<"Not invite self">>});
        {badrpc, Error} -> return({error, Error});
        {error, Error} -> return({error, iolist_to_binary(io_lib:format("~0p", [Error]))})
    end.

force_leave(#{node := Node}, _Params) ->
    case ekka:force_leave(ekka_node:parse_name(binary_to_list(Node))) of
        ok -> return(ok);
        ignore -> return({error, <<"Not force leave self">>});
        {error, Error} -> return({error, iolist_to_binary(io_lib:format("~0p", [Error]))})
    end.


format(ClusterConf) ->
    lists:reverse(format(ClusterConf, [])).

format([], Acc)->
    Acc;
format([{ssl_options, Val} | Conf], Acc)->
    format(Conf, [{ssl_options, format(Val, [])}| Acc]);
format([{Key, Val} | Conf], Acc) when Key =:= seeds orelse Key =:= ports ->
    format(Conf, [{Key, Val}| Acc]);
format([{Key, Val} | Conf], Acc) when Key =:= addr orelse Key =:= iface ->
    format(Conf, [{Key, list_to_binary(esockd_net:ntoa(Val))}| Acc]);
format([{Key, Val} | Conf], Acc) when is_list(Val) ->
    format(Conf, [{Key, list_to_binary(Val)}| Acc]);
format([{Key, Val} | Conf], Acc) ->
    format(Conf, [{Key, Val}| Acc]).
    