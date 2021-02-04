%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_monitor_api).

-include("emqx_dashboard.hrl").

-import(minirest, [return/1]).

-rest_api(#{ name   => current_metrics
           , method => 'GET'
           , path   => "monitor/current_metrics"
           , func   => current_metrics
           , descr  => "Get a current metric"
           }).

-rest_api(#{ name   => node_metrics
           , method => 'GET'
           , path   => "nodes/:atom:node/monitor/metrics"
           , func   => metrics
           , descr  => "Get History metrics"
           }).

-rest_api(#{ name   => lookup_node_metrics
           , method => 'GET'
           , path   => "nodes/:atom:node/monitor/metrics/:atom:type"
           , func   => lookup_metrics
           , descr  => "Get a History metrics"
           }).

-rest_api(#{ name   => metrics
           , method => 'GET'
           , path   => "monitor/metrics"
           , func   => metrics
           , descr  => "Get History metrics"
           }).

-rest_api(#{ name   => lookup_metrics
           , method => 'GET'
           , path   => "monitor/metrics/:atom:type"
           , func   => lookup_metrics
           , descr  => "Get a History metrics"
           }).

-export([ current_metrics/2
        , metrics/2
        , lookup_metrics/2
        ]).

-export([ sampling/1
        , sampling/2
        , get_collect/1
        ]).

current_metrics(_Bindings, _Params) ->
    Data = [get_collect(Node) || Node <- ekka_mnesia:running_nodes()],
    NodeSize = length(ekka_mnesia:running_nodes()),
    {Received, Sent, Sub, Conn} = format_current_metrics(Data),
    Info = [{node, NodeSize},
            {received, Received},
            {sent, Sent},
            {subscription, Sub},
            {connection, Conn}],
    return({ok, Info}).

metrics(#{node := Node}, _Params) ->
    return({ok, sampling(Node)});

metrics(_Bindings, _Params) ->
    Data = [{Node, sampling(Node)}  || Node <- ekka_mnesia:running_nodes()],
    return({ok, Data}).

lookup_metrics(#{node := Node, type := Type}, _Params) ->
    return({ok, sampling(Node, Type)});

lookup_metrics(#{type := Type}, _Params) ->
    Data = [{Node, sampling(Node, Type)}  || Node <- ekka_mnesia:running_nodes()],
    return({ok, Data}).

format_current_metrics(Collects) ->
    format_current_metrics(Collects, {0,0,0,0}).
format_current_metrics([], Acc) ->
    Acc;
format_current_metrics([{Received, Sent, Sub, Conn} | Collects], {Received1, Sent1, Sub1, Conn1}) ->
    format_current_metrics(Collects, {Received1 + Received, Sent1 + Sent, Sub1 + Sub, Conn1 + Conn}).

get_collect(Node) when Node =:= node() ->
    emqx_dashboard_collection:get_collect();
get_collect(Node) ->
    case rpc:call(Node, emqx_dashboard_collection, get_collect, []) of
        {badrpc, _Reason} -> {0, 0, 0, 0};
        Res -> Res
    end.

sampling(Node) when Node =:= node() ->
    Time = emqx_dashboard_collection:get_local_time() - 7200000,
    All = dets:select(emqx_collect, [{{mqtt_collect,'$1','$2'}, [{'>', '$1', Time}], ['$_']}]),
    format(lists:sort(All));
sampling(Node) ->
    rpc:call(Node, ?MODULE, sampling, [Node]).

sampling(Node, Type) when Node =:= node() ->
    Time = emqx_dashboard_collection:get_local_time() - 7200000,
    All = dets:select(emqx_collect, [{{mqtt_collect,'$1','$2'}, [{'>', '$1', Time}], ['$_']}]),
    format_single(lists:sort(All), Type);
sampling(Node, Type) ->
    rpc:call(Node, ?MODULE, sampling, [Node, Type]).

format(Collects) ->
    format(Collects, {[],[],[],[],[],[]}).
format([], {Connection, Route, Subscription, Received, Sent, Dropped}) ->
    [{connection, lists:reverse(Connection)},
     {route, lists:reverse(Route)},
     {subscriptions, lists:reverse(Subscription)},
     {received, lists:reverse(Received)},
     {sent, lists:reverse(Sent)},
     {dropped, lists:reverse(Dropped)}];


format([#mqtt_collect{timestamp = Ts, collect = {C, R, S, Re, S1, D}} | Collects],
       {Connection, Route, Subscription, Received, Sent, Dropped}) ->
    format(Collects, {[[Ts, C]  | Connection],
                      [[Ts, R]  | Route],
                      [[Ts, S]  | Subscription],
                      [[Ts, Re] | Received],
                      [[Ts, S1] | Sent],
                      [[Ts, D]  | Dropped]}).

format_single(Collects, Type) ->
    format_single(Collects, Type, []).
format_single([], _Type, Acc) ->
    lists:reverse(Acc);
format_single([#mqtt_collect{timestamp = Ts, collect = Collect} | Collects], Type, Acc) ->
    format_single(Collects, Type, [[Ts, erlang:element(type(Type), Collect)] | Acc]).

type(connection) -> 1;
type(route) -> 2;
type(subscriptions) -> 3;
type(received) -> 4;
type(sent) -> 5;
type(dropped) -> 6.
