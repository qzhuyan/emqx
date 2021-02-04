%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_license_api).

-include("emqx_dashboard.hrl").

-import(minirest, [return/1]).

-rest_api(#{ name   => license_info
           , method => 'GET'
           , path   => "/license_info"
           , func   => license_info
           , descr  => "Get a license info"
           }).

-export([license_info/2]).

license_info(_Bindings, _Params) ->
    Info = emqx_license_mgr:info(),
    return({ok, info_list_to_binary(Info)}).

info_list_to_binary(Info) ->
    list_to_binary(Info, []).
list_to_binary([], Acc) ->
    lists:reverse(Acc);
list_to_binary([{Key, Val}|Info], Acc) when is_list(Val) ->
    list_to_binary(Info, [{Key, list_to_binary(Val)}|Acc]);
list_to_binary([{Key, Val}|Info], Acc) ->
    list_to_binary(Info, [{Key, Val}|Acc]).

