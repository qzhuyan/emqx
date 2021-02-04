%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_dashboard_sup:start_link(),
    emqx_dashboard:start_listeners(),
    emqx_dashboard_cli:load(),
    {ok, Sup}.

stop(_State) ->
    emqx_dashboard_cli:unload(),
    emqx_dashboard:stop_listeners().
