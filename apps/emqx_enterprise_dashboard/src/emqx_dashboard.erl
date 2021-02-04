%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-import(proplists, [get_value/3]).

-export([ start_listeners/0
        , stop_listeners/0
        ]).

%% callback for minirest
-export([is_authorized/1]).

-define(APP, ?MODULE).

%%--------------------------------------------------------------------
%% Start/Stop listeners.
%%--------------------------------------------------------------------

start_listeners() ->
    lists:foreach(fun(Listener) -> start_listener(Listener) end, listeners()).

%% Start HTTP Listener
start_listener({Proto, Port, Options}) when Proto == http ->
    Dispatch = [{"/", cowboy_static, {priv_file, emqx_dashboard, "www/index.html"}},
                {"/static/[...]", cowboy_static, {priv_dir, emqx_dashboard, "www/static"}},
                {"/api/v4/[...]", minirest, http_handlers()}],
    minirest:start_http(listener_name(Proto), ranch_opts(Port, Options), Dispatch);

start_listener({Proto, Port, Options}) when Proto == https ->
    Dispatch = [{"/", cowboy_static, {priv_file, emqx_dashboard, "www/index.html"}},
                {"/static/[...]", cowboy_static, {priv_dir, emqx_dashboard, "www/static"}},
                {"/api/v4/[...]", minirest, http_handlers()}],
    minirest:start_https(listener_name(Proto), ranch_opts(Port, Options), Dispatch).

ranch_opts(Port, Options0) ->
    NumAcceptors = get_value(num_acceptors, Options0, 4),
    MaxConnections = get_value(max_connections, Options0, 512),
    Options = lists:foldl(fun({K, _V}, Acc) when K =:= max_connections orelse K =:= num_acceptors->
                              Acc;
                             ({K, V}, Acc)->
                              [{K, V} | Acc]
                          end, [], Options0),
    #{num_acceptors => NumAcceptors,
      max_connections => MaxConnections,
      socket_opts => [{port, Port} | Options]}.

stop_listeners() ->
    lists:foreach(fun(Listener) -> stop_listener(Listener) end, listeners()).

stop_listener({Proto, Port, _}) ->
    io:format("Stop http:dashboard listener on ~s successfully.~n",[format(Port)]),
    minirest:stop_http(listener_name(Proto)).

listeners() ->
    application:get_env(?APP, listeners, []).

listener_name(Proto) ->
    list_to_atom(atom_to_list(Proto) ++ ":dashboard").

%%--------------------------------------------------------------------
%% HTTP Handlers and Dispatcher
%%--------------------------------------------------------------------

http_handlers() ->
    Plugins = lists:map(fun(Plugin) -> Plugin#plugin.name end, emqx_plugins:list()),
    [{"/api/v4/", minirest:handler(#{apps => Plugins}),[{authorization, fun ?MODULE:is_authorized/1}]}].

%%--------------------------------------------------------------------
%% Basic Authorization
%%--------------------------------------------------------------------

is_authorized(Req) ->
    is_authorized(binary_to_list(cowboy_req:path(Req)), Req).

is_authorized("/api/v4/auth", _Req) ->
    true;
is_authorized(_Path, Req) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, Username, Password} ->
            case emqx_dashboard_admin:check(iolist_to_binary(Username),
                                            iolist_to_binary(Password)) of
                ok -> true;
                {error, Reason} ->
                    ?LOG(error, "[Dashboard] Authorization Failure: username=~s, reason=~p",
                                [Username, Reason]),
                    false
            end;
         _  -> false
    end.

format(Port) when is_integer(Port) ->
    io_lib:format("0.0.0.0:~w", [Port]);
format({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~s:~w", [Addr, Port]);
format({Addr, Port}) when is_tuple(Addr) ->
    io_lib:format("~s:~w", [inet:ntoa(Addr), Port]).