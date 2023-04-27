%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_listeners_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(tls_cert_helper, [ gen_ca/2
                         , gen_host_cert/3
                         ]).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    NewConfig = generate_config(),
    generate_tls_certs(Config),
    application:ensure_all_started(esockd),
    application:ensure_all_started(cowboy),
    lists:foreach(fun set_app_env/1, NewConfig),
    Config.

end_per_suite(_Config) ->
    application:stop(esockd),
    application:stop(cowboy).

t_start_stop_listeners(_) ->
    ok = emqx_listeners:start(),
    ?assertException(error, _, emqx_listeners:start_listener({ws,{"127.0.0.1", 8083}, []})),
    ok = emqx_listeners:stop().

t_restart_listeners(_) ->
    ok = emqx_listeners:start(),
    ok = emqx_listeners:stop(),
    ok = emqx_listeners:restart(),
    ok = emqx_listeners:stop().

t_wss_conn(_) ->
    ok = emqx_listeners:start(),
    {ok, Socket} = ssl:connect({127, 0, 0, 1}, 8084, [{verify, verify_none}], 1000),
    ok = ssl:close(Socket),
    ok = emqx_listeners:stop().

t_tls_conn_success_when_partial_chain_enabled_with_intermediate_ca_cert(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, root_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client1.key")},
                                                    {certfile,  filename:join(DataDir, "client1.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ssl:close(Socket).

t_tls_conn_fail_when_partial_chain_disabled_with_intermediate_ca_cert(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client1.key")},
                                                    {certfile,  filename:join(DataDir, "client1.pem")}
                                                   ], 1000),

  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).


t_tls_conn_fail_when_partial_chain_disabled_with_other_intermediate_ca_cert(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2.pem")}
                                                   ], 1000),

  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_tls_conn_fail_when_partial_chain_enabled_with_other_intermediate_ca_cert(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, root_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_tls_conn_success_when_partial_chain_enabled_root_ca_with_complete_cert_chain(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, root_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "root.pem")}
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).

t_tls_conn_success_when_partial_chain_disabled_with_intermediate_ca_cert(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client1.key")},
                                                    {certfile,  filename:join(DataDir, "client1.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_tls_conn_success_when_partial_chain_disabled_with_broken_cert_chain_other_intermediate(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  %% Server has root ca cert
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "root.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  %% Client has complete chain
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile, filename:join(DataDir, "client2.key")},
                                                    {certfile, filename:join(DataDir, "client2-intermediate2-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).

t_tls_conn_fail_when_partial_chain_enabled_with_other_complete_cert_chain(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, root_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_tls_conn_success_when_partial_chain_enabled_with_complete_cert_chain(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, root_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate2.pem")}
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).

t_tls_conn_success_when_partial_chain_disabled_with_complete_cert_chain_client(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "root.pem")}
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_ssl_error(Socket),
  ok = ssl:close(Socket).


t_tls_conn_fail_when_partial_chain_disabled_with_incomplete_cert_chain_server(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "intermediate2.pem")} %% imcomplete at server side
                           , {certfile, filename:join(DataDir, "server2.pem")}
                           , {keyfile, filename:join(DataDir, "server2.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-complete-bundle.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).


t_tls_conn_fail_when_partial_chain_enabled_with_imcomplete_cert_chain(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {partial_chain, root_from_cacertfile}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-intermediate2-bundle.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

t_tls_conn_fail_when_partial_chain_disabled_with_incomplete_cert_chain(Config) ->
  Port = tls_cert_helper:select_free_port(ssl),
  DataDir = ?config(data_dir, Config),
  Options = [{ssl_options, [ {verify, verify_peer}
                           , {fail_if_no_peer_cert, true}
                           , {cacertfile, filename:join(DataDir, "intermediate1.pem")}
                           , {certfile, filename:join(DataDir, "server1.pem")}
                           , {keyfile, filename:join(DataDir, "server1.key")}
                           ]}],
  emqx_listeners:start_listener(ssl, Port, Options),
  {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{keyfile,  filename:join(DataDir, "client2.key")},
                                                    {certfile,  filename:join(DataDir, "client2-intermediate2-bundle.pem")}
                                                   ], 1000),
  fail_when_no_ssl_alert(Socket, unknown_ca),
  ok = ssl:close(Socket).

render_config_file() ->
    Path = local_path(["..", "..", "..", "..", "etc", "emqx.conf"]),
    {ok, Temp} = file:read_file(Path),
    Vars0 = mustache_vars(),
    Vars = [{atom_to_list(N), iolist_to_binary(V)} || {N, V} <- Vars0],
    Targ = bbmustache:render(Temp, Vars),
    NewName = Path ++ ".rendered",
    ok = file:write_file(NewName, Targ),
    NewName.

mustache_vars() ->
    [{platform_data_dir, local_path(["data"])},
     {platform_etc_dir,  local_path(["etc"])},
     {platform_log_dir,  local_path(["log"])},
     {platform_plugins_dir,  local_path(["plugins"])}
    ].

generate_config() ->
    Schema = cuttlefish_schema:files([local_path(["priv", "emqx.schema"])]),
    ConfFile = render_config_file(),
    Conf = conf_parse:file(ConfFile),
    cuttlefish_generator:map(Schema, Conf).

set_app_env({App, Lists}) ->
    lists:foreach(fun({acl_file, _Var}) ->
                      application:set_env(App, acl_file, local_path(["etc", "acl.conf"]));
                     ({plugins_loaded_file, _Var}) ->
                      application:set_env(App, plugins_loaded_file, local_path(["test", "emqx_SUITE_data","loaded_plugins"]));
                     ({Par, Var}) ->
                      application:set_env(App, Par, Var)
                  end, Lists).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

local_path(Components) ->
    local_path(Components, ?MODULE).

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

get_base_dir() ->
    get_base_dir(?MODULE).

generate_tls_certs(Config) ->
  DataDir = ?config(data_dir, Config),
  gen_ca(DataDir, "root"),
  gen_host_cert("intermediate1", "root", DataDir),
  gen_host_cert("intermediate2", "root", DataDir),
  gen_host_cert("server1", "intermediate1", DataDir),
  gen_host_cert("client1", "intermediate1", DataDir),
  gen_host_cert("server2", "intermediate2", DataDir),
  gen_host_cert("client2", "intermediate2", DataDir),
  os:cmd(io_lib:format("cat ~p ~p ~p > ~p", [filename:join(DataDir, "client2.pem"),
                                             filename:join(DataDir, "intermediate2.pem"),
                                             filename:join(DataDir, "root.pem"),
                                             filename:join(DataDir, "client2-complete-bundle.pem")
                                            ])),
  os:cmd(io_lib:format("cat ~p ~p > ~p", [filename:join(DataDir, "client2.pem"),
                                          filename:join(DataDir, "intermediate2.pem"),
                                          filename:join(DataDir, "client2-intermediate2-bundle.pem")
                                         ])).


fail_when_ssl_error(Socket) ->
  receive
    {ssl_error, Socket, _} ->
      ct:fail("Handshake failed!")
  after 1000 ->
      ok
  end.

fail_when_no_ssl_alert(Socket, Alert) ->
  receive
    {ssl_error, Socket, {tls_alert, {Alert, AlertInfo}}} ->
        ct:pal("alert info: ~p~n", [AlertInfo]);
    {ssl_error, Socket, Other} ->
        ct:fail("recv unexpected ssl_error: ~p~n", [Other])
  after 1000 ->
      ct:fail("No expected alert: ~p from Socket: ~p ", [Alert, Socket])
  end.
