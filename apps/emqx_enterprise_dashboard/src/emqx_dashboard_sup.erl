%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CHILD(I, P), {I, {I, start_link, P}, permanent, 5000, worker, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, { {one_for_all, 10, 100}, [?CHILD(emqx_dashboard_admin, []),
                                    ?CHILD(emqx_dashboard_collection, [#{}])] } }.

