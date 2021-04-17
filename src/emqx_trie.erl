%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_trie).

-include("emqx.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Trie APIs
-export([ insert/1
        , match/1
        , lookup/1
        , delete/1
        , match_node/3
        , partition/1
        ]).

-export([empty/0]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-type(triple() :: {root | binary(), emqx_topic:word(), binary()}).

%% Mnesia tables
-define(TRIE_TAB, emqx_trie).
-define(TRIE_NODE_TAB, emqx_trie_node).

-elvis([{elvis_style, function_naming_convention, disable}]).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

%% @doc Create or replicate trie tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    %% Optimize storage
    StoreProps = [{ets, [{read_concurrency, true},
                         {write_concurrency, true}]}],
    %% Trie table
    ok = ekka_mnesia:create_table(?TRIE_TAB, [
                {ram_copies, [node()]},
                {record_name, trie},
                {attributes, record_info(fields, trie)},
                {storage_properties, StoreProps}]),
    %% Trie node table
    ok = ekka_mnesia:create_table(?TRIE_NODE_TAB, [
                {ram_copies, [node()]},
                {record_name, trie_node},
                {attributes, record_info(fields, trie_node)},
                {storage_properties, StoreProps}]);

mnesia(copy) ->
    %% Copy trie table
    ok = ekka_mnesia:copy_table(?TRIE_TAB, ram_copies),
    %% Copy trie_node table
    ok = ekka_mnesia:copy_table(?TRIE_NODE_TAB, ram_copies).

%%--------------------------------------------------------------------
%% Trie APIs
%%--------------------------------------------------------------------

%% @doc Insert a topic filter into the trie.
-spec(insert(emqx_topic:topic()) -> ok).
insert(Topic) when is_binary(Topic) ->
    case mnesia:wread({?TRIE_NODE_TAB, emqx_topic:words(Topic)}) of
        [#trie_node{topic = Topic}] ->
            ok;
        [TrieNode = #trie_node{topic = undefined}] ->
            write_trie_node(TrieNode#trie_node{topic = Topic});
        [] ->
            %% Add trie path
            ok = lists:foreach(fun add_path/1, triples(Topic)),
            %% Add last node
            write_trie_node(#trie_node{node_id = emqx_topic:words(Topic), topic = Topic})
    end.

%% @doc Find trie nodes that match the topic name.
-spec(match(emqx_topic:topic()) -> list(emqx_topic:topic())).
match(Topic) when is_binary(Topic) ->
    TrieNodes = match_node(emqx_topic:words(Topic)),
    [Name || #trie_node{topic = Name} <- TrieNodes, Name =/= undefined].

%% @doc Lookup a trie node.
-spec(lookup(NodeId :: binary()) -> [trie_node()]).
lookup(NodeId) ->
    mnesia:read(?TRIE_NODE_TAB, emqx_topic:words(NodeId)).

%% @doc Delete a topic filter from the trie.
-spec(delete(emqx_topic:topic()) -> ok).
delete(Topic0) when is_binary(Topic0) ->
    Topic = emqx_topic:words(Topic0),
    case mnesia:wread({?TRIE_NODE_TAB, Topic}) of
        [#trie_node{edge_count = 0}] ->
            ok = mnesia:delete({?TRIE_NODE_TAB, Topic}),
            delete_path(lists:reverse(triples(Topic0)));
        [TrieNode] ->
            write_trie_node(TrieNode#trie_node{topic = undefined});
        [] -> ok
    end.

%% @doc Is the trie empty?
-spec(empty() -> boolean()).
empty() ->
    ets:info(?TRIE_TAB, size) == 0.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% @doc Topic to triples.
-spec(triples(emqx_topic:topic()) -> list(triple())).
triples(Topic) when is_binary(Topic) ->
    triples(partition(emqx_topic:words(Topic)), root, []).

triples([], _Parent, Acc) ->
    lists:reverse(Acc);
triples([W | Words], Parent, Acc) ->
    Node = join(Parent, W),
    triples(Words, Node, [{Parent, W, Node}|Acc]).

join(root, W) when is_atom(W) ->
    [W];
join(root, W) ->
    W;
join(Parent, X) when is_atom(X)->
    join(Parent, [X]);
join(Parent, X) when is_atom(Parent)->
    join([Parent], X);
join(Parent, X) ->
    Parent ++ X.

%% @private
%% @doc Add a path to the trie.
add_path({Node, Word, Child}) ->
    Edge = #trie_edge{node_id = Node, word = Word},
    case mnesia:wread({?TRIE_NODE_TAB, Node}) of
        [TrieNode = #trie_node{edge_count = Count}] ->
            case mnesia:wread({?TRIE_TAB, Edge}) of
                [] ->
                    ok = write_trie_node(TrieNode#trie_node{edge_count = Count + 1}),
                    write_trie(#trie{edge = Edge, node_id = Child});
                [_] -> ok
            end;
        [] ->
            NodeId = case Node of
                         '#' -> ['#'];
                         '+' -> ['+'];
                         _ -> Node
                     end,
            ok = write_trie_node(#trie_node{node_id = NodeId, edge_count = 1}),
            write_trie(#trie{edge = Edge, node_id = Child})
    end.

%% @private
-spec match_node([binary()]) -> [trie_node()].
match_node(Topic) ->
    lists:foldl(fun({Sub, Left}, Acc) ->
                        match_node(Sub, Left, Acc)
                end, [], sub_topics(Topic)).


match_node(_Sub, [], ResAcc) ->
    ResAcc;
match_node(Sub, [This | LeftLevels], ResAcc) ->
    case mnesia:read(?TRIE_NODE_TAB, Sub) of
        [#trie_node{node_id = ParentId}] ->
            lists:foldl(fun(X, Acc) ->
                                case mnesia:read(?TRIE_NODE_TAB, maybe_root(ParentId) ++ [X]) of
                                    [Child] when X == '#' ->
                                        [Child | Acc];
                                    [#trie_node{}] when X == '+' andalso
                                                        length(This) > 1 ->
                                        %% avoid '+' hit a multi-level target.
                                        Acc;
                                    [#trie_node{} = Child] when LeftLevels == []
                                                                andalso X =='#' ->
                                        [Child | Acc];
                                    [#trie_node{node_id = ChildId} = Child] when LeftLevels == [] ->
                                        'match_#'(ChildId, [Child | Acc]);
                                    [#trie_node{}] when ParentId =:= root andalso not is_atom(X) ->
                                        Acc;
                                    [#trie_node{node_id = ChildId}] ->
                                        [_ | Left] = LeftLevels,
                                        match_node(ChildId, Left, Acc);
                                    [] ->
                                        Acc
                                end
                        end, ResAcc, ['#', '+', This]);
        [] ->
            ResAcc
    end.

maybe_root(root) -> [];
maybe_root(Other) -> Other.

%% @private
%% @doc Match node with '#'.
'match_#'(NodeId, ResAcc) ->
    case mnesia:read(?TRIE_TAB, #trie_edge{node_id = NodeId, word = '#'}) of
        [#trie{node_id = ChildId}] ->
            mnesia:read(?TRIE_NODE_TAB, ChildId) ++ ResAcc;
        [] -> ResAcc
    end.

%% @private
%% @doc Delete paths from the trie.
delete_path([]) ->
    ok;
delete_path([{NodeId, Word, _} | RestPath]) ->
    ok = mnesia:delete({?TRIE_TAB, #trie_edge{node_id = NodeId, word = Word}}),
    case mnesia:wread({?TRIE_NODE_TAB, NodeId}) of
        [#trie_node{edge_count = 1, topic = undefined}] ->
            ok = mnesia:delete({?TRIE_NODE_TAB, NodeId}),
            delete_path(RestPath);
        [TrieNode = #trie_node{edge_count = 1, topic = _}] ->
            write_trie_node(TrieNode#trie_node{edge_count = 0});
        [TrieNode = #trie_node{edge_count = C}] ->
            write_trie_node(TrieNode#trie_node{edge_count = C-1});
        [] ->
            mnesia:abort({node_not_found, NodeId})
    end.

%% @private
write_trie(Trie) ->
    mnesia:write(?TRIE_TAB, Trie, write).

%% @private
write_trie_node(TrieNode) ->
    mnesia:write(?TRIE_NODE_TAB, TrieNode, write).

partition(L) ->
    partition(L, [], []).
partition([], [], Acc) ->
    lists:reverse(Acc);
partition([], Part, Acc) ->
    lists:reverse([lists:reverse(Part) | Acc]);
partition(["" | T], Part, Acc) ->
    partition(T, Part, Acc);
partition([H | T], [], Acc) when H == '#' orelse H == '+' ->
    partition(T, [], [H | Acc]);
partition([H | T], Part, Acc) when H == '#' orelse H == '+' ->
    partition(T, [], [H, lists:reverse(Part) | Acc]);
partition([H | T], Part, Acc) ->
    partition(T, [ H | Part ], Acc).

sub_topics(Topic) ->
    Levels = length(Topic),
    sub_topics(Levels, Topic, []).

sub_topics(0, [<<$$, _/binary>> | _], Res) ->
    Res;
sub_topics(0, Topic, Res) ->
    [{root, Topic} | Res];
sub_topics(Level, Topic, Res) ->
    This = {lists:sublist(Topic, 1, Level), lists:sublist(Topic, Level+1, length(Topic))},
    sub_topics(Level -1, Topic, [This | Res]).
