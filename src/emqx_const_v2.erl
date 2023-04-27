%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
%% @doc Never update this module, create a v3 instead.
%%--------------------------------------------------------------------

-module(emqx_const_v2).

%% Is it risky to include a head file here?
-include_lib("public_key/include/public_key.hrl").

-export([ make_sni_fun/1
        , make_tls_root_fun/2
        , verify_cert_extKeyUsage/2
        , make_tls_verify_fun/2
        ]).

%% === V1 STARTS ===
make_sni_fun(ListenerID) ->
    fun(SN) -> emqx_ocsp_cache:sni_fun(SN, ListenerID) end.
%% === V1 ENDS ===


%% === V2 Starts ===
make_tls_root_fun(rootcert_from_cacertfile, CADer) ->
    fun(InputChain) ->
            case lists:member(CADer, InputChain) of
                true -> {trusted_ca, CADer};
                _ -> unknown_ca
            end
    end.

make_tls_verify_fun(verify_cert_extKeyUsage, KeyUsages) ->
    AllowedKeyUsages = ext_key_opts(KeyUsages),
    fun(_, {bad_cert, _} = Reason, _) ->
            {fail, Reason};
       (_, {extension, #'Extension'{extnID = ?'id-ce-extKeyUsage', extnValue = VL}}, UserState) ->
            case do_verify_ext_key_usage(VL, AllowedKeyUsages) of
                true -> {valid, UserState};
                false ->
                    Reason = "Unhappy extKeyUsage",
                    {fail, Reason}
            end;
       (_, {extension, _}, UserState) ->
            {unknown, UserState};
       (#'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{extensions = ExtL}}, valid, UserState) ->
            %% @TODO valid or valid_peer??
            %% must have id-ce-extKeyUsage
            case lists:keyfind(?'id-ce-extKeyUsage', 2, ExtL) of
                #'Extension'{extnID = ?'id-ce-extKeyUsage', extnValue = _VL} ->
                    {valid, UserState};
                _ ->
                    {fail, extKeyUsage_not_set}
            end;
       (_, valid_peer, UserState) ->
            {valid, UserState}
    end.

verify_cert_extKeyUsage(CertExtL, AllowedKeyUsages) when is_list(AllowedKeyUsages) ->
    case lists:keyfind(?'id-ce-extKeyUsage', 2, CertExtL) of
        #'Extension'{extnID = ?'id-ce-extKeyUsage', extnValue = VL} when is_list(VL) ->
            do_verify_ext_key_usage(VL, AllowedKeyUsages);
        _  ->
            false
    end.

do_verify_ext_key_usage(_, []) ->
    %% Verify finished
    true;
do_verify_ext_key_usage(CertExtL, [Usage | T]) ->
    case lists:member(Usage, CertExtL) of
        true ->
            do_verify_ext_key_usage(CertExtL, T);
        false ->
            false
    end.

%% @doc Helper tls cert extension
-spec ext_key_opts(string()) -> [OidString::string() | public_key:oid()];
                  (undefined) -> undefined.
ext_key_opts(undefined) ->
    %% disabled
    undefined;
ext_key_opts(Str) ->
    Usages = string:tokens(Str, ","),
    lists:map(fun("clientAuth") ->
                      ?'id-kp-clientAuth';
                 ("serverAuth") ->
                      ?'id-kp-serverAuth';
                 ("codeSigning") ->
                      ?'id-kp-codeSigning';
                 ("emailProtection") ->
                      ?'id-kp-emailProtection';
                 ("timeStamping") ->
                      ?'id-kp-timeStamping';
                 ("ocspSigning") ->
                      ?'id-kp-OCSPSigning';
                 ([$O,$I,$D,$: | OidStr]) ->
                      OidList = string:tokens(OidStr, "."),
                      list_to_tuple(lists:map(fun list_to_integer/1, OidList))
              end, Usages).
%% === V2 Ends ===
