#!/bin/bash

set -euo pipefail

force="${1:-no}"
curl_option=""
if [ -n "${2:-}" ]; then
    curl_option="-u ${2}"
fi

apps=(
"emqx_auth_http"
"emqx_web_hook"
"emqx_auth_jwt"
"emqx_auth_ldap"
"emqx_auth_mongo"
"emqx_auth_mysql"
"emqx_auth_pgsql"
"emqx_auth_redis"
"emqx_bridge_mqtt"
"emqx_coap"
"emqx_enterprise_dashboard"
"emqx_exhook"
"emqx_exproto"
"emqx_lua_hook"
"emqx_lwm2m"
"emqx_management"
"emqx_prometheus"
"emqx_psk_file"
"emqx_recon"
"emqx_retainer"
"emqx_rule_engine"
"emqx_sasl"
"emqx_sn"
"emqx_stomp"
"emqx_telemetry"
"emqx_auth_mnesia"
)

default_vsn="e4.2.4"

if git status --porcelain | grep -qE 'apps/'; then
    echo 'apps dir is not git-clear, refuse to sync'
#    exit 1
fi

mkdir -p tmp/

# get version number from
get_vsn() {
    local app="$1"
    case "$app" in
        emqx_telemetry)
            echo "4.2.7"
            ;;
        *)
            echo "$default_vsn"
            ;;
    esac
}

download_zip() {
    local app="$1"
    local vsn="$(get_vsn "$app")"
    local file="tmp/${app}-${vsn}.zip"
    if [ -f "$file" ] && [ "$force" != "force" ]; then
        return 0
    fi
    local repo
    repo=${app//_/-}
    local url="https://github.com/emqx/$repo/archive/$vsn.zip"
    echo "downloading ${url}"
    # shellcheck disable=SC2086
    curl $curl_option -fLsS -o "$file" "$url"
}

for app in "${apps[@]}"; do
    download_zip "$app"
done

extract_zip(){
    local app="$1"
    local vsn="$(get_vsn "$app")"
    local file="tmp/${app}-${vsn}.zip"
    local repo
    repo=${app//_/-}
    rm -rf "apps/${app}/"
    unzip "$file" -d apps/
    mv "apps/${repo}-${vsn}/" "apps/$app/"
}

for app in "${apps[@]}"; do
    extract_zip "$app"
done

cleanup_app(){
    local app="$1"
    pushd "apps/$app"
    rm -f Makefile rebar.config.script LICENSE src/*.app.src.script src/*.appup.src
    rm -rf ".github" ".ci"
    # restore rebar.config and app.src
    git checkout rebar.config
    git checkout src/*.app.src
    popd
}

for app in "${apps[@]}"; do
    cleanup_app "$app"
done
