#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

usage() {
    printf 'Uso: bash scripts/local-stack.sh {init|up|status|logs|down}\n'
    printf '  init    gera credenciais locais em .env sem sobrescrever um arquivo existente\n'
    printf '  up      inicia MongoDB, Kafka, SQL Server, API, worker e storefront\n'
    printf '  status  mostra o estado dos containers\n'
    printf '  logs    acompanha os logs da API, workers e storefront\n'
    printf '  down    para a stack e preserva os volumes locais\n'
}

require_env() {
    if [[ ! -f .env ]]; then
        printf 'Arquivo .env não encontrado. Execute primeiro: bash scripts/local-stack.sh init\n' >&2
        exit 1
    fi
}

initialize_env() {
    if [[ -e .env ]]; then
        printf 'O arquivo .env já existe; não foi alterado.\n'
        return
    fi

    if ! docker info >/dev/null 2>&1; then
        printf 'O daemon Docker precisa estar ativo para gerar credenciais dentro de um container.\n' >&2
        exit 1
    fi

    local generated_values
    if ! generated_values="$(docker run --rm alpine:3.21 sh -c \
        'apk add --no-cache openssl >/dev/null && for _ in 1 2 3 4 5; do openssl rand -hex 32; done')"; then
        printf 'Não foi possível gerar credenciais usando o container temporário Alpine.\n' >&2
        exit 1
    fi
    local -a secrets
    mapfile -t secrets <<< "$generated_values"
    if [[ ${#secrets[@]} -ne 5 ]]; then
        printf 'O gerador de credenciais retornou uma resposta incompleta.\n' >&2
        exit 1
    fi

    umask 077
    local temporary_env
    temporary_env="$(mktemp "$ROOT/.env.XXXXXX")"
    trap 'rm -f "$temporary_env"' RETURN

    {
        printf 'MSSQL_SA_PASSWORD=%sAa1!\n' "${secrets[0]}"
        printf 'SPLITPAYMENT_DB_PASSWORD=%sBb2!\n' "${secrets[1]}"
        printf 'MONGODB_ROOT_USERNAME=splitpayment_admin\n'
        printf 'MONGODB_ROOT_PASSWORD=%s\n' "${secrets[2]}"
        printf 'MONGODB_APP_USERNAME=splitpayment_app\n'
        printf 'MONGODB_APP_PASSWORD=%s\n' "${secrets[3]}"
        printf 'INVOICE_API_KEY=%s\n' "${secrets[4]}"
        printf 'DEMO_PAYMENTS_MODE=Mock\n'
        printf 'MERCADOPAGO_MODE=Disabled\n'
        printf 'MERCADOPAGO_ACCESS_TOKEN=\n'
        printf 'MERCADOPAGO_WEBHOOK_SECRET=\n'
        printf 'MERCADOPAGO_NOTIFICATION_URL=\n'
        printf 'STOREFRONT_PUBLIC_BASE_URL=http://localhost:5081\n'
    } > "$temporary_env"

    chmod 600 "$temporary_env"
    mv -- "$temporary_env" .env
    trap - RETURN
    printf 'Credenciais locais geradas em .env (permissão 600; arquivo ignorado pelo Git).\n'
    printf 'Mercado Pago permanece Disabled; nenhum token real ou pagamento será usado.\n'
}

if [[ $# -ne 1 ]]; then
    usage >&2
    exit 2
fi

case "$1" in
    init)
        initialize_env
        ;;
    up)
        if ! docker info >/dev/null 2>&1; then
            printf 'Não foi possível acessar o daemon Docker.\n' >&2
            printf 'Inicie o Docker Desktop/Engine ou use uma conta com acesso ao daemon.\n' >&2
            exit 1
        fi
        if [[ ! -f .env ]]; then
            initialize_env
        fi
        docker compose config --quiet
        docker compose up --build --detach --wait --wait-timeout 240
        docker compose wait kafka-init sqlserver-init
        printf 'Stack pronta:\n'
        printf '  Storefront: http://localhost:5081\n'
        printf '  API:        http://localhost:5080\n'
        printf '  MongoDB:    mongodb://localhost:27017 (somente loopback)\n'
        ;;
    status)
        require_env
        docker compose ps
        ;;
    logs)
        require_env
        docker compose logs --follow invoice-api engine-worker cgibs-gateway storefront
        ;;
    down)
        require_env
        docker compose down
        ;;
    help|-h|--help)
        usage
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
