#!/usr/bin/env bash
# Detiene todo el sistema. Con --purge elimina también volúmenes e imágenes locales.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
if [[ "${1:-}" == "--purge" ]]; then
  docker compose down -v --remove-orphans --rmi local
else
  docker compose down --remove-orphans
fi
echo "Sistema detenido."
