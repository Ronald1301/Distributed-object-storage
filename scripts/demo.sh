#!/usr/bin/env bash
# Demo guiada de P16 para la defensa. Avanza con ENTER en cada paso.
#
#   ./scripts/demo.sh             (con los tiempos por defecto)
#   FAST=1 ./scripts/demo.sh      (REPAIR_DELAY=10 s, reconciliación cada 3 s)
#
# Consejo: abre la interfaz visual en vivo:  python3 scripts/p16.py watch   (http://localhost:8080/ui)
set -euo pipefail
cd "$(dirname "$0")/.."

URL=${P16_URL:-http://localhost:8080}
P16="python3 scripts/p16.py"

title() { printf "\n\033[1;36m══════ %s ══════\033[0m\n" "$1"; }
say()   { printf "\033[2m# %s\033[0m\n" "$1"; }
run()   { printf "\033[33m$ %s\033[0m\n" "$*"; eval "$@"; }
pause() { read -r -p $'\n\033[2m[ENTER para continuar]\033[0m ' _; }

if [[ "${FAST:-0}" == "1" ]]; then
  export REPAIR_DELAY_SECONDS=10 RECONCILE_INTERVAL_SECONDS=3
fi

title "0. Arranque: gateway + 3 storage nodes + PostgreSQL"
run docker compose up -d --build --wait
run docker compose ps --format "'table {{.Service}}\t{{.Status}}'"
run $P16 status
pause

title "1. PUT: ubicación (HRW) y replicación (RF=2)"
say "El gateway calcula el SHA-256, elige 2 nodos distintos con HRW y sólo responde tras 2 fsync + commit."
run "curl -s -X PUT --data-binary @docker-compose.yml $URL/objects/docs/docker-compose.yml"
run $P16 seed 12
run $P16 status
say "Las réplicas se reparten entre los 3 nodos; 'DESEADO' es lo que dicta HRW."
pause

title "2. GET / HEAD / LIST"
run "curl -s -D - -o /dev/null $URL/objects/docs/docker-compose.yml"
run "curl -sI $URL/objects/docs/docker-compose.yml"
run $P16 ls
run $P16 locate docs/docker-compose.yml
pause

title "3. Fallo de un nodo: docker compose stop storage2"
run docker compose stop storage2
say "Las lecturas siguen funcionando: si una réplica no responde se usa la otra."
for i in 000 001 002 003 004 005; do run $P16 get demo/objeto-$i.txt ">/dev/null"; done
run $P16 wait storage2 DOWN
say "Las escrituras también: se usan los siguientes nodos de la lista de preferencia HRW."
run $P16 seed 3 durante-caida/
run $P16 status
pause

title "4. Reparación: re-replicación tras REPAIR_DELAY"
say "Pasado REPAIR_DELAY el nodo se da por perdido y se re-crean sus copias en los demás."
run $P16 wait repaired --timeout 300
run $P16 events 12
run $P16 status
pause

title "5. Recuperación: docker compose start storage2"
run docker compose start storage2
run $P16 wait storage2 HEALTHY
run $P16 wait converged
say "Reconciliación: storage2 recibe las réplicas que le tocan y sobran copias en otros nodos."
run $P16 events 10
run $P16 status
pause

title "6. Integridad: corrupción silenciosa detectada al leer"
run $P16 corrupt demo/objeto-003.txt
run $P16 get demo/objeto-003.txt ">/dev/null"
say "El gateway recalcula el SHA-256, descarta la copia corrupta, sirve la válida y la repara (read-repair)."
sleep 1
run $P16 events 3
pause

title "7. Integridad: scrub (detección sin que nadie lea el objeto)"
run $P16 corrupt demo/objeto-004.txt
run $P16 scrub
pause

title "8. Disco perdido: storage1 arranca con el volumen vacío"
run $P16 wipe storage1
say "Forzamos un ciclo de reconciliación (el periódico lo haría igualmente):"
run $P16 reconcile
run $P16 wait converged
run $P16 events 6
pause

title "9. Membresía: se añade storage4"
run docker compose --profile scale up -d --wait storage4
run $P16 wait storage4 HEALTHY
run $P16 wait converged
say "Sólo se mueve ~1/4 de las réplicas, y todas HACIA el nodo nuevo (propiedad de HRW)."
run $P16 status
pause

title "10. Membresía: se retira storage4 (drain)"
run $P16 drain storage4
run $P16 wait storage4 DECOMMISSIONED
run $P16 status
run docker compose --profile scale stop storage4
pause

title "11. Comparación de estrategias de ubicación"
run docker compose exec gateway python -m tools.simulate_placement 20000

title "Fin de la demo"
say "Eventos completos: $P16 events 50   ·   Documentación: README.md y docs/"
