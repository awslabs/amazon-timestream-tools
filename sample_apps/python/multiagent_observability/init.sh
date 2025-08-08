#!/bin/bash
log() { echo "$(date '+%H:%M:%S') - $1"; }
SERVICES=("cfn-agent" "aws-agent" "grafana")
log "Starting services..."

if ! docker compose up -d; then
    log "ERROR: docker compose up failed. Exiting."
    exit 1
fi

get_container_id() {
  docker compose ps -q "$1"
}
check_health() {
  local cid
  cid=$(get_container_id "$1")
  [[ -n "$cid" ]] && docker inspect --format='{{.State.Health.Status}}' "$cid" 2>/dev/null
}
wait_for_status() {
  for s in "${SERVICES[@]}"; do
    log "Waiting for $s to report healthy status..."
    while true; do
      h=$(check_health "$s")
      [[ "$h" == "healthy" || "$h" == "unhealthy" ]] && break
      sleep 2
    done
  done
}
wait_for_status
while true; do
  all_healthy=true
  for s in "${SERVICES[@]}"; do
    h=$(check_health "$s")
    if [[ "$h" == "unhealthy" ]]; then
      log "$s is unhealthy, restarting..."
      docker compose restart "$s"
      wait_for_status
      all_healthy=false
      break
    elif [[ "$h" != "healthy" ]]; then
      all_healthy=false
    fi
  done
  $all_healthy && break
  sleep 3
done
log "All services healthy."
