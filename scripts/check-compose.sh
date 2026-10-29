#!/usr/bin/env bash
# Brings up the compose stack and checks that Postgres runs with logical
# WAL and that Electric serves both a plain shape and a subquery shape,
# including a live change. Leaves the stack running; `docker compose down -v`
# to reset.
set -euo pipefail
cd "$(dirname "$0")/.."

pg_port=${LOCALSYNC_PG_PORT:-5532}
el_port=${LOCALSYNC_ELECTRIC_PORT:-3130}
secret=${ELECTRIC_SECRET:-localsync-dev-secret}
base="http://localhost:${el_port}/v1/shape"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

psql() { docker compose exec -T postgres psql -U postgres -d localsync -v ON_ERROR_STOP=1 -qAt "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

docker compose up -d --wait --quiet-pull 2>&1 | grep -v -e Pulling -e Waiting || true

[ "$(psql -c 'SHOW wal_level')" = logical ] || fail 'wal_level is not logical'
echo "ok   postgres $(psql -c 'SHOW server_version'), wal_level=logical, max_slot_wal_keep_size=$(psql -c 'SHOW max_slot_wal_keep_size')"

# Fresh table names per run: recreating a table Electric already has a
# shape for gets a stale handle and then a must-refetch, which is DDL
# behaviour for the harness to test, not something a smoke check should
# trip over.
run=$(date +%s)
item=smoke_item_$run
owner=smoke_owner_$run
psql <<SQL
SET client_min_messages = warning;
DO \$\$ DECLARE t text; BEGIN
	FOR t IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'smoke\\_%' LOOP
		EXECUTE format('DROP TABLE %I', t);
	END LOOP;
END \$\$;
CREATE TABLE $owner (id int PRIMARY KEY, user_id text NOT NULL);
CREATE TABLE $item (id int PRIMARY KEY, owner_id int NOT NULL, title text);
INSERT INTO $owner VALUES (1, 'u1'), (2, 'u2');
INSERT INTO $item VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c');
SQL

# Plain shape: initial snapshot.
body=$(curl -sf "${base}?table=${item}&offset=-1&secret=${secret}") || fail 'plain shape request failed'
rows=$(grep -o '"operation":"insert"' <<<"$body" | wc -l)
[ "$rows" -eq 3 ] || fail "plain shape returned $rows rows, want 3: $body"
echo "ok   electric plain shape: $rows rows"

# Subquery shape (a per-user membership chain).
where=$(printf '%s' "owner_id IN (SELECT id FROM ${owner} WHERE user_id = 'u1')" | jq -sRr @uri)
url="${base}?table=${item}&where=${where}&secret=${secret}"
body=$(curl -sf -D "$tmp/hdr" "${url}&offset=-1") || fail 'subquery shape request failed'
rows=$(grep -o '"operation":"insert"' <<<"$body" | wc -l)
[ "$rows" -eq 2 ] || fail "subquery shape returned $rows rows, want 2: $body"
handle=$(grep -i '^electric-handle:' "$tmp/hdr" | tr -d '\r' | cut -d' ' -f2)
offset=$(grep -i '^electric-offset:' "$tmp/hdr" | tr -d '\r' | cut -d' ' -f2)
echo "ok   electric subquery shape: $rows rows (handle $handle)"

# Catch up to up-to-date, then a live long-poll must see a move-in caused
# by a write to the subquery's table.
while :; do
	body=$(curl -sf -D "$tmp/hdr" "${url}&offset=${offset}&handle=${handle}") || fail 'catch-up failed'
	offset=$(grep -i '^electric-offset:' "$tmp/hdr" | tr -d '\r' | cut -d' ' -f2)
	grep -q 'up-to-date' <<<"$body" && break
done
curl -sf -m 20 "${url}&offset=${offset}&handle=${handle}&live=true" >"$tmp/live" &
poll=$!
sleep 1
psql -c "UPDATE ${owner} SET user_id = 'u1' WHERE id = 2"
wait "$poll" || fail 'live long-poll failed'
grep -q '"id":"3"' "$tmp/live" || fail "live poll did not deliver the move-in: $(cat "$tmp/live")"
echo "ok   electric live move-in via related-table write"
echo 'compose stack OK'
