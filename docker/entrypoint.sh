#!/bin/bash
set -e

echo "=== Deploying Constraints Builder with self-contained Reth/Lighthouse ==="

echo "EL Bootnodes: ${BOOTNODES_EL}..."
echo "CL Bootnodes: ${BOOTNODES_CL}..."
echo "LIBP2P_ADDR: ${LIBP2P_ADDR}..."
echo "TRUSTED_PEER: ${TRUSTED_PEER}..."
echo "FEE_RECIPIENT: ${FEE_RECIPIENT}..."

# Start Reth
echo "[1/3] Starting Reth..."
/usr/local/bin/reth node \
    -vvv \
    --datadir=/data/reth/execution-data \
    --chain=/network-configs/genesis.json \
    --http \
    --http.port=8545 \
    --http.addr=0.0.0.0 \
    --http.corsdomain=* \
    --http.api=admin,net,eth,web3,debug,txpool,trace \
    --ws \
    --ws.addr=0.0.0.0 \
    --ws.port=8546 \
    --ws.api=net,eth \
    --ws.origins=* \
    --authrpc.port=8551 \
    --authrpc.jwtsecret=/jwt/jwtsecret \
    --authrpc.addr=0.0.0.0 \
    --metrics=0.0.0.0:9001 \
    --discovery.port=30303 \
    --port=30303 \
    --bootnodes=${BOOTNODES_EL} \
    --ipcpath=/data/reth/reth.ipc \
    &
RETH_PID=$!

# Wait for Reth
for i in {1..60}; do [ -S /data/reth/reth.ipc ] && break; sleep 1; done

# Start Lighthouse
echo "[2/3] Starting Lighthouse..."
/usr/local/bin/lighthouse beacon_node \
    --debug-level=info \
    --datadir=/data/lighthouse/beacon-data \
    --testnet-dir=/network-configs \
    --listen-address=0.0.0.0 \
    --port=9000 \
    --http \
    --http-address=0.0.0.0 \
    --http-port=4000 \
    --disable-packet-filter \
    --execution-endpoints=http://127.0.0.1:8551 \
    --jwt-secrets=/jwt/jwtsecret \
    --suggested-fee-recipient=${FEE_RECIPIENT:-0x0000000000000000000000000000000000000000} \
    --metrics \
    --metrics-address=0.0.0.0 \
    --metrics-port=5054 \
    --enable-private-discovery \
    --allow-insecure-genesis-sync \
    --boot-nodes=${BOOTNODES_CL} \
    --libp2p-addresses=${LIBP2P_ADDR} \
    --trusted-peers=${TRUSTED_PEER:-} \
    --always-prepare-payload \
    &
LIGHTHOUSE_PID=$!

# Wait for Lighthouse
for i in {1..60}; do
    curl -s http://127.0.0.1:4000/eth/v1/node/health > /dev/null 2>&1 && break
    sleep 1
done

# Start rbuilder
echo "[3/3] Starting rbuilder..."
sleep 5
/usr/local/bin/rbuilder run /app/config.toml &
RBUILDER_PID=$!

echo "=== All services started ==="

# Monitor
trap "kill $RBUILDER_PID $LIGHTHOUSE_PID $RETH_PID 2>/dev/null; exit 0" SIGTERM SIGINT
while kill -0 $RETH_PID $LIGHTHOUSE_PID 2>/dev/null; do sleep 5; done