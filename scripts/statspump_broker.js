// StatsPump-flavored test MQTT broker.
//
// Unlike scripts/test_broker.js (which emits simple scalars on second/N topics),
// this publishes several hierarchical topics under `pump/system/...`, each carrying
// a NESTED JSON object in StatsPump's real shape:
//
//   { hostname, name, stats: { totalTimeMs, objectCount, ... } }
//
// This exercises the discovery pick-list (multiple topics), the field flattener
// (dig into stats.totalTimeMs) and the nested-payload extraction that motivated the
// fork. Messages are published with retain:true so a fresh subscription gets an
// immediate sample per topic.
//
// Usage: node scripts/statspump_broker.js        (listens on tcp://0.0.0.0:1884)
//        MQTT_PORT=1885 node scripts/statspump_broker.js

import { Aedes } from 'aedes';
import { createServer } from 'node:net';

const PORT = Number(process.env.MQTT_PORT || 1884);
const INTERVAL_MS = Number(process.env.MQTT_INTERVAL_MS || 1000);

const broker = await Aedes.createBroker();
const server = createServer(broker.handle);

// Each entry is one publishing poller with its own baseline metrics.
const pollers = [
  { topic: 'pump/system/pollerA', name: 'poller-meta', base: 12 },
  { topic: 'pump/system/pollerB', name: 'poller-meta', base: 30 },
  { topic: 'pump/system/message-spool', name: 'spool-meta', base: 5 },
];

const jitter = (center, spread) => center + (Math.random() - 0.5) * spread;

let tick = 0;
const publishAll = () => {
  tick += 1;
  for (const p of pollers) {
    const wave = Math.sin(tick / 10);
    const payload = {
      hostname: 'host1',
      name: p.name,
      stats: {
        totalTimeMs: Number(jitter(p.base + wave * 3, 2).toFixed(3)),
        objectCount: Math.round(jitter(p.base * 4, 6)),
        charCount: Math.round(jitter(p.base * 1000, 400)),
        pollerSuccessCount: tick,
        elementCount: Math.round(jitter(p.base * 2, 3)),
        sempRequestPageCount: Math.round(jitter(3, 2)),
      },
    };
    broker.publish({
      topic: p.topic,
      cmd: 'publish',
      qos: 0,
      retain: true,
      payload: JSON.stringify(payload),
    });
  }
};

// Bind dual-stack (IPv6 '::' accepts IPv4-mapped too). Docker Desktop resolves
// host.docker.internal to an IPv6 address, so an IPv4-only listener would accept the
// proxied TCP connection but break the MQTT stream (paho reports "EOF").
server.listen(PORT, '::', () => {
  console.log(`StatsPump test broker listening on tcp://[::]:${PORT} (dual-stack)`);
  console.log(`publishing every ${INTERVAL_MS}ms to:`, pollers.map((p) => p.topic).join(', '));
  setInterval(publishAll, INTERVAL_MS);
  publishAll();
  broker.on('client', (c) => console.log(`connect: ${c.id}`));
  broker.on('subscribe', (subs, c) => console.log(`subscribe: ${c.id} ->`, subs.map((s) => s.topic).join(', ')));
});
