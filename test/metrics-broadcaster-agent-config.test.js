import assert from 'node:assert/strict';
import test from 'node:test';
import { MetricsBroadcaster } from '../src/durable/MetricsBroadcaster.js';
import { getHistoryMetrics } from '../src/handlers/update.js';

globalThis.WebSocketRequestResponsePair = class WebSocketRequestResponsePair {
  constructor(request, response) {
    this.request = request;
    this.response = response;
  }
};

function makeBroadcaster(webSockets = [], env = { DB: {} }) {
  return new MetricsBroadcaster({
    setWebSocketAutoResponse() {},
    getWebSockets() {
      return webSockets;
    },
    storage: {
      async get() {
        return null;
      },
      async put() {}
    }
  }, env);
}

function makeDescriptor(md5 = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') {
  return {
    serialized: 'collect_interval=0&report_interval=60&reset_day=1&schema_version=3&custom_ct=&custom_cu=&custom_cm=&custom_bd=&interface=',
    md5,
    config: {
      collect_interval: 0,
      report_interval: 60,
      reset_day: 1,
      schema_version: 3,
      custom_ct: '',
      custom_cu: '',
      custom_cm: '',
      custom_bd: '',
      interface: ''
    },
    correction: null
  };
}

test('WSS agent config state only requests ack for fields in current report', () => {
  const broadcaster = makeBroadcaster();
  assert.deepEqual(
    broadcaster._getAgentConfigState({}, { configSchema: '3', configMd5: 'none' }),
    { schema: '3', md5: 'none', requested: false }
  );
  assert.deepEqual(
    broadcaster._getAgentConfigState({ config_md5: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' }, { configSchema: '3', configMd5: 'none' }),
    { schema: '3', md5: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', requested: true }
  );
});

test('WSS agent ack suggests realtime or idle report interval', () => {
  const broadcaster = makeBroadcaster();
  assert.equal(broadcaster._getAgentNextWssReportAfterMs(60000, true), 3000);
  assert.equal(broadcaster._getAgentNextWssReportAfterMs(60000, false), 60000);
  assert.equal(broadcaster._getAgentNextWssReportAfterMs(30000, true), 2000);
  assert.equal(
    broadcaster._getAgentNextWssReportAfterMs(30000, {
      frontendActive: false,
      resourceAlertActive: true,
      realtimeActive: true
    }),
    60000
  );
  assert.equal(
    broadcaster._getAgentNextWssReportAfterMs(120000, {
      frontendActive: false,
      resourceAlertActive: true,
      realtimeActive: true
    }),
    120000
  );
  assert.equal(
    broadcaster._getAgentNextWssReportAfterMs(60000, {
      frontendActive: true,
      resourceAlertActive: true,
      realtimeActive: true
    }),
    3000
  );
});

test('WSS agent context uses current report interval from payload', async () => {
  const broadcaster = makeBroadcaster();
  let serialized = null;
  const context = await broadcaster._resolveAgentContext({
    serializeAttachment(value) {
      serialized = value;
    }
  }, {
    kind: 'agent-report',
    authenticated: true,
    serverId: 'server-1',
    historyPartitionId: 42,
    reportIntervalMs: 60000,
    configSchema: '3',
    configMd5: 'none'
  }, {
    id: 'server-1',
    report_interval: 120
  });

  assert.equal(context.reportIntervalMs, 120000);
  assert.equal(serialized.reportIntervalMs, 120000);
});

test('WSS agent config ack is skipped when report omits config state', async () => {
  const broadcaster = makeBroadcaster();
  let loads = 0;
  broadcaster._loadAgentConfigDescriptor = async () => {
    loads += 1;
    return makeDescriptor();
  };

  const ack = await broadcaster._buildAgentConfigAck({
    attachment: {
      configSchema: '3',
      configMd5: 'none'
    },
    serverId: 'server-1',
    agentConfig: { schema: '3', md5: 'none', requested: false }
  });

  assert.equal(loads, 0);
  assert.equal(ack, null);
});

test('WSS agent config ack is built when report includes config state', async () => {
  const broadcaster = makeBroadcaster();
  let loads = 0;
  broadcaster._loadAgentConfigDescriptor = async () => {
    loads += 1;
    return makeDescriptor();
  };

  const ack = await broadcaster._buildAgentConfigAck({
    attachment: {},
    serverId: 'server-1',
    agentConfig: { schema: '3', md5: 'none', requested: true }
  });

  assert.equal(loads, 1);
  assert.equal(ack.has_config, true);
  assert.equal(ack.config_md5, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');
  assert.equal(ack.body, makeDescriptor().serialized);
  assert.equal(ack.config_body, makeDescriptor().serialized);
  assert.equal(ack.payload.report_interval, 60);
  assert.equal(Object.prototype.hasOwnProperty.call(ack, 'config'), false);
});

test('WSS agent config push uses string body and structured payload', () => {
  const sent = [];
  const ws = {
    deserializeAttachment() {
      return {
        kind: 'agent-report',
        authenticated: true,
        serverId: 'server-1',
        configSchema: '3',
        configMd5: 'none'
      };
    },
    send(message) {
      sent.push(JSON.parse(message));
    }
  };
  const broadcaster = makeBroadcaster([ws]);

  const result = broadcaster._pushAgentConfigFrame('server-1', makeDescriptor());

  assert.deepEqual(result, { matched: 1, delivered: 1 });
  assert.equal(sent.length, 1);
  assert.equal(sent[0].type, 'config');
  assert.equal(sent[0].body, makeDescriptor().serialized);
  assert.equal(sent[0].config_body, makeDescriptor().serialized);
  assert.equal(sent[0].payload.report_interval, 60);
  assert.equal(Object.prototype.hasOwnProperty.call(sent[0], 'config'), false);
});

test('resource alert rule batches are capped at 20 rules', () => {
  const broadcaster = makeBroadcaster();
  const makeRule = index => ({
    ruleId: `rule-${index}`,
    serverIds: ['server-1'],
    mode: 'average',
    windowMinutes: 5,
    thresholds: { cpuPercent: 80 }
  });

  assert.equal(
    broadcaster._normalizeResourceAlertEvaluationRules(Array.from({ length: 20 }, (_, index) => makeRule(index))).ok,
    true
  );
  assert.equal(
    broadcaster._normalizeResourceAlertEvaluationRules(Array.from({ length: 21 }, (_, index) => makeRule(index))).ok,
    false
  );
});

test('resource alert batch evaluation returns results per rule', async () => {
  const broadcaster = makeBroadcaster();
  const now = Date.now();
  const currentMinute = Math.floor(now / 60_000) * 60_000;
  const samples = [];
  for (let index = 4; index >= 0; index--) {
    const ts = currentMinute - index * 60_000;
    samples.push({
      ts,
      minuteTs: ts,
      cpu: 90,
      ram: 30,
      disk: 40,
      netIn: 0,
      netOut: 0,
      netTotal: 0
    });
  }
  broadcaster.resourceAlertWindows.set('server-1', { samples });

  const result = await broadcaster._evaluateResourceAlertRules([
    {
      ruleId: 'cpu-rule',
      serverIds: ['server-1'],
      mode: 'average',
      windowMinutes: 5,
      thresholds: { cpuPercent: 80 }
    },
    {
      ruleId: 'ram-rule',
      serverIds: ['server-1'],
      mode: 'average',
      windowMinutes: 5,
      thresholds: { ramPercent: 80 }
    }
  ]);

  const byRule = new Map(result.results.map(item => [item.ruleId, item]));
  assert.equal(byRule.get('cpu-rule').alerts.length, 1);
  assert.deepEqual(byRule.get('cpu-rule').evaluatedServerIds, ['server-1']);
  assert.equal(byRule.get('ram-rule').alerts.length, 0);
  assert.deepEqual(byRule.get('ram-rule').evaluatedServerIds, ['server-1']);
});

test('resource alert cache accepts payload samples from WSS broadcasts', async () => {
  const broadcaster = makeBroadcaster();
  const now = Date.now();
  const currentMinute = Math.floor(now / 60_000) * 60_000;

  await broadcaster._cacheResourceAlertSamples([{
    serverId: 'server-1',
    samples: [
      {
        ts: currentMinute - 60_000,
        payload: {
          cpu: 90,
          ram_total: 100,
          ram_used: 90,
          disk_total: 100,
          disk_used: 40,
          net_in_speed: 0,
          net_out_speed: 0
        }
      },
      {
        ts: currentMinute,
        payload: {
          cpu: 92,
          ram_total: 100,
          ram_used: 91,
          disk_total: 100,
          disk_used: 40,
          net_in_speed: 0,
          net_out_speed: 0
        }
      }
    ]
  }], now);

  const result = broadcaster._evaluateResourceAlertRule({
    ruleId: 'cpu-ram-rule',
    serverIds: ['server-1'],
    mode: 'average',
    windowMinutes: 5,
    thresholds: {
      cpuPercent: 80,
      ramPercent: 80
    }
  }, now);

  assert.deepEqual(result.evaluatedServerIds, ['server-1']);
  assert.equal(result.alerts.length, 1);
  assert.deepEqual(result.alerts[0].metrics.map(metric => metric.metric), ['cpu', 'ram']);
});

test('history metrics apply configured aggregation policies to batched POST samples', () => {
  const samples = [
    {
      ts: 1,
      metrics: {
        cpu: 20,
        ping_ct: 10,
        net_in_speed: 1024,
        net_out_speed: 2048,
        tcp_conn: 10,
        net_rx: 1000,
        disk: { read_bps: 1024 }
      }
    },
    {
      ts: 2,
      metrics: {
        cpu: 30,
        ping_ct: 20,
        net_in_speed: 40 * 1024 * 1024,
        net_out_speed: 4096,
        tcp_conn: 30,
        net_rx: 2000,
        disk: { read_bps: 80 * 1024 * 1024 }
      }
    },
    {
      ts: 3,
      metrics: {
        cpu: 40,
        ping_ct: 30,
        net_in_speed: 8192,
        net_out_speed: 8 * 1024 * 1024,
        tcp_conn: 20,
        net_rx: 3000,
        disk: { read_bps: 4 * 1024 * 1024 }
      }
    }
  ];

  const metrics = getHistoryMetrics({
    metrics: {
      cpu: 10,
      net_in_speed: 512,
      net_out_speed: 512
    }
  }, samples, samples[samples.length - 1]);

  assert.equal(metrics.cpu, 30);
  assert.equal(metrics.ping_ct, 20);
  assert.equal(metrics.net_in_speed, 40 * 1024 * 1024);
  assert.equal(metrics.net_out_speed, 8 * 1024 * 1024);
  assert.equal(metrics.tcp_conn, 30);
  assert.equal(metrics.net_rx, 3000);
  assert.equal(metrics.disk_read_bps, 80 * 1024 * 1024);
  assert.equal(metrics.disk.read_bps, 80 * 1024 * 1024);
});

test('WSS history persistence carries pending history aggregates into next D1 write', async () => {
  const savedRows = [];
  const db = {
    prepare() {
      return {
        bind(...args) {
          return {
            async run() {
              savedRows.push(args);
              return { meta: { changes: 1 } };
            }
          };
        }
      };
    }
  };
  const broadcaster = makeBroadcaster([], { DB: db });
  const originalNow = Date.now;
  let now = Date.UTC(2026, 0, 1, 0, 0, 0);
  let attachment = {
    reportIntervalMs: 60_000,
    lastD1WriteTs: now
  };
  const ws = {
    deserializeAttachment() {
      return attachment;
    },
    serializeAttachment(value) {
      attachment = value;
    }
  };

  try {
    Date.now = () => now;
    const skipped = await broadcaster._persistAgentHistoryIfDue(ws, attachment, {
      serverId: 'server-1',
      historyPartitionId: 42,
      metrics: {
        cpu: 10,
        net_in_speed: 40 * 1024 * 1024,
        net_out_speed: 3 * 1024 * 1024
      },
      regionCode: 'US',
      timestamp: now,
      agentVersion: 'test',
      reportIntervalMs: 60_000
    });

    assert.equal(skipped.persisted, false);
    assert.equal(savedRows.length, 0);

    now += 61_000;
    const persisted = await broadcaster._persistAgentHistoryIfDue(ws, attachment, {
      serverId: 'server-1',
      historyPartitionId: 42,
      metrics: {
        cpu: 12,
        net_in_speed: 128 * 1024,
        net_out_speed: 9 * 1024 * 1024
      },
      regionCode: 'US',
      timestamp: now,
      agentVersion: 'test',
      reportIntervalMs: 60_000
    });

    assert.equal(persisted.persisted, true);
    assert.equal(savedRows.length, 1);
    assert.equal(savedRows[0][4], 11);
    assert.equal(savedRows[0][6], 40 * 1024 * 1024);
    assert.equal(savedRows[0][7], 9 * 1024 * 1024);
    assert.equal(
      Object.prototype.hasOwnProperty.call(
        broadcaster.agentHistoryWrites.get('server-1'),
        'pendingHistoryAggregate'
      ),
      false
    );
  } finally {
    Date.now = originalNow;
  }
});
