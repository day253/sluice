var SluiceLoadLab = (function () {
  'use strict';

  const LIMITS = Object.freeze({
    maxTenants: 100,
    maxTasks: 100000,
    maxTasksPerTenant: 5000,
    maxWaves: 20,
    tenantPrefix: 'load-lab-',
    minRandomTenants: 3,
    maxRandomTenants: 7
  });
  const SUBMISSION_CONCURRENCY = Object.freeze({
    autoStart: 8,
    autoMin: 4,
    autoMax: 16,
    manual: Object.freeze([4, 8, 16, 32]),
    slowMilliseconds: 1000
  });

  const RANDOM_TENANT_ADJECTIVES = Object.freeze([
    'Amber', 'Atlas', 'Blue', 'Cedar', 'Cloud', 'Copper',
    'Delta', 'Evergreen', 'Maple', 'Nimbus', 'Northstar', 'Silver'
  ]);
  const RANDOM_TENANT_SECTORS = Object.freeze([
    'Analytics', 'Commerce', 'Energy', 'Finance', 'Foods', 'Health',
    'Logistics', 'Media', 'Mobility', 'Retail', 'Systems', 'Travel'
  ]);
  const RANDOM_TENANT_LIMITS = Object.freeze([5, 10, 20, 30, 50, 60, 100, 200, 500]);

  const RECIPES = Object.freeze([
    {
      id: 'hundred-tenant-burst',
      name: '100-tenant burst',
      description: 'Create 100 tenants, then submit 200 tasks per tenant in round-robin order.',
      source: 'PERF-001 shape',
      options: {
        tenantCount: 100, tasksPerTenant: 200, quota: 50,
        quotaProfile: 'equal', loadShape: 'even', delivery: 'burst', waves: 1
      }
    },
    {
      id: 'quota-tier-contention',
      name: 'Tiered quota contention',
      description: 'Mix 5 / 20 / 100 worker limits while every tenant receives the same burst.',
      source: 'TestOversubscription',
      options: {
        tenantCount: 60, tasksPerTenant: 200, quota: 50,
        quotaProfile: 'tiered', loadShape: 'even', delivery: 'burst', waves: 1
      }
    },
    {
      id: 'hot-tenant-borrowing',
      name: 'Hot tenant + cold tail',
      description: 'One tenant receives 100× the base load while 99 tenants remain lightly active.',
      source: 'TestAdaptiveIdleBorrowing',
      options: {
        tenantCount: 100, tasksPerTenant: 50, quota: 30,
        quotaProfile: 'equal', loadShape: 'hotspot', delivery: 'burst', waves: 1
      }
    },
    {
      id: 'wave-arrivals',
      name: 'Five arrival waves',
      description: 'Submit 10,000 tasks across 50 tenants in five observable one-second waves.',
      source: 'SCHED-004 shape',
      options: {
        tenantCount: 50, tasksPerTenant: 200, quota: 40,
        quotaProfile: 'ramp', loadShape: 'even', delivery: 'waves', waves: 5
      }
    }
  ]);

  function integer(value, fallback, min, max) {
    const parsed = Math.round(Number(value));
    return Math.min(max, Math.max(min, Number.isFinite(parsed) ? parsed : fallback));
  }

  function normalizeOptions(input) {
    const options = input || {};
    const normalized = {
      tenantCount: integer(options.tenantCount, 100, 1, LIMITS.maxTenants),
      tasksPerTenant: integer(options.tasksPerTenant, 200, 1, LIMITS.maxTasksPerTenant),
      quota: integer(options.quota, 50, 1, 100000),
      quotaProfile: ['equal', 'tiered', 'ramp'].includes(options.quotaProfile)
        ? options.quotaProfile : 'equal',
      loadShape: ['even', 'hotspot', 'pyramid'].includes(options.loadShape)
        ? options.loadShape : 'even',
      delivery: options.delivery === 'waves' ? 'waves' : 'burst',
      waves: integer(options.waves, 5, 1, LIMITS.maxWaves)
    };
    if (normalized.delivery === 'burst') normalized.waves = 1;
    return normalized;
  }

  function quotaFor(options, index) {
    if (options.quotaProfile === 'tiered') return [5, 20, 100][index % 3];
    if (options.quotaProfile === 'ramp' && options.tenantCount > 1) {
      const low = Math.max(1, Math.round(options.quota / 4));
      return Math.round(low + (options.quota - low) * index / (options.tenantCount - 1));
    }
    return options.quota;
  }

  function tasksFor(options, index) {
    if (options.loadShape === 'hotspot') {
      return index === 0 ? options.tasksPerTenant * 100 : options.tasksPerTenant;
    }
    if (options.loadShape === 'pyramid') {
      return options.tasksPerTenant * [1, 3, 8][index % 3];
    }
    return options.tasksPerTenant;
  }

  function buildTenantSpecs(input, label) {
    const options = normalizeOptions(input);
    const specs = [];
    let totalTasks = 0;
    for (let index = 0; index < options.tenantCount; index++) {
      const ordinal = index + 1;
      const taskCount = tasksFor(options, index);
      if (taskCount > LIMITS.maxTasksPerTenant) {
        throw new Error(
          `Tenant ${LIMITS.tenantPrefix}${String(ordinal).padStart(3, '0')} contains ` +
          `${taskCount} tasks; the per-tenant safety limit is ${LIMITS.maxTasksPerTenant}.`
        );
      }
      totalTasks += taskCount;
      specs.push({
        id: LIMITS.tenantPrefix + String(ordinal).padStart(3, '0'),
        name: `Load Lab ${String(ordinal).padStart(3, '0')} · ${label || 'Custom'}`,
        maxWorkers: quotaFor(options, index),
        taskCount
      });
    }
    if (totalTasks > LIMITS.maxTasks) {
      throw new Error(`Load contains ${totalTasks} tasks; the browser safety limit is ${LIMITS.maxTasks}.`);
    }
    return specs;
  }

  function buildRoundRobinJobs(specs) {
    const offsets = specs.map(() => 0);
    const jobs = [];
    let active = specs.length;
    while (active > 0) {
      active = 0;
      for (let index = 0; index < specs.length; index++) {
        if (offsets[index] >= specs[index].taskCount) continue;
        jobs.push({tenant: specs[index].id, index: offsets[index]});
        offsets[index]++;
        if (offsets[index] < specs[index].taskCount) active++;
      }
    }
    if (jobs.length > LIMITS.maxTasks) {
      throw new Error(`Load contains ${jobs.length} tasks; the browser safety limit is ${LIMITS.maxTasks}.`);
    }
    return jobs;
  }

  function normalizeSeed(value) {
    const parsed = Number(value);
    const seed = Number.isFinite(parsed) ? Math.trunc(parsed) >>> 0 : 0;
    return seed || 0x9e3779b9;
  }

  function buildRandomTenantConfigs(existingIDs, seedValue) {
    const existing = new Set((existingIDs || []).map(id => String(id)));
    const seed = normalizeSeed(seedValue);
    let state = seed;
    const next = maximum => {
      state ^= state << 13;
      state ^= state >>> 17;
      state ^= state << 5;
      return (state >>> 0) % maximum;
    };
    const count = LIMITS.minRandomTenants +
      next(LIMITS.maxRandomTenants - LIMITS.minRandomTenants + 1);
    const configs = [];
    const names = new Set();
    const generation = seed.toString(36).padStart(7, '0');
    for (let index = 0; index < count; index++) {
      const adjective = RANDOM_TENANT_ADJECTIVES[next(RANDOM_TENANT_ADJECTIVES.length)];
      const sector = RANDOM_TENANT_SECTORS[next(RANDOM_TENANT_SECTORS.length)];
      let name = `${adjective} ${sector}`;
      if (names.has(name)) name += ` ${index + 1}`;
      names.add(name);

      const baseID = `sample-${generation}-${String(index + 1).padStart(2, '0')}`;
      let id = baseID;
      let collision = 1;
      while (existing.has(id)) {
        id = `${baseID}-${collision++}`;
      }
      existing.add(id);
      configs.push({
        id,
        name,
        maxWorkers: RANDOM_TENANT_LIMITS[next(RANDOM_TENANT_LIMITS.length)]
      });
    }
    return configs;
  }

  function splitWaves(jobs, requestedWaves) {
    const waveCount = Math.min(integer(requestedWaves, 1, 1, LIMITS.maxWaves), Math.max(1, jobs.length));
    const waves = [];
    let offset = 0;
    for (let index = 0; index < waveCount; index++) {
      const remaining = jobs.length - offset;
      const size = Math.ceil(remaining / (waveCount - index));
      waves.push(jobs.slice(offset, offset + size));
      offset += size;
    }
    return waves;
  }

  function summarize(input, label) {
    const options = normalizeOptions(input);
    const specs = buildTenantSpecs(options, label);
    const totalTasks = specs.reduce((sum, spec) => sum + spec.taskCount, 0);
    return {options, tenantCount: specs.length, totalTasks, specs};
  }

  function recipe(id) {
    return RECIPES.find(item => item.id === id) || null;
  }

  function normalizeSubmissionMode(value) {
    if (value === 'auto' || value === undefined || value === null) return 'auto';
    const parsed = Number(value);
    return SUBMISSION_CONCURRENCY.manual.includes(parsed) ? String(parsed) : 'auto';
  }

  // Auto uses additive increase and multiplicative decrease. It controls only
  // browser request concurrency; the Leader remains the authoritative,
  // bounded Raft ingress boundary.
  function createSubmissionController(value) {
    const mode = normalizeSubmissionMode(value);
    const fixed = mode === 'auto' ? null : Number(mode);
    const state = {
      mode,
      current: fixed || SUBMISSION_CONCURRENCY.autoStart,
      maxObserved: fixed || SUBMISSION_CONCURRENCY.autoStart,
      backoffs: 0,
      successful: 0
    };
    state.observe = sample => {
      if (fixed) return state.current;
      const failed = Boolean(sample && sample.failed);
      const backpressured = Boolean(sample && sample.backpressured);
      const latency = Math.max(0, Number(sample && sample.latencyMs) || 0);
      if (failed || backpressured || latency >= SUBMISSION_CONCURRENCY.slowMilliseconds) {
        state.current = Math.max(
          SUBMISSION_CONCURRENCY.autoMin,
          Math.floor(state.current / 2)
        );
        state.backoffs++;
        state.successful = 0;
        return state.current;
      }
      state.successful++;
      if (state.successful >= state.current) {
        state.current = Math.min(SUBMISSION_CONCURRENCY.autoMax, state.current + 2);
        state.maxObserved = Math.max(state.maxObserved, state.current);
        state.successful = 0;
      }
      return state.current;
    };
    return state;
  }

  // Keep the pipeline full as each request settles instead of imposing
  // Promise.all wave barriers. The controller may change its target while the
  // operation is running.
  function runRolling(items, controller, work, onSettled, shouldStop) {
    return new Promise(resolve => {
      const results = new Array(items.length);
      let next = 0;
      let active = 0;
      const pump = () => {
        while (
          next < items.length &&
          active < controller.current &&
          !(shouldStop && shouldStop())
        ) {
          const index = next++;
          const started = Date.now();
          active++;
          Promise.resolve()
            .then(() => work(items[index], index))
            .then(
              value => ({status: 'fulfilled', value}),
              reason => ({status: 'rejected', reason})
            )
            .then(result => {
              active--;
              results[index] = result;
              const status = Number(result.reason && result.reason.status) || 0;
              controller.observe({
                latencyMs: Date.now() - started,
                failed: result.status === 'rejected',
                backpressured: status === 429 || status === 503
              });
              if (onSettled) onSettled(result, items[index], index, controller);
              if (
                active === 0 &&
                (next >= items.length || (shouldStop && shouldStop()))
              ) {
                resolve(results);
                return;
              }
              pump();
            });
        }
        if (
          active === 0 &&
          (next >= items.length || (shouldStop && shouldStop()))
        ) {
          resolve(results);
        }
      };
      pump();
    });
  }

  return Object.freeze({
    LIMITS,
    SUBMISSION_CONCURRENCY,
    RECIPES,
    normalizeOptions,
    buildTenantSpecs,
    buildRandomTenantConfigs,
    buildRoundRobinJobs,
    splitWaves,
    summarize,
    recipe,
    normalizeSubmissionMode,
    createSubmissionController,
    runRolling
  });
})();
