var SluiceLoadLab = (function () {
  'use strict';

  const LIMITS = Object.freeze({
    maxTenants: 100,
    maxTasks: 100000,
    maxTasksPerTenant: 5000,
    maxWaves: 20,
    tenantPrefix: 'load-lab-'
  });

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

  return Object.freeze({
    LIMITS,
    RECIPES,
    normalizeOptions,
    buildTenantSpecs,
    buildRoundRobinJobs,
    splitWaves,
    summarize,
    recipe
  });
})();
