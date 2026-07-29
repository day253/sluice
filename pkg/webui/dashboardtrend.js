var SluiceDashboardTrend = (function () {
  'use strict';

  var PERIODS = ['days', 'hours', 'mins', 'secs'];

  function number(value) {
    value = Number(value);
    return Number.isFinite(value) ? value : 0;
  }

  function recordSample(store, key, values, limit) {
    limit = Math.max(2, Math.floor(number(limit) || 60));
    var entry = store[key] || (store[key] = {});
    Object.keys(values || {}).forEach(function (name) {
      var samples = entry[name] || (entry[name] = []);
      samples.push(number(values[name]));
      if (samples.length > limit) {
        samples.splice(0, samples.length - limit);
      }
    });
    return entry;
  }

  function prune(store, activeKeys) {
    var active = Object.create(null);
    (activeKeys || []).forEach(function (key) {
      active[key] = true;
    });
    Object.keys(store).forEach(function (key) {
      if (!active[key]) {
        delete store[key];
      }
    });
  }

  function historyValues(item) {
    var history = item && item.history || {};
    return PERIODS.reduce(function (values, period) {
      return values.concat((history[period] || []).map(number));
    }, []);
  }

  function score(item) {
    return Math.max.apply(Math, [number(item && item.current)].concat(historyValues(item)));
  }

  function aggregateHistory(items, divisor) {
    divisor = Math.max(1, number(divisor) || 1);
    var result = {};
    PERIODS.forEach(function (period) {
      var width = (items[0] && items[0].history && items[0].history[period] || []).length;
      result[period] = Array.from({ length: width }, function (_, index) {
        return items.reduce(function (sum, item) {
          return sum + number(item && item.history && item.history[period] && item.history[period][index]);
        }, 0) / divisor;
      });
    });
    return result;
  }

  function collapseSeries(items, limit, noun, dropZero, aggregateMode) {
    limit = Math.max(1, Math.floor(number(limit) || 8));
    var ranked = (items || []).map(function (item) {
      return { item: item, score: score(item) };
    }).filter(function (candidate) {
      return !dropZero || candidate.score > 0;
    }).sort(function (left, right) {
      return right.score - left.score ||
        String(left.item.name || left.item.id).localeCompare(
          String(right.item.name || right.item.id),
          undefined,
          { numeric: true, sensitivity: 'base' }
        );
    });
    var visible = ranked.slice(0, limit).map(function (candidate) {
      return candidate.item;
    });
    var remainder = ranked.slice(limit).map(function (candidate) {
      return candidate.item;
    });
    var currentTotal = ranked.reduce(function (sum, candidate) {
      return sum + number(candidate.item.current);
    }, 0);
    var limitTotal = ranked.reduce(function (sum, candidate) {
      return sum + number(candidate.item.limit);
    }, 0);
    if (!remainder.length) {
      return {
        series: visible, hidden: 0, total: ranked.length,
        currentTotal: currentTotal, limitTotal: limitTotal
      };
    }
    var average = aggregateMode === 'average';
    var divisor = average ? remainder.length : 1;
    var hasLimit = remainder.some(function (item) {
      return Number.isFinite(item.limit);
    });
    visible.push({
      id: '__others__',
      name: 'Other ' + remainder.length + ' ' + (noun || 'series') +
        (average ? ' (avg)' : ''),
      current: remainder.reduce(function (sum, item) {
        return sum + number(item.current);
      }, 0) / divisor,
      limit: hasLimit ? remainder.reduce(function (sum, item) {
        return sum + number(item.limit);
      }, 0) / divisor : undefined,
      history: aggregateHistory(remainder, divisor),
      aggregate: true
    });
    return {
      series: visible, hidden: remainder.length, total: ranked.length,
      currentTotal: currentTotal, limitTotal: limitTotal
    };
  }

  function stackRows(rows) {
    var width = (rows || []).reduce(function (maximum, row) {
      return Math.max(maximum, (row && row.data || []).length);
    }, 0);
    var cumulative = Array(width).fill(0);
    return (rows || []).map(function (row) {
      var data = Array.from({ length: width }, function (_, index) {
        return number(row && row.data && row.data[index]);
      });
      var base = cumulative.slice();
      var top = data.map(function (value, index) {
        return base[index] + value;
      });
      cumulative = top;
      return Object.assign({}, row, {
        data: data,
        base: base,
        top: top
      });
    });
  }

  return {
    PERIODS: PERIODS,
    recordSample: recordSample,
    prune: prune,
    score: score,
    collapseSeries: collapseSeries,
    stackRows: stackRows
  };
})();
