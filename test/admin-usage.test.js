import assert from 'node:assert/strict';
import { estimateDurableObjectsBillableRequests } from '../src/handlers/admin.js';

assert.equal(estimateDurableObjectsBillableRequests(0), 0);
assert.equal(estimateDurableObjectsBillableRequests(1), 1);
assert.equal(estimateDurableObjectsBillableRequests(20), 1);
assert.equal(estimateDurableObjectsBillableRequests(21), 2);
assert.equal(estimateDurableObjectsBillableRequests(100), 5);
assert.equal(estimateDurableObjectsBillableRequests('101'), 6);

console.log('admin usage tests passed');
