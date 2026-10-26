const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'index.js'), 'utf8');
const sentinel = 'postgresql://test_user:FAKE_PASSWORD_SENTINEL@127.0.0.1:1/test_db';
const publicArguments = ['first,second', '43210', '1000', '10000', 'true', 'false', 'true'];

function runEntrypoint(arguments_, environment) {
    const output = { messages: [], exitCode: undefined, database: undefined };
    const app = { use() {}, get() {}, listen(_port, _host, ready) { ready(); } };
    const dependencies = {
        express: Object.assign(() => app, { json() {} }),
        cors: () => {},
        postgraphile: { postgraphile(connection, schemas, options) {
            output.database = { connection, schemas, options };
        } },
        'graphile-utils': { makeWrapResolversPlugin() {} },
        '@graphile-contrib/pg-simplify-inflector': {},
        'postgraphile-plugin-connection-filter': {},
        'graphile-build-pg': { PgNodeAliasPostGraphile: {} },
    };
    const stopped = new Error('fixture process exit');
    const process_ = { argv: ['node', 'index.js', ...arguments_], env: { ...environment },
        exit(code) { output.exitCode = code; throw stopped; } };
    try {
        vm.runInNewContext(source, {
            require(name) { assert.ok(Object.hasOwn(dependencies, name), `unexpected dependency: ${name}`); return dependencies[name]; },
            process: process_,
            console: { error(message) { output.messages.push(String(message)); }, log(message) { output.messages.push(String(message)); } },
        });
    } catch (error) {
        if (error !== stopped) throw error;
    }
    output.remainingEnvironment = process_.env;
    return output;
}

test('entrypoint reads credentials from the child environment and preserves public options', () => {
    const result = runEntrypoint(publicArguments, { RINDEXER_GRAPHQL_DATABASE_URL: sentinel });
    assert.equal(result.exitCode, undefined);
    assert.equal(result.database.connection, sentinel);
    assert.equal(JSON.stringify(result.database.schemas), JSON.stringify(['first', 'second']));
    assert.equal(result.database.options.ignoreIndexes, false);
    assert.equal(result.database.options.pgSettings.statement_timeout, 10000);
    assert.equal(result.database.options.skipPlugins.length, 1);
    assert.ok(result.messages.some(message => message.includes('http://localhost:43210/graphql')));
    assert.equal(result.database.options.appendPlugins.length, 3);
    assert.ok(!Object.hasOwn(result.remainingEnvironment, 'RINDEXER_GRAPHQL_DATABASE_URL'), 'do not retain an extra credential in the child environment after reading it');
    assert.ok(result.messages.every(message => !message.includes('FAKE_PASSWORD_SENTINEL')));
});

test('entrypoint rejects absent or empty credentials without accepting a positional fallback', () => {
    for (const environment of [{}, { RINDEXER_GRAPHQL_DATABASE_URL: '' }, { RINDEXER_GRAPHQL_DATABASE_URL: '   ' }]) {
        const result = runEntrypoint(publicArguments, environment);
        assert.equal(result.exitCode, 1);
        assert.equal(result.database, undefined);
        assert.ok(result.messages.some(message => message.includes('RINDEXER_GRAPHQL_DATABASE_URL')));
    }
});

test('legacy positional database URLs are rejected without echoing their credential', () => {
    for (const environment of [{}, { RINDEXER_GRAPHQL_DATABASE_URL: sentinel }]) {
        for (const arguments_ of [[sentinel, ...publicArguments], [sentinel, ...publicArguments.slice(0, 4)]]) {
            const result = runEntrypoint(arguments_, environment);
            assert.equal(result.exitCode, 1);
            assert.equal(result.database, undefined);
            assert.ok(result.messages.every(message => !message.includes('FAKE_PASSWORD_SENTINEL')));
        }
    }
});
