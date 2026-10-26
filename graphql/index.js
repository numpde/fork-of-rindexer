const args = process.argv.slice(2);
const connectionString = process.env.RINDEXER_GRAPHQL_DATABASE_URL;
delete process.env.RINDEXER_GRAPHQL_DATABASE_URL;

if (/^postgres(?:ql)?:\/\//i.test((args[0] || "").trimStart())) {
    console.error("Cannot start GraphQL server: database URLs are not accepted in command-line arguments. Supply RINDEXER_GRAPHQL_DATABASE_URL in the child environment.");
    process.exit(1);
}
if (!connectionString || !connectionString.trim()) {
    console.error("Cannot start GraphQL server: RINDEXER_GRAPHQL_DATABASE_URL is missing or empty. Supply the database URL in the child environment before launching the server.");
    process.exit(1);
}
if (args.length < 4 || args.length > 7) {
    console.error("Cannot start GraphQL server: expected schemas and server options. Usage: rindexer-graphql <schemas> <port> <page_limit> <timeout> [<filterOnlyOnIndexedColumns> <disableAdvancedFilters> <skipNodeAliasPlugin>]; supply the database URL through RINDEXER_GRAPHQL_DATABASE_URL.");
    process.exit(1);
}

const schemas = args[0].split(",");
const port = parseInt(args[1]);
let graphqlPageLimit = parseInt(args[2]);
let graphqlTimeout = parseInt(args[3]);
let filterOnlyOnIndexedColumns = args[4] === "true";
let disableAdvancedFilters = args[5] === "true";
let skipNodeAliasPlugin = args[6] === "true";

const express = require('express')
const cors = require('cors')
const { postgraphile } = require("postgraphile");
const { makeWrapResolversPlugin } = require("graphile-utils");
const PgSimplifyInflectorPlugin = require("@graphile-contrib/pg-simplify-inflector");
const ConnectionFilterPlugin = require("postgraphile-plugin-connection-filter");
const { PgNodeAliasPostGraphile } = require("graphile-build-pg");

const byteaToHex = makeWrapResolversPlugin(
    (context) => {
        return context;
    },
    ({ scope }) =>
        async (resolver, user, args, context, _resolveInfo) => {
            if (typeof args === "object") {
                const first = args['first'];
                const last = args['last'];

                const firstValue = first ? parseInt(first) : 0;
                const lastValue = last ? parseInt(last) : 0;

                if (firstValue > graphqlPageLimit || lastValue > graphqlPageLimit) {
                    throw new Error(`Pagination limit exceeded. Maximum allowed is ${graphqlPageLimit}.`)
                }
            }

            // always add a limit on the amount you can bring back if last is defined
            // then let the resolver handle it as limits has been handled above already
            if (args['last'] === undefined) {
                args['first'] = args['first'] ? args['first'] : graphqlPageLimit;
            }

            let result = await resolver();
            if (result && typeof result === "string") {
                // it is a bytea need to turn back to a hex
                result = result.startsWith('\\x') ? result.replace('\\x', '0x') : result;
            }
            return result;
        },
);

let appendPlugins = [byteaToHex, PgSimplifyInflectorPlugin];
if (!disableAdvancedFilters) {
    appendPlugins.push(ConnectionFilterPlugin);
}

const options = {
    watchPg: true,
    host: "localhost",
    disableDefaultMutations: true,
    dynamicJson: true,
    cors: true,
    retryOnInitFail: true,
    enableQueryBatching: true,
    sortExport: true,
    ignoreIndexes: !filterOnlyOnIndexedColumns,
    enhanceGraphiql: false,
    graphiql: false,
    disableQueryLog: true,
    pgSettings: {
        statement_timeout: graphqlTimeout,
    },
    simpleCollections: 'omit',
    appendPlugins,
    // Only skip PgNodeAliasPostGraphile when there are table name conflicts across schemas
    // This avoids node alias conflicts but changes nodeId format
    ...(skipNodeAliasPlugin ? { skipPlugins: [PgNodeAliasPostGraphile] } : {}),
    graphileBuildOptions: {
        pgOmitListSuffix: false,
        pgSimplifyAllRows: false,
        pgShortPk: false,
        connectionFilterAllowedOperators: [
            "isNull",
            "equalTo",
            "notEqualTo",
            "distinctFrom",
            "notDistinctFrom",
            "lessThan",
            "lessThanOrEqualTo",
            "greaterThan",
            "greaterThanOrEqualTo",
            "in",
            "notIn",
        ],
    },
};



const htmlContent = `
    <div style="width: 100%; height: 100%;" id='embedded-sandbox'></div>
    <script src="https://embeddable-sandbox.cdn.apollographql.com/_latest/embeddable-sandbox.umd.production.min.js"></script>
    <script>
      new window.EmbeddedSandbox({
        target: '#embedded-sandbox',
        initialEndpoint: window.location.origin + '/graphql',
      });
    </script>
`;

const app = express()
app.use(cors())
app.use(express.json())
app.use(postgraphile(connectionString, schemas, options))

app.get('/playground', (req, res) => {
    res.send(htmlContent);
});

app.listen(port, "0.0.0.0", () => {
    console.log(`GraphQL endpoint: http://localhost:${port}/graphql`);
    console.log(
        `GraphiQL Playground endpoint: http://localhost:${port}/playground`
    );
});