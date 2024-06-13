function getValueType(value: Table | Column): "table" | "column" {
    if (isColumn(value)) {
        return "column";
    }

    if (isTable(value)) {
        return "table";
    }

    throw ClickifyError.Internal(`could not determine type of value (${value})`);
}

function getValue(value: Table | Column): string {
    if (isColumn(value)) {
        return value.column;
    }

    if (isTable(value)) {
        return value.table;
    }

    throw ClickifyError.Internal(`could not determine value (${value})`);
}

function getClickhouseType(value: Table | Column): "Identifier" {
    if (isColumn(value) || isTable(value)) {
        return "Identifier";
    }

    throw ClickifyError.Internal(`could not determine clickhouse type of value (${value})`);
}

function createQuery() {
    const query: (string | string[])[] = [];
    const parameters: Record<"table" | "column", string[]> = {
        table: [],
        column: [],
    };

    function buildParameterName(type: string, index: number) {
        return `${type}_${index}`;
    }

    function parameterise(value: Table | Column): string {
        const valueType = getValueType(value);
        const parameterName = buildParameterName(valueType, parameters[valueType].length);
        const clickhouseType = getClickhouseType(value);

        const parameter = `{${parameterName}: ${clickhouseType}}`;

        parameters[valueType].push(getValue(value));

        return parameter;
    }

    return {
        push(value: string | Table | Column) {
            if (typeof value === "string") {
                query.push(value);
                return this;
            }

            query.push(parameterise(value));

            return this;
        },

        list(list: Table[] | Column[]) {
            query.push(list.map(parameterise));

            return this;
        },

        build(options?: { semi?: boolean }) {
            let queryStr = query
                .map((part) => {
                    if (typeof part === "string") {
                        return part;
                    }

                    return part.join(", ");
                })
                .join(" ");

            if (options?.semi !== false) {
                queryStr += ";";
            }

            return {
                query: queryStr,
                parameters: Object.entries(parameters).reduce(
                    (finalParameters, [parameterType, parameters]) => {
                        parameters.forEach((parameter, i) => {
                            finalParameters[buildParameterName(parameterType, i)] = parameter;
                        });

                        return finalParameters;
                    },
                    {} as Record<string, string>,
                ),
            };
        },
    };
}

class ClickifyError extends Error {
    static ColumnsRequired() {
        return new ClickifyError("columns are required to make a query");
    }

    static TableRequired() {
        return new ClickifyError("a table must be provided to query");
    }

    static Internal(message: string) {
        return new ClickifyError(`Internal Clickify error: ${message}`);
    }
}

type Table = { table: string };
type Column = { table: string; column: string };

export function table(table: string) {
    return {
        table,
        column: (column: string) => ({ table, column }),
    };
}

export function isTable(value: Table | Column): value is Table {
    return "table" in value;
}

export function isColumn(value: Table | Column): value is Column {
    return "column" in value && typeof value.column === "string";
}

export function select(...cols: Column[]) {
    const columns = cols;
    let table: Table | null = null;

    return {
        select(column: Column) {
            columns.push(column);

            return this;
        },
        from(t: Table) {
            table = t;

            return this;
        },

        toQuery() {
            if (columns.length === 0) {
                throw ClickifyError.ColumnsRequired();
            }

            if (!table) {
                throw ClickifyError.TableRequired();
            }

            const query = createQuery();

            // Begin select statement with columns
            query.push("SELECT");
            query.list(columns);

            // Select table that the query is for
            query.push("FROM");
            query.push(table);

            return query.build();
        },
    };
}
