import { describe, test } from "vitest";
import { select, table } from "../../src";

describe("select", () => {
    test("single column", ({ expect }) => {
        const myTable = table("table");

        const { query, parameters } = select(myTable.column("a")).from(myTable).toQuery();

        expect(query).eq("SELECT {column_0: Identifier} FROM {table_0: Identifier};");
        expect(parameters).toMatchObject({
            column_0: "a",
            table_0: "table",
        });
    });
});
