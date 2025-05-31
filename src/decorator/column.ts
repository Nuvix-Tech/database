import { Document } from "../core/Document";
import type { ColumnOptions } from "./options/columnOptions";

export function Column(options?: ColumnOptions): any {
    return function (target: Object, propertyKey: string | symbol) {
        const columns =
            Reflect.getMetadata("columns", (target as Object).constructor) ||
            [];
        const typeMap: { [key: string]: string } = {
            string: "string",
            boolean: "boolean",
            integer: "integer",
            datetime: "datetime",
            varchar: "string",
            text: "string",
            json: "string",
            timestamp: "datetime",
        };

        const filters = options?.filters || [];
        if (options?.type === "json" && !filters.includes("json")) {
            filters.push("json");
        }
        if (
            (options?.type === "datetime" || options?.type === "timestamp") &&
            !filters.includes("datetime")
        ) {
            filters.push("datetime");
        }

        columns.push(
            new Document<any>({
                $id: propertyKey,
                key: propertyKey,
                type: options?.type ? typeMap[options?.type] : options?.type,
                size: options?.size || options?.length || undefined,
                required:
                    options?.required !== undefined
                        ? options.required
                        : !options?.nullable,
                signed: options?.signed || false,
                array: options?.array || false,
                filters: filters,
                default: options?.default || undefined,
            }),
        );
        Reflect.defineMetadata("columns", columns, target.constructor);
    };
}
