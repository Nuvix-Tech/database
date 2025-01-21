import { EntityOptions } from "./options/entityOptions";

export function Entity(options?: EntityOptions): ClassDecorator {
    return function (target: any) {
        Reflect.defineMetadata(
            "entity",
            {
                $id: options?.name,
                $permissions: options?.permissions,
                name: options?.name,
                documentSecurity: options?.documentSecurity,
                database: options?.database,
                schema: options?.schema,
            },
            target,
        );
    };
}
