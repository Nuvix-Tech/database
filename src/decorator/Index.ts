import { Document } from "../core/Document";
import { IndexOptions } from "./options/IndexOptions";

export function Index(options?: IndexOptions): ClassDecorator {
    return function (target: any) {
        const indexes =
            Reflect.getMetadata("indexes", (target as Object).constructor) ||
            [];

        indexes.push(new Document<any>(options));

        Reflect.defineMetadata("indexes", indexes, target.constructor);
    };
}
