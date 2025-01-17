import { EntityOptions } from "./options/entityOptions";


export function Entity(options?: EntityOptions): ClassDecorator {
  return function (target: any) {
    Reflect.defineMetadata('entity', options, target);
  };
}