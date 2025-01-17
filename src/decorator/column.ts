import { ColumnOptions } from "./options/columnOptions";


export function Column(options?: ColumnOptions): any {
  return function (target: Object, propertyKey: string | symbol) {
    const columns = Reflect.getMetadata('columns', (target as Object).constructor) || [];
    columns.push({
      $id: propertyKey,
      options,
    });
    Reflect.defineMetadata('columns', columns, target.constructor);
  };
}