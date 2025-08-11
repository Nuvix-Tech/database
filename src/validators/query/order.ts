import { Doc } from "@core/doc.js";
import { Base, MethodType } from "./base.js";
import { Query, QueryType } from "@core/query.js";
import { Attribute } from "@validators/schema.js";

export class Order extends Base {
  private readonly schema: Record<string, Doc<Attribute>> = {};

  constructor(attributes: Doc<Attribute>[] = []) {
    super();
    this.buildSchema(attributes);
  }

  private buildSchema(attributes: Doc<Attribute>[]): void {
    for (const attribute of attributes) {
      const key = attribute.get("key") ?? attribute.get("$id");
      if (key) {
        this.schema[key] = attribute;
      }
    }
  }

  protected isValidAttribute(attribute: string): boolean {
    if (!this.schema[attribute]) {
      this.message = `Attribute not found in schema: ${attribute}`;
      return false;
    }
    return true;
  }

  public $valid(value: unknown): boolean {
    if (!(value instanceof Query)) {
      this.message = "Value must be a Query instance";
      return false;
    }

    const method = value.getMethod();
    const attribute = value.getAttribute();

    if (!this.isValidOrderMethod(method)) {
      this.message = "Invalid order method";
      return false;
    }

    return attribute === "" || this.isValidAttribute(attribute);
  }

  private isValidOrderMethod(method: string): boolean {
    return method === QueryType.OrderAsc || method === QueryType.OrderDesc;
  }

  public getMethodType(): MethodType {
    return MethodType.Order;
  }
}
