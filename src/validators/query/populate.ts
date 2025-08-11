import { Base, MethodType } from "./base.js";

export class Populate extends Base {
  $valid(query: unknown): boolean {
    return true;
  }

  getMethodType(): MethodType {
    return MethodType.Populate;
  }
}
