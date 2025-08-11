import { Validator } from "./interface.js";

export class Json implements Validator {
  $description: string = "invalid json";

  constructor() {}

  $valid(value: any): boolean {
    return true;
  }
}
