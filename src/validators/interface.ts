import { AttributeEnum } from "@core/enums.js";

export interface Validator {
  $valid: (value: any) => Promise<boolean> | boolean;
  readonly $description: string;
}

export interface Format {
  type: AttributeEnum;
  callback: (...params: unknown[]) => Validator;
}
