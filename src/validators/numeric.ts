import { Validator } from "./interface.js";

export class Numeric implements Validator {
    public readonly $description: string = "Value must be a valid number";

    public $valid(value: any) {
        return typeof value === "number" || !isNaN(Number(value));
    };
}
