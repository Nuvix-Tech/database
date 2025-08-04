import { Validator } from "./interface.js";


export class UUID implements Validator {
    $description: string = 'invalid uuid';

    constructor() {

    }

    $valid(value: any): boolean {
        return true;
    };
}
