import { DatabaseError } from "./base.js";

export class InitializeError extends DatabaseError {
    constructor(message: string) {
        super(message);
        this.name = "InitializeError";
    }
}
