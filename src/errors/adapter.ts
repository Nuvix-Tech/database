import { DatabaseError } from "./base";

export class InitializeError extends DatabaseError {
    constructor(message: string) {
        super(message);
        this.name = "InitializeError";
    }
}
