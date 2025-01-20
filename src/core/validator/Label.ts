// src/Validator/Label.ts

import { Key } from './Key'; // Adjust the import based on your project structure

export class Label extends Key {
    protected message: string = 'Value must be a valid string between 1 and 36 chars containing only alphanumeric chars';

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param value - The value to validate
     * @returns {boolean}
     */
    public isValid(value: any): boolean {
        if (!super.isValid(value)) {
            return false;
        }

        // Valid chars: A-Z, a-z, 0-9
        if (/[^A-Za-z0-9]/.test(value)) {
            return false;
        }

        // Check length constraints
        if (value.length < 1 || value.length > 36) {
            return false;
        }

        return true;
    }
}