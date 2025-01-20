

export abstract class Validator {

  abstract isValid(value: any): boolean;

  abstract getDescription(): string;

}