
export interface Validator {
    $valid: (value: any) => Promise<boolean> | boolean;
    readonly $description: string;
}
