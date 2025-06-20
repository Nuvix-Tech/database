export interface Attribute {
    $id: string;
    type: string;
    size: number;
    format?: string;
    signed?: boolean;
    required?: boolean;
    default?: any | null;
    array?: boolean;
    filters?: any[];
    options?: { [key: string]: any };
}
