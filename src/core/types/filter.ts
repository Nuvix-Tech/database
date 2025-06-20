export interface Filter {
    /**
     * Filter function , encodes the value
     */
    encode: (value: any, ...args: any) => any | Promise<any>;

    /**
     * Filter function , decodes the value
     */
    decode: (value: any, ...args: any) => any | Promise<any>;
}
