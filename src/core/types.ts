
export type FilterValue = string | number | boolean | Date | null | undefined | object | any[] | Record<string, any> | FilterValue[];

export type Filter<T = FilterValue, U = FilterValue> = {
    encode: (value: T) => U | Promise<U>;
    decode: (value: U) => T | Promise<T>;
};

export type Filters = Record<string, Filter>;
