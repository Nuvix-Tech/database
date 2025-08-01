
export type Filters<T = any, V = any> = Record<string, {
    encode: (value: T) => string | Promise<V>;
    decode: (value: V) => string | Promise<T>;
}>

export type FilteredValue<T, V> = {
    [K in keyof T]: K extends keyof Filters<T[K], V> ? V : T[K];
};
