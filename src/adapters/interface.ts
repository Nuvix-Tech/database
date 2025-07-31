

export interface IAdapter {

}

export interface IClient {
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    transaction<T>(callback: (client: any) => Promise<T>): Promise<T>;
}
