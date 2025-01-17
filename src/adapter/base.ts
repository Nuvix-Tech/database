interface IDatabaseAdapter {
  connect(): void;
  disconnect(): void;
  send(data: any): void;
  receive(): any;
}

/**
 * Base adapter class
 */
export class DatabaseAdapter implements IDatabaseAdapter {
  connect() {
    throw new Error('Method not implemented.');
  }
  disconnect() {
    throw new Error('Method not implemented.');
  }
  send(data: any) {
    throw new Error('Method not implemented.');
  }
  receive() {
    throw new Error('Method not implemented.');
  }

}