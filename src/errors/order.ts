import { DatabaseError } from "./base.js";

export default class Order extends DatabaseError {
  constructor(
    message: string,
    public order?: string,
  ) {
    super(message);
    this.name = "OrderException";
    this.order = order;
  }

  get() {
    return this.order;
  }
}
