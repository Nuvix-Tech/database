import { IndexedQueries } from "../indexed-queries.js";
import { Limit } from "../query/limit.js";
import { Offset } from "../query/offset.js";
import { Cursor } from "../query/cursor.js";
import { Filter } from "../query/filter.js";
import { Order } from "../query/order.js";
import { Select } from "../query/select.js";
import { Doc } from "@core/doc.js";
import { Attribute } from "@validators/schema.js";
import { AttributeEnum } from "@core/enums.js";
import { Base, MethodType } from "@validators/query/base.js";
import { Populate } from "@validators/query/populate.js";

export class Documents extends IndexedQueries {
  /**
   * Documents constructor
   *
   * @param attributes - Array of attributes
   * @param indexes - Array of indexes
   * @param maxValuesCount - Maximum number of values allowed
   * @param minAllowedDate - Minimum allowed date
   * @param maxAllowedDate - Maximum allowed date
   */
  constructor(
    attributes: Doc<Attribute>[] = [],
    indexes: any[],
    maxValuesCount: number = 100,
    allowedValidators: Partial<Record<MethodType, boolean>> = {
      limit: true,
      cursor: true,
      filter: true,
      offset: true,
      order: true,
      populate: true,
      select: true,
    },
    customValidators: Base[] = [],
    minAllowedDate: Date = new Date("0000-01-01"),
    maxAllowedDate: Date = new Date("9999-12-31"),
  ) {
    attributes.push(
      new Doc({
        $id: "$id",
        key: "$id",
        type: AttributeEnum.String,
      }),
      new Doc({
        $id: "$sequence",
        key: "$sequence",
        type: AttributeEnum.Integer,
        size: 8,
      }),
      new Doc({
        $id: "$createdAt",
        key: "$createdAt",
        type: AttributeEnum.Timestamptz,
      }),
      new Doc({
        $id: "$updatedAt",
        key: "$updatedAt",
        type: AttributeEnum.Timestamptz,
      }),
    );

    const validators: Base[] = [];
    const defaultValidators = [
      new Limit(),
      new Offset(),
      new Cursor(),
      new Filter(attributes, maxValuesCount, minAllowedDate, maxAllowedDate),
      new Order(attributes),
      new Select(attributes),
      new Populate(),
    ];

    defaultValidators.forEach((v) => {
      const method = v.getMethodType();
      if (allowedValidators[method]) {
        validators.push(v);
      }
    });
    validators.push(...customValidators);

    super(attributes, indexes, validators);
  }
}
