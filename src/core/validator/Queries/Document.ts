import { Queries } from '../Queries';
import {Database} from '../../database';
import { Document as DatabaseDocument } from '../../Document';
import { Select } from '../Query/Select';

export class Document extends Queries {
  /**
   * Document constructor.
   *
   * @param attributes - Array of attributes
   */
  constructor(attributes: DatabaseDocument[]) {
    // Add default attributes
    attributes.push(new DatabaseDocument({
      '$id': '$id',
      'key': '$id',
      'type': Database.VAR_STRING,
      'array': false,
    }));
    attributes.push(new DatabaseDocument({
      '$id': '$createdAt',
      'key': '$createdAt',
      'type': Database.VAR_DATETIME,
      'array': false,
    }));
    attributes.push(new DatabaseDocument({
      '$id': '$updatedAt',
      'key': '$updatedAt',
      'type': Database.VAR_DATETIME,
      'array': false,
    }));

    const validators = [
      new Select(attributes),
    ];

    super(validators);
  }
}