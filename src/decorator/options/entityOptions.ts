import Permission from "../../security/Permission";


export interface EntityOptions {
  /**
   * The name of the entity
   */
  name: string;

  documentSecurity?: boolean;

  permissions?: Permission[] | string[]

  /**
   * The name of the database
   */
  database?: string;

  /**
   * The schema of the entity
   */
  schema?: string;
}