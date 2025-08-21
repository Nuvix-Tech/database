import { StructureException } from "@errors/index.js";

export enum RoleName {
  ANY = "any",
  GUESTS = "guests",
  USERS = "users",
  USER = "user",
  TEAM = "team",
  MEMBER = "member",
  LABEL = "label",
}

export enum UserDimension {
  VERIFIED = "verified",
  UNVERIFIED = "unverified",
}

/**
 * Represents a structured role with a name, optional identifier, and optional dimension.
 * Provides methods for string conversion and parsing from a string.
 */
export class Role {
  constructor(
    private _role: RoleName,
    private _identifier: string | null = null,
    private _dimension: string | null = null,
  ) {
    if (_identifier === "") this._identifier = null;
    if (_dimension === "") this._dimension = null;
  }

  /**
   * Create a role string from this Role instance
   * Format: "roleName:identifier/dimension" (parts are optional)
   *
   * @returns {string} The string representation of the role.
   */
  public toString(): string {
    let str: string = this._role;

    if (this._identifier !== null) {
      str += `:${this._identifier}`;
    }

    if (this._dimension !== null) {
      if (
        this._identifier === null &&
        this._role !== RoleName.ANY &&
        this._role !== RoleName.GUESTS
      ) {
        str += `/${this._dimension}`;
      } else if (this._identifier !== null) {
        str += `/${this._dimension}`;
      } else {
        str += `/${this._dimension}`;
      }
    }
    return str;
  }

  public toObject(): string {
    return this.toString(); // For consistency, returning the string representation.
  }

  public toJSON(): string {
    return this.toString();
  }

  /**
   * @returns {RoleName} The base name of the role (e.g., "user", "team").
   */
  public get role(): RoleName {
    return this._role;
  }

  /**
   * @returns {string | null} The associated identifier for the role, or null if none.
   */
  public get identifier(): string | null {
    return this._identifier;
  }

  /**
   * @returns {string | null} The associated dimension for the role, or null if none.
   */
  public get dimension(): string | null {
    return this._dimension;
  }

  /**
   * Parse a role string into a Role object.
   * Supports formats: "roleName", "roleName:identifier", "roleName/dimension", "roleName:identifier/dimension".
   *
   * @param {string} roleString - The role string to parse.
   * @returns {Role} A new Role instance.
   * @throws {StructureException} If the role string is invalid or cannot be parsed.
   */
  public static parse(roleString: string): Role {
    if (typeof roleString !== "string" || roleString.trim() === "") {
      throw new StructureException("Role string cannot be empty.");
    }

    let roleNamePart: string;
    let identifierPart: string | null = null;
    let dimensionPart: string | null = null;

    const colonSplit = roleString.split(":");
    roleNamePart = colonSplit[0]!;

    if (colonSplit.length > 2) {
      throw new StructureException("Invalid role format: too many colons.");
    }

    if (colonSplit.length === 2) {
      const identifierOrDimensionPart = colonSplit[1]!;

      const slashSplit = identifierOrDimensionPart.split("/");
      if (slashSplit.length > 2) {
        throw new StructureException(
          "Invalid role format: too many slashes in identifier/dimension section.",
        );
      }

      if (slashSplit.length === 2) {
        identifierPart = slashSplit[0] || null; // Could be empty string if ":/dimension"
        dimensionPart = slashSplit[1] || null; // Could be empty string if "id/"
      } else {
        identifierPart = slashSplit[0] || null; // If no slash, this whole part is identifier
      }
    } else {
      // No colon means no identifier, but could have dimension directly
      const slashSplit = roleNamePart.split("/");
      if (slashSplit.length > 2) {
        throw new StructureException(
          "Invalid role format: too many slashes in roleName section.",
        );
      }
      if (slashSplit.length === 2) {
        roleNamePart = slashSplit[0]!;
        dimensionPart = slashSplit[1] || null;
      }
    }

    if (!Object.values(RoleName).includes(roleNamePart as RoleName)) {
      throw new StructureException(`Invalid role name: "${roleNamePart}".`);
    }

    if (identifierPart === "") identifierPart = null;
    if (dimensionPart === "") dimensionPart = null;

    if (
      dimensionPart !== null &&
      identifierPart === null &&
      colonSplit.length === 2
    ) {
      // Case: "role:/dimension" -> invalid as identifier must precede dimension if colon is used
      throw new StructureException(
        "Invalid role format: dimension cannot follow an empty identifier with a colon.",
      );
    }

    // Additional sanity check for `any` and `guests` roles, which should not have identifier/dimension
    if (
      (roleNamePart === RoleName.ANY || roleNamePart === RoleName.GUESTS) &&
      (identifierPart !== null || dimensionPart !== null)
    ) {
      throw new StructureException(
        `Role "${roleNamePart}" cannot have an identifier or dimension.`,
      );
    }

    return new Role(roleNamePart as RoleName, identifierPart, dimensionPart);
  }

  /**
   * Create a user role with an ID and optional status (dimension).
   * @param identifier - The user ID.
   * @param status - User status (e.g., 'verified', 'unverified'). Defaults to null.
   * @returns {Role}
   */
  public static user(
    identifier: string,
    status: UserDimension | null = null,
  ): Role {
    if (!identifier)
      throw new StructureException("User role must have an identifier.");
    return new Role(RoleName.USER, identifier, status);
  }

  /**
   * Create a users role with an optional status (dimension).
   * @param status - User status (e.g., 'verified', 'unverified'). Defaults to null.
   * @returns {Role}
   */
  public static users(status: UserDimension | null = null): Role {
    return new Role(RoleName.USERS, null, status);
  }

  /**
   * Create a team role with an ID and optional dimension.
   * @param identifier - The team ID.
   * @param dimension - Optional team dimension. Defaults to null.
   * @returns {Role}
   */
  public static team(
    identifier: string,
    dimension: string | null = null,
  ): Role {
    if (!identifier)
      throw new StructureException("Team role must have an identifier.");
    return new Role(RoleName.TEAM, identifier, dimension);
  }

  /**
   * Create a label role with an ID.
   * @param identifier - The label ID.
   * @returns {Role}
   */
  public static label(identifier: string): Role {
    if (!identifier)
      throw new StructureException("Label role must have an identifier.");
    return new Role(RoleName.LABEL, identifier, null);
  }

  /**
   * Create an 'any' role.
   * @returns {Role}
   */
  public static any(): Role {
    return new Role(RoleName.ANY, null, null);
  }

  /**
   * Create a 'guests' role.
   * @returns {Role}
   */
  public static guests(): Role {
    return new Role(RoleName.GUESTS, null, null);
  }

  /**
   * Create a member role with an ID.
   * @param identifier - The member ID.
   * @returns {Role}
   */
  public static member(identifier: string): Role {
    if (!identifier)
      throw new StructureException("Member role must have an identifier.");
    return new Role(RoleName.MEMBER, identifier, null);
  }

  /**
   * Create a custom role with the given role name, identifier, and dimension.
   * Use this for roles not covered by specific static methods or for dynamic creation.
   * @param roleName - The name of the custom role.
   * @param identifier - Optional identifier. Defaults to null.
   * @param dimension - Optional dimension. Defaults to null.
   * @returns {Role}
   */
  public static custom(
    roleName: RoleName,
    identifier: string | null = null,
    dimension: string | null = null,
  ): Role {
    return new Role(roleName, identifier, dimension);
  }
}
