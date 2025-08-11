import { Role as RoleParser, RoleName, UserDimension } from "../utils/role.js";
import { Validator } from "./interface.js";
import { Key } from "./key.js";
import { Label } from "./label.js";

interface RoleConfigDetail {
  allowed: boolean;
  required: boolean;
  options?: string[];
}

interface RoleConfiguration {
  identifier: RoleConfigDetail;
  dimension: RoleConfigDetail;
}

/**
 * Roles Validator
 *
 * Validates an array of role strings based on defined configurations and constraints.
 */
export class Roles implements Validator {
  public static readonly ROLES_LIST: RoleName[] = Object.values(RoleName);
  public static readonly USER_DIMENSIONS_LIST: UserDimension[] =
    Object.values(UserDimension);

  public static readonly CONFIG: Record<RoleName, RoleConfiguration> = {
    [RoleName.ANY]: {
      identifier: { allowed: false, required: false },
      dimension: { allowed: false, required: false },
    },
    [RoleName.GUESTS]: {
      identifier: { allowed: false, required: false },
      dimension: { allowed: false, required: false },
    },
    [RoleName.USERS]: {
      identifier: { allowed: false, required: false },
      dimension: {
        allowed: true,
        required: false,
        options: Roles.USER_DIMENSIONS_LIST,
      },
    },
    [RoleName.USER]: {
      identifier: { allowed: true, required: true },
      dimension: {
        allowed: true,
        required: false,
        options: Roles.USER_DIMENSIONS_LIST,
      },
    },
    [RoleName.TEAM]: {
      identifier: { allowed: true, required: true },
      dimension: { allowed: true, required: false },
    },
    [RoleName.MEMBER]: {
      identifier: { allowed: true, required: true },
      dimension: { allowed: false, required: false },
    },
    [RoleName.LABEL]: {
      identifier: { allowed: true, required: true },
      dimension: { allowed: false, required: false },
    },
  };

  protected message: string = "Roles Error";
  protected allowedRoles: RoleName[];
  protected maxLength: number;

  /**
   * Roles constructor.
   *
   * @param maxLength - Maximum amount of roles. 0 means unlimited.
   * @param allowedRoles - Allowed roles. Defaults to all available.
   * @throws Error if any allowed role is not recognized.
   */
  constructor(
    maxLength: number = 0,
    allowedRoles: RoleName[] = Roles.ROLES_LIST,
  ) {
    if (maxLength < 0) {
      throw new Error("Maximum length of roles must be a non-negative number.");
    }
    this.maxLength = maxLength;

    if (
      !Array.isArray(allowedRoles) ||
      allowedRoles.some((role) => !Object.values(RoleName).includes(role))
    ) {
      throw new Error(
        "Allowed roles must be an array containing valid RoleName enum values.",
      );
    }
    this.allowedRoles = allowedRoles;
  }

  /**
   * Get Description.
   * Returns validator description
   * @returns {string}
   */
  public get $description(): string {
    return this.message;
  }

  /**
   * Is valid.
   * Returns true if valid or false if not.
   *
   * @param roles - Roles to validate
   * @returns {boolean}
   */
  public $valid(roles: unknown): boolean {
    this.message = "Roles Error";

    if (!Array.isArray(roles)) {
      this.message = "Roles must be an array of strings.";
      return false;
    }

    if (this.maxLength > 0 && roles.length > this.maxLength) {
      this.message = `You can only provide up to ${this.maxLength} roles.`;
      return false;
    }

    for (const roleString of roles) {
      if (typeof roleString !== "string") {
        this.message = "Every role must be of type string.";
        return false;
      }

      const isBaseRoleAllowed = this.allowedRoles.some((allowedBaseRole) =>
        roleString.startsWith(allowedBaseRole),
      );

      if (!isBaseRoleAllowed) {
        this.message = `Role "${roleString}" is not allowed. Must start with one of: ${this.allowedRoles.join(", ")}.`;
        return false;
      }

      try {
        const parsedRole = RoleParser.parse(roleString);
        const roleName = parsedRole.role;
        const identifier = parsedRole.identifier;
        const dimension = parsedRole.dimension;

        if (!this.isValidRoleComponents(roleName, identifier, dimension)) {
          return false;
        }
      } catch (error) {
        this.message =
          error instanceof Error
            ? error.message
            : "Failed to parse role string.";
        return false;
      }
    }

    return true;
  }

  /**
   * Internal helper to validate role components (name, identifier, dimension).
   * This method directly sets the `message` property on failure.
   */
  protected isValidRoleComponents(
    roleName: RoleName,
    identifier: string | null,
    dimension: string | null,
  ): boolean {
    const keyValidator = new Key();
    const labelValidator = new Label();

    const config = Roles.CONFIG[roleName];

    if (!config) {
      this.message = `Internal error: Configuration not found for role "${roleName}".`;
      return false;
    }

    const { allowed: identifierAllowed, required: identifierRequired } =
      config.identifier;

    if (!identifierAllowed && identifier !== null) {
      this.message = `Role "${roleName}" cannot have an ID value.`;
      return false;
    }

    if (identifierAllowed && identifierRequired && identifier === null) {
      this.message = `Role "${roleName}" must have an ID value.`;
      return false;
    }

    if (identifierAllowed && identifier !== null) {
      let isIdentifierValid = true;
      let identifierErrorMessage = "";

      if (roleName === RoleName.LABEL) {
        if (!labelValidator.$valid(identifier)) {
          isIdentifierValid = false;
          identifierErrorMessage = labelValidator.$description;
        }
      } else {
        if (!keyValidator.$valid(identifier)) {
          isIdentifierValid = false;
          identifierErrorMessage = keyValidator.$description;
        }
      }

      if (!isIdentifierValid) {
        this.message = `Role "${roleName}" identifier value is invalid: ${identifierErrorMessage}`;
        return false;
      }
    }

    const {
      allowed: dimensionAllowed,
      required: dimensionRequired,
      options: dimensionOptions = [],
    } = config.dimension;

    if (!dimensionAllowed && dimension !== null) {
      this.message = `Role "${roleName}" cannot have a dimension value.`;
      return false;
    }

    if (dimensionAllowed && dimensionRequired && dimension === null) {
      this.message = `Role "${roleName}" must have a dimension value.`;
      return false;
    }

    if (dimensionAllowed && dimension !== null) {
      if (
        dimensionOptions.length > 0 &&
        !dimensionOptions.includes(dimension)
      ) {
        this.message = `Role "${roleName}" dimension value is invalid. Must be one of: ${dimensionOptions.join(", ")}.`;
        return false;
      }

      if (!keyValidator.$valid(dimension)) {
        this.message = `Role "${roleName}" dimension value is invalid: ${keyValidator.$description}.`;
        return false;
      }
    }

    return true;
  }
}
