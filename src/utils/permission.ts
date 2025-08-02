import { PermissionEnum } from "@core/enums.js";
import { Role, RoleName } from "./role.js";
import { StructureException } from "@errors/index.js";
import { Database } from "@core/database.js";

/**
 * Represents a specific permission with an associated Role.
 * It provides methods for string conversion, parsing, and aggregation.
 */
export class Permission {
    private static aggregates: Record<string, PermissionEnum[]> = {
        // "write" is an aggregate of create, update, and delete.
        [PermissionEnum.Write]: [
            PermissionEnum.Create,
            PermissionEnum.Update,
            PermissionEnum.Delete,
        ],
    };

    /**
     * The permission name (e.g., "read", "write").
     */
    public readonly permission: PermissionEnum;

    /**
     * The role associated with the permission.
     */
    public readonly role: Role;

    /**
     * Permission constructor.
     *
     * @param permission - The specific permission (e.g., read, write).
     * @param role - The Role instance associated with this permission.
     */
    constructor(permission: PermissionEnum, role: Role) {
        this.permission = permission;
        this.role = role;
    }

    /**
     * Create a permission string from this Permission instance.
     * Format: "permission("role:identifier/dimension")".
     *
     * @returns {string} The string representation of the permission.
     */
    public toString(): string {
        return `${this.permission}("${this.role.toString()}")`;
    }

    /**
     * Get the permission name.
     * @returns {PermissionEnum}
     */
    public getPermission(): PermissionEnum {
        return this.permission;
    }

    /**
     * Get the role name from the associated Role instance.
     * @returns {RoleName}
     */
    public getRole(): RoleName {
        return this.role.role;
    }

    /**
     * Get the identifier from the associated Role instance.
     * @returns {string | null}
     */
    public getIdentifier(): string | null {
        return this.role.identifier;
    }

    /**
     * Get the dimension from the associated Role instance.
     * @returns {string | null}
     */
    public getDimension(): string | null {
        return this.role.dimension;
    }

    /**
     * Parse a permission string into a Permission object.
     *
     * @param permissionString - The permission string to parse.
     * @returns {Permission} A new Permission instance.
     * @throws {StructureException} If the permission string is invalid.
     */
    public static parse(permissionString: string): Permission {
        const regex = /^(\w+)\("(.+)"\)$/;
        const match = permissionString.match(regex);

        if (!match || match.length !== 3) {
            throw new StructureException(
                `Invalid permission string format: "${permissionString}". Expected "permission(\"role:id/dim\")".`
            );
        }

        const permissionName = match[1] as PermissionEnum;
        const roleString = match[2]!;

        if (!this.isValidPermission(permissionName)) {
            throw new StructureException(`Invalid permission type: "${permissionName}".`);
        }

        try {
            const role = Role.parse(roleString);
            return new Permission(permissionName, role);
        } catch (error) {
            if (error instanceof Error) {
                throw new StructureException(`Failed to parse role from permission string: ${error.message}`);
            }
            throw new StructureException("Failed to parse role from permission string.");
        }
    }

    /**
     * Checks if a given permission string is a valid, recognized permission.
     *
     * @param permission - The permission string to validate.
     * @returns {boolean} True if the permission is valid, false otherwise.
     */
    private static isValidPermission(permission: string): boolean {
        return (
            Object.keys(this.aggregates).includes(permission) ||
            Database.PERMISSIONS.includes(permission as PermissionEnum)
        );
    }

    /**
     * Expands an array of permissions, replacing aggregate permissions (like "write")
     * with their sub-permissions.
     *
     * @param permissions - An array of permission strings. Can be null.
     * @param allowed - The allowed permissions to check against.
     * @returns {string[] | null} A new array of aggregated permission strings, or null.
     */
    public static aggregate(
        permissions: string[] | null,
        allowed: PermissionEnum[] = Database.PERMISSIONS,
    ): string[] | null {
        if (permissions === null) {
            return null;
        }

        const aggregatedPermissions: string[] = [];
        const seen = new Set<string>();

        for (const permission of permissions) {
            let parsedPermission;
            try {
                parsedPermission = this.parse(permission);
            } catch (e) {
                // If a permission string can't be parsed, skip it.
                continue;
            }

            const permissionName = parsedPermission.getPermission();
            const role = parsedPermission.role;

            if (Object.keys(this.aggregates).includes(permissionName)) {
                // If the permission is an aggregate, expand it.
                for (const subType of this.aggregates[permissionName] as PermissionEnum[]) {
                    if (allowed.includes(subType)) {
                        const newPermissionString = new Permission(subType, role).toString();
                        if (!seen.has(newPermissionString)) {
                            aggregatedPermissions.push(newPermissionString);
                            seen.add(newPermissionString);
                        }
                    }
                }
            } else {
                // If it's a standard permission, just add it.
                const newPermissionString = parsedPermission.toString();
                if (!seen.has(newPermissionString)) {
                    aggregatedPermissions.push(newPermissionString);
                    seen.add(newPermissionString);
                }
            }
        }
        return aggregatedPermissions;
    }

    /**
     * Create a "read" permission for a given Role.
     * @param role - The Role instance.
     * @returns {Permission} A new Permission instance.
     */
    public static read(role: Role): Permission {
        return new Permission(PermissionEnum.Read, role);
    }

    /**
     * Create a "create" permission for a given Role.
     * @param role - The Role instance.
     * @returns {Permission} A new Permission instance.
     */
    public static create(role: Role): Permission {
        return new Permission(PermissionEnum.Create, role);
    }

    /**
     * Create an "update" permission for a given Role.
     * @param role - The Role instance.
     * @returns {Permission} A new Permission instance.
     */
    public static update(role: Role): Permission {
        return new Permission(PermissionEnum.Update, role);
    }

    /**
     * Create a "delete" permission for a given Role.
     * @param role - The Role instance.
     * @returns {Permission} A new Permission instance.
     */
    public static delete(role: Role): Permission {
        return new Permission(PermissionEnum.Delete, role);
    }

    /**
     * Create a "write" permission for a given Role.
     * @param role - The Role instance.
     * @returns {Permission} A new Permission instance.
     */
    public static write(role: Role): Permission {
        return new Permission(PermissionEnum.Write, role);
    }
}
