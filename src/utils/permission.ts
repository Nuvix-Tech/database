import { Constant as Database } from "../core/constant";
import { DatabaseError as Exception } from "../errors/base";
import Role from "./role.js";

export default class Permission {
    private static aggregates: Record<string, string[]> = {
        write: [
            Database.PERMISSION_CREATE,
            Database.PERMISSION_UPDATE,
            Database.PERMISSION_DELETE,
        ],
    };

    private role: Role;

    constructor(
        private permission: string,
        role: string,
        identifier: string = "",
        dimension: string = "",
    ) {
        this.role = new Role(role, identifier, dimension);
    }

    public toString(): string {
        return `${this.permission}("${this.role.toString()}")`;
    }

    public getPermission(): string {
        return this.permission;
    }

    public getRole(): string {
        return this.role.getRole();
    }

    public getIdentifier(): string {
        return this.role.getIdentifier();
    }

    public getDimension(): string {
        return this.role.getDimension();
    }

    public static parse(permission: string): Permission {
        const permissionParts = permission.split('("');

        if (permissionParts.length !== 2) {
            throw new Exception(
                `Invalid permission string format: "${permission}".`,
            );
        }

        const perm = permissionParts[0]!;

        if (!this.isValidPermission(perm)) {
            throw new Exception(`Invalid permission type: "${perm}".`);
        }

        const fullRole = permissionParts[1]!.replace('")', "");
        const roleParts = fullRole.split(":");
        const role = roleParts[0]!;

        const hasIdentifier = roleParts.length > 1;
        const hasDimension = fullRole.includes("/");

        if (!hasIdentifier && !hasDimension) {
            return new Permission(perm, role);
        }

        if (hasIdentifier && !hasDimension) {
            const identifier = roleParts[1];
            return new Permission(perm, role, identifier);
        }

        if (!hasIdentifier) {
            const dimensionParts = fullRole.split("/");
            if (dimensionParts.length !== 2) {
                throw new Exception("Only one dimension can be provided");
            }

            const role = dimensionParts[0]!;
            const dimension = dimensionParts[1];

            if (!dimension) {
                throw new Exception("Dimension must not be empty");
            }
            return new Permission(perm, role, "", dimension);
        }

        // Has both identifier and dimension
        const dimensionParts = roleParts[1]!.split("/");
        if (dimensionParts.length !== 2) {
            throw new Exception("Only one dimension can be provided");
        }

        const identifier = dimensionParts[0];
        const dimension = dimensionParts[1];

        if (!dimension) {
            throw new Exception("Dimension must not be empty");
        }

        return new Permission(perm, role, identifier, dimension);
    }

    private static isValidPermission(permission: string): boolean {
        return (
            this.aggregates.hasOwnProperty(permission) ||
            Database.PERMISSIONS.includes(permission)
        );
    }

    public static aggregate(
        permissions: string[] | null,
        allowed: string[] = Database.PERMISSIONS,
    ): string[] | null {
        if (permissions === null) {
            return null;
        }

        const mutated: string[] = [];
        for (const permission of permissions) {
            const parsedPermission = this.parse(permission);
            for (const [type, subTypes] of Object.entries(this.aggregates)) {
                if (parsedPermission.getPermission() !== type) {
                    mutated.push(parsedPermission.toString());
                    continue;
                }
                for (const subType of subTypes) {
                    if (!allowed.includes(subType)) {
                        continue;
                    }
                    mutated.push(
                        new Permission(
                            subType,
                            parsedPermission.getRole(),
                            parsedPermission.getIdentifier(),
                            parsedPermission.getDimension(),
                        ).toString(),
                    );
                }
            }
        }
        return Array.from(new Set(mutated)); // Remove duplicates
    }

    public static read(role: Role): string {
        const permission = new Permission(
            "read",
            role.getRole(),
            role.getIdentifier(),
            role.getDimension(),
        );
        return permission.toString();
    }

    public static create(role: Role): string {
        const permission = new Permission(
            "create",
            role.getRole(),
            role.getIdentifier(),
            role.getDimension(),
        );
        return permission.toString();
    }

    public static update(role: Role): string {
        const permission = new Permission(
            "update",
            role.getRole(),
            role.getIdentifier(),
            role.getDimension(),
        );
        return permission.toString();
    }

    public static delete(role: Role): string {
        const permission = new Permission(
            "delete",
            role.getRole(),
            role.getIdentifier(),
            role.getDimension(),
        );
        return permission.toString();
    }

    public static write(role: Role): string {
        const permission = new Permission(
            "write",
            role.getRole(),
            role.getIdentifier(),
            role.getDimension(),
        );
        return permission.toString();
    }
}
