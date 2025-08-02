import { Database } from "@core/database.js";
import { Roles } from "./roles.js";
import Permission from "@utils/permission.js";
import { PermissionEnum } from "@core/enums.js";

export class Permissions extends Roles {
    protected override message: string = "Permissions Error";
    protected override allowed: string[];
    protected override length: number;

    /**
     * Permissions constructor.
     *
     * @param length - Maximum amount of permissions. 0 means unlimited.
     * @param allowed - Allowed permissions. Defaults to all available.
     */
    constructor(
        length: number = 0,
        allowed: string[] = [
            ...Database.PERMISSIONS,
            PermissionEnum.Write,
        ],
    ) {
        super(length, allowed);
        this.length = length;
        this.allowed = allowed;
    }

    /**
     * Get Description.
     *
     * Returns validator description
     *
     * @returns {string}
     */
    public override getDescription(): string {
        return this.message;
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param permissions - Permissions to validate
     * @returns {boolean}
     */
    public override isValid(permissions: any): boolean {
        if (!Array.isArray(permissions)) {
            this.message = "Permissions must be an array of strings.";
            return false;
        }

        if (this.length && permissions.length > this.length) {
            this.message =
                "You can only provide up to " + this.length + " permissions.";
            return false;
        }

        for (const permission of permissions) {
            if (typeof permission !== "string") {
                this.message = "Every permission must be of type string.";
                return false;
            }

            if (permission === "*") {
                this.message =
                    'Wildcard permission "*" has been replaced. Use "any" instead.';
                return false;
            }

            if (permission.startsWith("role:")) {
                this.message =
                    'Permissions using the "role:" prefix have been replaced. Use "users", "guests", or "any" instead.';
                return false;
            }

            const isAllowed = this.allowed.some((allowed) =>
                permission.startsWith(allowed),
            );
            if (!isAllowed) {
                this.message =
                    'Permission "' +
                    permission +
                    '" is not allowed. Must be one of: ' +
                    this.allowed.join(", ") +
                    ".";
                return false;
            }

            try {
                const parsedPermission = Permission.parse(permission);
                const role = parsedPermission.getRole();
                const identifier = parsedPermission.getIdentifier();
                const dimension = parsedPermission.getDimension();

                if (!this.isValidRole(role, identifier, dimension)) {
                    return false;
                }
            } catch (error) {
                this.message = (error as Error).message;
                return false;
            }
        }
        return true;
    }
}
