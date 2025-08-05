import { Database } from "@core/database.js";
import { Permission } from "@utils/permission.js";
import { PermissionEnum } from "@core/enums.js";
import { Validator } from "./interface.js";
import { Roles } from "./roles.js";

/**
 * Permissions Validator
 *
 * Extends the `Roles` validator to specifically handle permission strings,
 * which may include both a permission action and a role-based target.
 */
export class Permissions extends Roles implements Validator {
    protected override message: string = "Permissions Error";
    protected allowedPermissions: PermissionEnum[];

    /**
     * Permissions constructor.
     *
     * @param maxLength - Maximum amount of permissions. 0 means unlimited.
     * @param allowedPermissions - Allowed permissions. Defaults to a combined list of predefined permissions and all roles.
     * @throws Error if any allowed permission is not recognized.
     */
    constructor(
        maxLength: number = 0,
        allowedPermissions: PermissionEnum[] = [
            ...Database.PERMISSIONS,
            PermissionEnum.Write,
        ]
    ) {
        super();
        this.maxLength = maxLength;
        this.allowedPermissions = allowedPermissions;

        if (maxLength < 0) {
            throw new Error("Maximum length of permissions must be a non-negative number.");
        }

        if (!Array.isArray(allowedPermissions) || allowedPermissions.some(perm => !Object.values(PermissionEnum).includes(perm))) {
            throw new Error("Allowed permissions must be an array containing valid PermissionName enum values.");
        }
        this.allowedPermissions = allowedPermissions;
    }

    /**
     * Get Description.
     *
     * Returns validator description
     * @returns {string}
     */
    public override get $description(): string {
        return this.message;
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     * @param permissions - Permissions to validate
     * @returns {boolean}
     */
    public override $valid(permissions: unknown): boolean {
        this.message = "Permissions Error";

        if (!Array.isArray(permissions)) {
            this.message = "Permissions must be an array of strings.";
            return false;
        }

        if (this.maxLength > 0 && permissions.length > this.maxLength) {
            this.message = `You can only provide up to ${this.maxLength} permissions.`;
            return false;
        }

        for (const permissionString of permissions) {
            if (!(typeof permissionString === "string" || permissionString  instanceof Permission)) {
                this.message = "Every permission must be of type string.";
                return false;
            }

            const isAllowed = this.allowedPermissions.some((allowedPerm) =>
                permissionString.toString().startsWith(allowedPerm)
            );
            if (!isAllowed) {
                this.message = `Permission "${permissionString}" is not allowed. Must start with one of: ${this.allowedPermissions.join(", ")}.`;
                return false;
            }

            try {
                const parsedPermission = typeof permissionString === 'string' ? Permission.parse(permissionString) : permissionString;
                const roleName = parsedPermission.getRole();
                const identifier = parsedPermission.getIdentifier();
                const dimension = parsedPermission.getDimension();

                if (!this.isValidRoleComponents(roleName, identifier, dimension)) {
                    // The parent method sets the `message` for us
                    return false;
                }
            } catch (error) {
                this.message = (error instanceof Error) ? error.message : "Failed to parse permission string.";
                return false;
            }
        }
        return true;
    }
}
