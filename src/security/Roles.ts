import { Key } from "../core/validator/Key";
import { Label } from "../core/validator/Label";
import Role from "./Role";

export class Roles {
    // Roles
    public static readonly ROLE_ANY = "any";
    public static readonly ROLE_GUESTS = "guests";
    public static readonly ROLE_USERS = "users";
    public static readonly ROLE_USER = "user";
    public static readonly ROLE_TEAM = "team";
    public static readonly ROLE_MEMBER = "member";
    public static readonly ROLE_LABEL = "label";

    public static readonly ROLES = [
        Roles.ROLE_ANY,
        Roles.ROLE_GUESTS,
        Roles.ROLE_USERS,
        Roles.ROLE_USER,
        Roles.ROLE_TEAM,
        Roles.ROLE_MEMBER,
        Roles.ROLE_LABEL,
    ];

    // Dimensions
    public static readonly DIMENSION_VERIFIED = "verified";
    public static readonly DIMENSION_UNVERIFIED = "unverified";

    public static readonly USER_DIMENSIONS = [
        Roles.DIMENSION_VERIFIED,
        Roles.DIMENSION_UNVERIFIED,
    ];

    protected message: string = "Roles Error";
    protected allowed: string[];
    protected length: number;

    public static readonly CONFIG: Record<
        string,
        {
            identifier: { allowed: boolean; required: boolean };
            dimension: {
                allowed: boolean;
                required: boolean;
                options?: string[];
            };
        }
    > = {
        [Roles.ROLE_ANY]: {
            identifier: { allowed: false, required: false },
            dimension: { allowed: false, required: false },
        },
        [Roles.ROLE_GUESTS]: {
            identifier: { allowed: false, required: false },
            dimension: { allowed: false, required: false },
        },
        [Roles.ROLE_USERS]: {
            identifier: { allowed: false, required: false },
            dimension: {
                allowed: true,
                required: false,
                options: Roles.USER_DIMENSIONS,
            },
        },
        [Roles.ROLE_USER]: {
            identifier: { allowed: true, required: true },
            dimension: {
                allowed: true,
                required: false,
                options: Roles.USER_DIMENSIONS,
            },
        },
        [Roles.ROLE_TEAM]: {
            identifier: { allowed: true, required: true },
            dimension: { allowed: true, required: false },
        },
        [Roles.ROLE_MEMBER]: {
            identifier: { allowed: true, required: true },
            dimension: { allowed: false, required: false },
        },
        [Roles.ROLE_LABEL]: {
            identifier: { allowed: true, required: true },
            dimension: { allowed: false, required: false },
        },
    };

    /**
     * Roles constructor.
     *
     * @param length - Maximum amount of roles. 0 means unlimited.
     * @param allowed - Allowed roles. Defaults to all available.
     */
    constructor(length: number = 0, allowed: string[] = Roles.ROLES) {
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
    public getDescription(): string {
        return this.message;
    }

    /**
     * Is valid.
     *
     * Returns true if valid or false if not.
     *
     * @param roles - Roles to validate
     * @returns {boolean}
     */
    public isValid(roles: any): boolean {
        if (!Array.isArray(roles)) {
            this.message = "Roles must be an array of strings.";
            return false;
        }

        if (this.length && roles.length > this.length) {
            this.message =
                "You can only provide up to " + this.length + " roles.";
            return false;
        }

        for (const role of roles) {
            if (typeof role !== "string") {
                this.message = "Every role must be of type string.";
                return false;
            }

            const isAllowed = this.allowed.some((allowed) =>
                role.startsWith(allowed),
            );
            if (!isAllowed) {
                this.message =
                    'Role "' +
                    role +
                    '" is not allowed. Must be one of: ' +
                    this.allowed.join(", ") +
                    ".";
                return false;
            }

            try {
                const parsedRole = Role.parse(role);
                const roleName = parsedRole.getRole();
                const identifier = parsedRole.getIdentifier();
                const dimension = parsedRole.getDimension();

                if (!this.isValidRole(roleName, identifier, dimension)) {
                    return false;
                }
            } catch (error) {
                this.message = (error as Error).message;
                return false;
            }
        }
        return true;
    }

    protected isValidRole(
        role: string,
        identifier: string,
        dimension: string,
    ): boolean {
        const key = new Key();
        const label = new Label();
        const config = Roles.CONFIG[role as keyof typeof Roles.CONFIG] || null;

        if (!config) {
            this.message =
                'Role "' +
                role +
                '" is not allowed. Must be one of: ' +
                Roles.ROLES.join(", ") +
                ".";
            return false;
        }

        // Process identifier configuration
        const { allowed: identifierAllowed, required: identifierRequired } =
            config.identifier;

        if (!identifierAllowed && identifier) {
            this.message = 'Role "' + role + '" cannot have an ID value.';
            return false;
        }

        if (identifierAllowed && identifierRequired && !identifier) {
            this.message = 'Role "' + role + '" must have an ID value.';
            return false;
        }

        if (
            identifierAllowed &&
            identifier &&
            role === Roles.ROLE_LABEL &&
            !label.isValid(identifier)
        ) {
            this.message =
                'Role "' +
                role +
                '" identifier value is invalid: ' +
                label.getDescription();
            return false;
        } else if (
            identifierAllowed &&
            identifier &&
            role !== Roles.ROLE_LABEL &&
            !key.isValid(identifier)
        ) {
            this.message =
                'Role "' +
                role +
                '" identifier value is invalid: ' +
                key.getDescription();
            return false;
        }

        // Process dimension configuration
        const {
            allowed: dimensionAllowed,
            required: dimensionRequired,
            options = [],
        } = config.dimension;

        if (!dimensionAllowed && dimension) {
            this.message = 'Role "' + role + '" cannot have a dimension value.';
            return false;
        }

        if (dimensionAllowed && dimensionRequired && !dimension) {
            this.message = 'Role "' + role + '" must have a dimension value.';
            return false;
        }

        if (
            dimensionAllowed &&
            dimension &&
            Array.isArray(options) &&
            options.length > 0 &&
            !options.includes(dimension)
        ) {
            this.message =
                'Role "' +
                role +
                '" dimension value is invalid. Must be one of: ' +
                options.join(", ") +
                ".";
            return false;
        }

        if (dimensionAllowed && dimension && !key.isValid(dimension)) {
            this.message =
                'Role "' +
                role +
                '" dimension value is invalid: ' +
                key.getDescription();
            return false;
        }

        return true;
    }
}
