export default class Role {
    constructor(
        private role: string,
        private identifier: string = "",
        private dimension: string = "",
    ) {}

    /**
     * Create a role string from this Role instance
     *
     * @returns {string}
     */
    public toString(): string {
        let str = this.role;
        if (this.identifier) {
            str += `:${this.identifier}`;
        }
        if (this.dimension) {
            str += `/${this.dimension}`;
        }
        return str;
    }

    /**
     * @returns {string}
     */
    public getRole(): string {
        return this.role;
    }

    /**
     * @returns {string}
     */
    public getIdentifier(): string {
        return this.identifier;
    }

    /**
     * @returns {string}
     */
    public getDimension(): string {
        return this.dimension;
    }

    /**
     * Parse a role string into a Role object
     *
     * @param {string} role
     * @returns {Role}
     * @throws {Error}
     */
    public static parse(role: string): Role {
        const roleParts = role.split(":");
        const hasIdentifier = roleParts.length > 1;
        const hasDimension = role.includes("/");
        let roleName = roleParts[0]!;

        if (!hasIdentifier && !hasDimension) {
            return new Role(roleName);
        }

        if (hasIdentifier && !hasDimension) {
            const identifier = roleParts[1];
            return new Role(roleName, identifier);
        }

        if (!hasIdentifier) {
            const dimensionParts = role.split("/");
            if (dimensionParts.length !== 2) {
                throw new Error("Only one dimension can be provided");
            }

            roleName = dimensionParts[0]!;
            const dimension = dimensionParts[1];

            if (!dimension) {
                throw new Error("Dimension must not be empty");
            }
            return new Role(roleName, "", dimension);
        }

        // Has both identifier and dimension
        const dimensionParts = roleParts[1]!.split("/");
        if (dimensionParts.length !== 2) {
            throw new Error("Only one dimension can be provided");
        }

        const identifier = dimensionParts[0];
        const dimension = dimensionParts[1];

        if (!dimension) {
            throw new Error("Dimension must not be empty");
        }
        return new Role(roleName, identifier, dimension);
    }

    /**
     * Create a user role from the given ID
     *
     * @param {string} identifier
     * @param {string} status
     * @returns {Role}
     */
    public static user(identifier: string, status: string = ""): Role {
        return new Role("user", identifier, status);
    }

    /**
     * Create a users role
     *
     * @param {string} status
     * @returns {Role}
     */
    public static users(status: string = ""): Role {
        return new Role("users", "", status);
    }

    /**
     * Create a team role from the given ID and dimension
     *
     * @param {string} identifier
     * @param {string} dimension
     * @returns {Role}
     */
    public static team(identifier: string, dimension: string = ""): Role {
        return new Role("team", identifier, dimension);
    }

    /**
     * Create a label role from the given ID
     *
     * @param {string} identifier
     * @returns {Role}
     */
    public static label(identifier: string): Role {
        return new Role("label", identifier, "");
    }

    /**
     * Create an any satisfy role
     *
     * @returns {Role}
     */
    public static any(): Role {
        return new Role("any");
    }

    /**
     * Create a guests role
     *
     * @returns {Role}
     */
    public static guests(): Role {
        return new Role("guests");
    }

    /**
     * Create a member role from the given ID
     *
     * @param {string} identifier
     * @returns {Role}
     */
    public static member(identifier: string): Role {
        return new Role("member", identifier);
    }

    /**
     * Create a custom role with the given role name, identifier, and dimension
     *
     * @param {string} roleName
     * @param {string} identifier
     * @param {string} dimension
     * @returns {Role}
     */
    public static custom(
        roleName: string,
        identifier: string = "",
        dimension: string = "",
    ): Role {
        return new Role(roleName, identifier, dimension);
    }
}
