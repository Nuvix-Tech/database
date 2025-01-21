export class Authorization {
  private static roles: Record<string, boolean> = {
    any: true,
  };

  protected action: string;
  protected message: string = 'Authorization Error';
  public static status: boolean = true;
  public static statusDefault: boolean = true;

  /**
   * Authorization constructor.
   *
   * @param action - The action to authorize
   */
  constructor(action: string) {
    this.action = action;
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
   * @param permissions - Permissions to validate
   * @returns {boolean}
   */
  public isValid(permissions: any): boolean {
    if (!Authorization.status) {
      return true;
    }

    if (!permissions || permissions.length === 0) {
      this.message = `No permissions provided for action '${this.action}'`;
      return false;
    }

    for (const permission of permissions) {
      if (Authorization.roles.hasOwnProperty(permission)) {
        return true;
      }
    }

    this.message = `Missing "${this.action}" permission for role "${permissions[0]}". Only "${JSON.stringify(Authorization.getRoles())}" scopes are allowed and "${JSON.stringify(permissions)}" was given.`;
    return false;
  }

  /**
   * Set a role.
   *
   * @param role - The role to set
   */
  public static setRole(role: string): void {
    Authorization.roles[role] = true;
  }

  /**
   * Unset a role.
   *
   * @param role - The role to unset
   */
  public static unsetRole(role: string): void {
    delete Authorization.roles[role];
  }

  /**
   * Get all roles.
   *
   * @returns {string[]}
   */
  public static getRoles(): string[] {
    return Object.keys(Authorization.roles);
  }

  /**
   * Clean all roles.
   */
  public static cleanRoles(): void {
    Authorization.roles = {};
  }

  /**
   * Check if a role exists.
   *
   * @param role - The role to check
   * @returns {boolean}
   */
  public static isRole(role: string): boolean {
    return Authorization.roles.hasOwnProperty(role);
  }

  /**
   * Change default status.
   *
   * @param status - The new default status
   */
  public static setDefaultStatus(status: boolean): void {
    Authorization.statusDefault = status;
    Authorization.status = status;
  }

  /**
   * Skip Authorization
   *
   * @template T
   * @param callback - The callback to execute
   * @returns {T}
   */
  public static async skip<T>(callback: () => Promise<T>): Promise<T> {
    const initialStatus = Authorization.status;
    Authorization.disable();

    try {
      return await callback();
    } finally {
      Authorization.status = initialStatus;
    }
  }

  /**
   * Enable Authorization checks
   */
  public static enable(): void {
    Authorization.status = true;
  }

  /**
   * Disable Authorization checks
   */
  public static disable(): void {
    Authorization.status = false;
  }

  /**
   * Reset Authorization status
   */
  public static reset(): void {
    Authorization.status = Authorization.statusDefault;
  }

  /**
   * Is array
   *
   * @returns {boolean}
   */
  public isArray(): boolean {
    return false;
  }

  /**
   * Get Type
   *
   * @returns {string}
   */
  public getType(): string {
    return 'array'; // Assuming you want to return a string representation of the type
  }
}