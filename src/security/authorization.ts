import { AsyncLocalStorage } from "async_hooks";

export const storage = new AsyncLocalStorage<Map<string, any>>();

export class Authorization {
    private static roles: Record<string, boolean> = { any: true };
    protected action: string;
    protected message: string = "Authorization Error";

    private static statusDefault: boolean = true;
    private static useStorage: boolean = false; // Determines if AsyncLocalStorage is enabled

    constructor(action: string) {
        this.action = action;
    }

    // ------ STORAGE MANAGEMENT ------

    /**
     * Enables per-request storage (AsyncLocalStorage).
     */
    public static enableStorage(): void {
        this.useStorage = true;
    }

    /**
     * Disables per-request storage, reverting to default static behavior.
     */
    public static disableStorage(): void {
        this.useStorage = false;
    }

    // ------ AUTHORIZATION METHODS ------

    public getDescription(): string {
        return this.message;
    }

    public isValid(permissions: any): boolean {
        if (!Authorization.getStatus()) {
            return true;
        }

        if (!permissions || permissions.length === 0) {
            this.message = `No permissions provided for action '${this.action}'`;
            return false;
        }

        for (const permission of permissions) {
            if (Authorization.getRoles().includes(permission)) {
                return true;
            }
        }

        this.message = `Missing "${this.action}" permission for role "${permissions[0]}". Only "${JSON.stringify(Authorization.getRoles())}" scopes are allowed and "${JSON.stringify(permissions)}" was given.`;
        return false;
    }

    // ------ ROLES MANAGEMENT ------

    public static setRole(role: string): void {
        if (this.useStorage) {
            const store = storage.getStore();
            if (store) {
                const roles = store.get("roles") || {};
                roles[role] = true;
                store.set("roles", roles);
                return;
            }
            this.roles[role] = true;
        }
        this.roles[role] = true;
    }

    public static unsetRole(role: string): void {
        if (this.useStorage) {
            const store = storage.getStore();
            if (store) {
                const roles = store.get("roles") || {};
                delete roles[role];
                store.set("roles", roles);
                return;
            }
            delete this.roles[role];
        }
        delete this.roles[role];
    }

    public static getRoles(): string[] {
        if (this.useStorage) {
            const store = storage.getStore();
            return store
                ? Object.keys(store.get("roles") || {})
                : Object.keys(this.roles);
        }
        return Object.keys(this.roles);
    }

    public static cleanRoles(): void {
        if (this.useStorage) {
            const store = storage.getStore();
            if (store) {
                store.set("roles", {});
                return;
            }
            this.roles = {};
        }
        this.roles = {};
    }

    public static isRole(role: string): boolean {
        return this.getRoles().includes(role);
    }

    // ------ STATUS MANAGEMENT ------

    public static setDefaultStatus(status: boolean): void {
        this.statusDefault = status;
        this.setStatus(status);
    }

    public static setStatus(status: boolean): void {
        if (this.useStorage) {
            const store = storage.getStore();
            if (store) {
                store.set("status", status);
                return;
            }
            this.statusDefault = status;
        }
        this.statusDefault = status;
    }

    public static getStatus(): boolean {
        if (this.useStorage) {
            const store = storage.getStore();
            return store?.get("status") ?? false;
        }
        return this.statusDefault;
    }

    public static async skip<T>(callback: () => Promise<T>): Promise<T> {
        const initialStatus = this.getStatus();
        this.disable();
        try {
            return await callback();
        } finally {
            this.setStatus(initialStatus);
        }
    }

    public static enable(): void {
        this.setStatus(true);
    }

    public static disable(): void {
        this.setStatus(false);
    }

    public static reset(): void {
        this.setStatus(this.statusDefault);
    }
}
